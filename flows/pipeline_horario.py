"""
Pipeline principal para procesamiento de datos horarios de contaminantes.

Este pipeline implementa un proceso ETL incremental que:
1. Extrae datos de InfluxDB para múltiples contaminantes en paralelo
2. Transforma los datos de formato wide a long con agregaciones horarias
3. Exporta a múltiples destinos (Excel, BigQuery, MotherDuck)
4. Persiste el estado de la última hora procesada en Prefect Blocks
"""
import asyncio
from datetime import datetime
from typing import List, Optional

import polars as pl
from prefect import flow, task, get_run_logger
from prefect.cache_policies import INPUTS
from prefect.futures import wait

from config import get_settings, POLLUTANTS_TO_PROCESS
from config.pollutants import TABLE_CONFIG
from src.utils import (
    # Clients
    influxdb_client,
    fetch_incremental_data,
    motherduck_client,
    # State
    get_pipeline_state,
    # Transformers
    build_metric_columns,
    aggregate_to_long_format,
    # Exporters
    export_to_excel,
    export_to_bigquery,
    export_to_motherduck,
    # Schema
    create_empty_output_dataframe,
    validate_output_schema,
    # Datetime
    get_previous_hour_start,
)


# Cache expiration times
from datetime import timedelta

FETCH_CACHE_EXPIRATION = timedelta(hours=1)
TRANSFORM_CACHE_EXPIRATION = timedelta(hours=24)


@task(
    name="fetch_pollutant_data",
    retries=2,
    retry_delay_seconds=60,
    cache_policy=INPUTS,
    cache_expiration=FETCH_CACHE_EXPIRATION,
)
async def fetch_pollutant_data_task(
    pollutant: str, 
    last_processed_hour: Optional[datetime] = None
) -> pl.DataFrame:
    """
    Task para obtener datos de un contaminante desde InfluxDB.

    Args:
        pollutant: Nombre del contaminante (e.g., 'co', 'nox', 'pm10')
        last_processed_hour: Última hora procesada (para incremental loading)

    Returns:
        pl.DataFrame: DataFrame con los datos del contaminante
    """
    logger = get_run_logger()
    incremental = last_processed_hour is not None
    logger.info(f"📡 Fetching {pollutant} (incremental={incremental})")

    async with influxdb_client() as client:
        df = await fetch_incremental_data(client, pollutant, last_processed_hour)
        logger.info(f"✅ {pollutant}: {len(df)} rows fetched")
        return df


@task(
    name="transform_pollutant_data",
    cache_policy=INPUTS,
    cache_expiration=TRANSFORM_CACHE_EXPIRATION,
)
def transform_pollutant_data_task(
    pollutant: str, 
    df: pl.DataFrame, 
    pipeline_version: str
) -> pl.DataFrame:
    """
    Task para transformar datos de formato wide a long.

    Args:
        pollutant: Nombre del contaminante
        df: DataFrame con datos en formato wide
        pipeline_version: Versión del pipeline

    Returns:
        pl.DataFrame: DataFrame transformado en formato long
    """
    logger = get_run_logger()
    logger.info(f"🔄 Transforming {pollutant} ({len(df)} input rows)")

    if df.is_empty():
        logger.info(f"⏭️ {pollutant}: no data to transform")
        return create_empty_output_dataframe()

    config = TABLE_CONFIG[pollutant]
    metric_columns = build_metric_columns(config["metrics"])

    transformed = aggregate_to_long_format(
        df, 
        metric_columns=metric_columns, 
        version=pipeline_version
    )
    
    logger.info(f"✅ {pollutant}: {len(transformed)} output rows")
    return transformed


@task(name="export_to_excel_task")
def export_to_excel_task(df: pl.DataFrame, output_path: str) -> None:
    """Task para exportar datos a Excel."""
    logger = get_run_logger()
    logger.info(f"📊 Exporting {len(df)} rows to Excel")
    export_to_excel(df, output_path)
    logger.info("✅ Excel export completed")


@task(name="export_to_bigquery_task")
def export_to_bigquery_task(
    df: pl.DataFrame, 
    project_id: str, 
    dataset_id: str, 
    table_id: str
) -> None:
    """Task para exportar datos a BigQuery."""
    logger = get_run_logger()
    logger.info(f"☁️ Exporting {len(df)} rows to BigQuery")
    export_to_bigquery(project_id, dataset_id, table_id, df)
    logger.info("✅ BigQuery export completed")


@task(name="export_to_motherduck_task")
def export_to_motherduck_task(df: pl.DataFrame, table_name: str) -> None:
    """Task para exportar datos a MotherDuck."""
    logger = get_run_logger()
    logger.info(f"🦆 Exporting {len(df)} rows to MotherDuck")
    
    with motherduck_client() as conn:
        export_to_motherduck(conn, table_name, df)
    
    logger.info("✅ MotherDuck export completed")


@task(name="save_pipeline_state")
async def save_pipeline_state_task(
    state_block_name: str,
    processed_hour: datetime,
    pipeline_version: str
) -> None:
    """
    Task para guardar el estado del pipeline en Prefect Blocks.
    
    Args:
        state_block_name: Nombre del block de estado
        processed_hour: Hora que fue procesada exitosamente
        pipeline_version: Versión del pipeline
    """
    logger = get_run_logger()
    
    state_manager = get_pipeline_state(state_block_name)
    await state_manager.set_last_processed_hour(processed_hour)
    await state_manager.update_metadata("pipeline_version", pipeline_version)
    await state_manager.update_metadata("last_execution", datetime.now().isoformat())
    
    logger.info(f"💾 State saved: last_processed_hour={processed_hour}")


@flow(name="pipeline-horario")
async def pipeline_horario(
    pollutants: Optional[List[str]] = None,
    export_excel: bool = False,
    export_bigquery: bool = False,
    export_motherduck: bool = True,
    incremental: bool = True,
    state_block_name: str = "pipeline-state",
) -> pl.DataFrame:
    """
    Flow principal del pipeline de procesamiento de datos horarios.

    Args:
        pollutants: Lista de contaminantes a procesar (None = todos)
        export_excel: Exportar a Excel
        export_bigquery: Exportar a BigQuery
        export_motherduck: Exportar a MotherDuck
        incremental: Usar procesamiento incremental con estado persistido
        state_block_name: Nombre del Prefect Block para el estado

    Returns:
        pl.DataFrame: DataFrame consolidado con todos los datos transformados
    """
    logger = get_run_logger()
    settings = get_settings()
    
    pollutants_to_process = pollutants or POLLUTANTS_TO_PROCESS
    
    logger.info(f"🚀 Starting pipeline for {len(pollutants_to_process)} pollutants")
    logger.info(f"📋 Config: version={settings.PIPELINE_VERSION}, incremental={incremental}")

    # Get last processed hour for incremental mode
    last_processed_hour = None
    if incremental:
        state_manager = get_pipeline_state(state_block_name)
        last_processed_hour = await state_manager.get_last_processed_hour()
        if last_processed_hour:
            logger.info(f"📅 Resuming from {last_processed_hour}")
        else:
            logger.info("📅 First run, no previous state")

    # Fetch data for all pollutants in parallel
    logger.info("📥 Fetching data from InfluxDB")
    fetch_tasks = [
        fetch_pollutant_data_task(pollutant, last_processed_hour)
        for pollutant in pollutants_to_process
    ]
    dataframes = await asyncio.gather(*fetch_tasks)

    # Transform all dataframes
    logger.info("🔄 Transforming data")
    transformed_dfs = [
        transform_pollutant_data_task(
            pollutant=pollutant,
            df=df,
            pipeline_version=settings.PIPELINE_VERSION
        )
        for pollutant, df in zip(pollutants_to_process, dataframes)
    ]

    # Filter empty dataframes and validate schema
    valid_dfs = [
        validate_output_schema(df) 
        for df in transformed_dfs 
        if not df.is_empty()
    ]

    # Concatenate results
    if valid_dfs:
        result = pl.concat(valid_dfs)
    else:
        result = create_empty_output_dataframe()
    
    logger.info(f"📊 Total rows: {len(result)}")

    # Export to configured destinations
    if len(result) > 0:
        await _run_exports(
            result, settings, 
            export_excel, export_bigquery, export_motherduck
        )

    # Save state after successful processing
    if incremental and len(result) > 0:
        processed_hour = get_previous_hour_start()
        await save_pipeline_state_task(
            state_block_name, 
            processed_hour, 
            settings.PIPELINE_VERSION
        )

    logger.info("✅ Pipeline completed")
    return result


async def _run_exports(
    df: pl.DataFrame,
    settings,
    export_excel: bool,
    export_bigquery: bool,
    export_motherduck: bool
) -> None:
    """Run export tasks concurrently."""
    logger = get_run_logger()
    export_futures = []

    if export_excel:
        export_to_excel_task(df, settings.output_path)

    if export_bigquery:
        if all([settings.GOOGLE_PROJECT_ID, settings.BIGQUERY_DATASET_ID, settings.BIGQUERY_TABLE_ID]):
            export_futures.append(
                export_to_bigquery_task.submit(
                    df,
                    settings.GOOGLE_PROJECT_ID,
                    settings.BIGQUERY_DATASET_ID,
                    settings.BIGQUERY_TABLE_ID,
                )
            )
        else:
            logger.warning("BigQuery export skipped: missing configuration")

    if export_motherduck:
        if settings.MOTHERDUCK_DATABASE:
            table_name = f"{settings.MOTHERDUCK_DATABASE}.contaminantes_horarios"
            export_futures.append(
                export_to_motherduck_task.submit(df, table_name)
            )
        else:
            logger.warning("MotherDuck export skipped: missing configuration")

    if export_futures:
        logger.info(f"🚀 Running {len(export_futures)} export tasks")
        wait(export_futures)
        logger.info("✅ All exports completed")


if __name__ == "__main__":
    asyncio.run(pipeline_horario())
