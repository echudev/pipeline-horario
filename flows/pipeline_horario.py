"""
Pipeline principal para procesamiento de datos horarios de contaminantes
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import List
import polars as pl
from prefect import flow, task, get_run_logger
from prefect.cache_policies import INPUTS
from prefect.futures import wait
from config import get_settings, POLLUTANTS_TO_PROCESS
from config.pollutants import TABLE_CONFIG
from src.utils.clients import (
    get_influxdb_client,
    close_influxdb_client,
    fetch_incremental_data,
    get_bigquery_client,
    get_motherduck_client,
)
from src.utils.state import get_pipeline_state
from src.utils.transformers import (
    build_metric_columns,
    aggregate_to_long_format,
)
from src.utils.exporters import (
    export_to_excel,
    export_to_bigquery,
    export_to_motherduck,
)


@task(
    name="fetch_pollutant_data",
    retries=2,
    retry_delay_seconds=60,
    cache_policy=INPUTS,
    cache_expiration=timedelta(hours=1),  # Cache for 1 hour
)
async def fetch_pollutant_data(
    pollutant: str, last_processed_hour: datetime | None = None
) -> pl.DataFrame:
    """
    Task para obtener datos de un contaminante específico desde InfluxDB

    Args:
        pollutant: Nombre del contaminante (e.g., 'co', 'nox', 'pm10')
        last_processed_hour: Última hora procesada exitosamente (para incremental loading)

    Returns:
        pl.DataFrame: DataFrame con los datos del contaminante
    """
    logger = get_run_logger()
    incremental_mode = last_processed_hour is not None
    logger.info(f"📡 Fetching {pollutant} data (incremental: {incremental_mode})")

    client = get_influxdb_client()
    try:
        df = await fetch_incremental_data(client, pollutant, last_processed_hour)
        row_count = len(df)
        logger.info(f"✅ {pollutant}: {row_count} rows fetched successfully")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to fetch {pollutant} data: {e}")
        raise


@task(
    name="transform_pollutant_data",
    cache_policy=INPUTS,
    cache_expiration=timedelta(
        hours=24
    ),  # Cache for 24 hours since transformations are stable
)
def transform_pollutant_data(
    pollutant: str, df: pl.DataFrame, pipeline_version: str
) -> pl.DataFrame:
    """
    Task para transformar datos de un contaminante de formato wide a long

    Args:
        pollutant: Nombre del contaminante
        df: DataFrame con datos en formato wide
        pipeline_version: Versión del pipeline

    Returns:
        pl.DataFrame: DataFrame transformado en formato long
    """
    logger = get_run_logger()
    input_rows = len(df)
    logger.info(f"🔄 Transforming {pollutant} data ({input_rows} input rows)")

    try:
        config = TABLE_CONFIG[pollutant]
        metrics = config["metrics"]

        # Build metric columns mapping
        metric_columns = build_metric_columns(metrics)

        # Transform the dataframe
        transformed_df = aggregate_to_long_format(
            df, metric_columns=metric_columns, version=pipeline_version
        )

        output_rows = len(transformed_df)
        logger.info(f"✅ {pollutant} transformed: {output_rows} output rows")
        return transformed_df
    except Exception as e:
        logger.error(f"❌ Failed to transform {pollutant} data: {e}")
        raise


@task(name="export_to_excel_task")
def export_to_excel_task(df: pl.DataFrame, output_path: str) -> None:
    """
    Task para exportar datos a Excel

    Args:
        df: DataFrame a exportar
        output_path: Ruta completa del archivo de salida
    """
    logger = get_run_logger()
    row_count = len(df)
    logger.info(f"📊 Exporting {row_count} rows to Excel: {output_path}")
    export_to_excel(df, output_path)
    logger.info("✅ Excel export completed successfully")


@task(name="export_to_bigquery_task")
def export_to_bigquery_task(
    df: pl.DataFrame, project_id: str, dataset_id: str, table_id: str
) -> None:
    """
    Task para exportar datos a BigQuery

    Args:
        df: DataFrame a exportar
        project_id: ID del proyecto de Google Cloud
        dataset_id: ID del dataset de BigQuery
        table_id: ID de la tabla de BigQuery
    """
    logger = get_run_logger()
    row_count = len(df)
    logger.info(
        f"☁️ Exporting {row_count} rows to BigQuery: {project_id}.{dataset_id}.{table_id}"
    )
    export_to_bigquery(project_id, dataset_id, table_id, df)
    logger.info("✅ BigQuery export completed successfully")


@task(name="export_to_motherduck_task")
def export_to_motherduck_task(df: pl.DataFrame, table_name: str) -> None:
    """
    Task para exportar datos a MotherDuck

    Args:
        df: DataFrame a exportar
        table_name: Nombre de la tabla en MotherDuck
    """
    logger = get_run_logger()
    row_count = len(df)
    logger.info(f"🦆 Exporting {row_count} rows to MotherDuck: {table_name}")
    client = get_motherduck_client()
    export_to_motherduck(client, table_name, df)
    logger.info("✅ MotherDuck export completed successfully")


@task(name="save_pipeline_state")
async def save_pipeline_state() -> None:
    """
    Task para guardar el estado del pipeline después de una ejecución exitosa usando Prefect Blocks
    """
    logger = get_run_logger()
    try:
        state_manager = get_pipeline_state()
        # Guardar la hora anterior a la actual (la que acabamos de procesar)
        now = datetime.now(timezone.utc)
        last_processed_hour = now.replace(
            minute=0, second=0, microsecond=0, hour=now.hour - 1
        )

        await state_manager.set_last_processed_hour(last_processed_hour)
        # Update metadata with execution info
        await state_manager.update_metadata("last_execution", now.isoformat())
        await state_manager.update_metadata(
            "pipeline_version", get_settings().PIPELINE_VERSION
        )

        logger.info(f"💾 State saved: last processed hour = {last_processed_hour}")
        logger.info(
            f"🔄 Prefect Block '{state_manager.block_name}' updated successfully"
        )
    except Exception as e:
        logger.error(f"❌ Failed to save pipeline state: {e}")
        raise


@flow(
    name="pipeline-horario",
)
async def pipeline_horario(
    pollutants: List[str] | None = None,
    export_excel: bool = False,
    export_bigquery: bool = False,
    export_motherduck: bool = True,
    incremental: bool = True,
    state_block_name: str = "pipeline-state",
) -> pl.DataFrame:
    """
    Flow principal del pipeline de procesamiento de datos horarios de contaminantes

    Este flow ejecuta el proceso completo ETL usando características nativas de Prefect Cloud:
    1. Extrae datos de InfluxDB para múltiples contaminantes en paralelo (con task caching)
    2. Transforma los datos de formato wide a long
    3. Exporta los datos a los destinos configurados (Excel, BigQuery, MotherDuck)
    4. Gestiona estado incremental usando Prefect Blocks

    Args:
        pollutants: Lista de contaminantes a procesar. Si es None, usa POLLUTANTS_TO_PROCESS
        export_excel: Si True, exporta a Excel
        export_bigquery: Si True, exporta a BigQuery
        export_motherduck: Si True, exporta a MotherDuck
        incremental: Si True, usa procesamiento incremental con estado persistido en Prefect Blocks
        state_block_name: Nombre del Prefect Block para almacenar el estado incremental

    Returns:
        pl.DataFrame: DataFrame consolidado con todos los datos transformados
    """
    logger = get_run_logger()
    settings = get_settings()

    # Use provided pollutants or default from config
    pollutants_to_process = pollutants or POLLUTANTS_TO_PROCESS

    logger.info(
        f"🚀 Starting pipeline execution for {len(pollutants_to_process)} pollutants"
    )
    logger.info(
        f"📋 Configuration: version={settings.PIPELINE_VERSION}, incremental={incremental}, block='{state_block_name}'"
    )

    # Get pipeline state for incremental processing
    state_manager = get_pipeline_state(state_block_name)
    last_processed_hour = None
    if incremental:
        last_processed_hour = await state_manager.get_last_processed_hour()
        if last_processed_hour:
            logger.info(f"📅 Incremental mode: resuming from {last_processed_hour}")
        else:
            logger.info("📅 Incremental mode: first run, no previous state found")

    try:
        # Fetch all data in parallel
        logger.info(
            f"📥 Fetching data for {len(pollutants_to_process)} pollutants from InfluxDB"
        )
        fetch_tasks = [
            fetch_pollutant_data(pollutant, last_processed_hour)
            for pollutant in pollutants_to_process
        ]
        dataframes = await asyncio.gather(*fetch_tasks)

        # Transform all dataframes
        logger.info("🔄 Transforming data to long format")
        transform_tasks = [
            transform_pollutant_data(
                pollutant=pollutant, df=df, pipeline_version=settings.PIPELINE_VERSION
            )
            for pollutant, df in zip(pollutants_to_process, dataframes)
        ]
        transformed_dataframes = transform_tasks

        # Standardize DataFrames: ensure all have same columns and filter out empty ones
        standardized_dataframes = []
        expected_columns = [
            "time",
            "location",
            "metrica",
            "valor",
            "count_ok",
            "version",
        ]

        for df in transformed_dataframes:
            # Skip empty DataFrames
            if df.is_empty() or len(df) == 0:
                continue

            # Ensure all expected columns exist
            for col in expected_columns:
                if col not in df.columns:
                    if col in ["valor", "count_ok"]:
                        df = df.with_columns(
                            pl.lit(
                                None, dtype=pl.Float64 if col == "valor" else pl.UInt32
                            ).alias(col)
                        )
                    else:
                        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

            # Ensure consistent column order and timestamp format
            df = df.select(expected_columns)
            if "time" in df.columns:
                df = df.with_columns(
                    pl.col("time").cast(pl.Datetime("us", "UTC")).alias("time")
                )

            standardized_dataframes.append(df)

        # If no data at all, create empty DataFrame with correct schema
        if not standardized_dataframes:
            contaminantes_horarios = pl.DataFrame(
                {
                    "time": pl.Series(dtype=pl.Datetime("us", "UTC")),
                    "location": pl.Series(dtype=pl.Utf8),
                    "metrica": pl.Series(dtype=pl.Utf8),
                    "valor": pl.Series(dtype=pl.Float64),
                    "count_ok": pl.Series(dtype=pl.UInt32),
                    "version": pl.Series(dtype=pl.Utf8),
                }
            )
        else:
            # Concatenate all transformed dataframes
            contaminantes_horarios = pl.concat(standardized_dataframes)
        logger.info(
            f"📊 Data processing complete: {len(contaminantes_horarios)} total rows"
        )

        # Export to configured destinations
        logger.info("💾 Exporting data to configured destinations")

        # Submit export tasks for concurrent execution using .submit()
        export_futures = []

        # Excel export (sequential, since it's fast and creates output file)
        if export_excel:
            output_path = settings.output_path
            export_to_excel_task(contaminantes_horarios, output_path)

        # BigQuery export (concurrent via .submit())
        if export_bigquery:
            if (
                settings.GOOGLE_PROJECT_ID
                and settings.BIGQUERY_DATASET_ID
                and settings.BIGQUERY_TABLE_ID
            ):
                export_futures.append(
                    export_to_bigquery_task.submit(
                        contaminantes_horarios,
                        settings.GOOGLE_PROJECT_ID,
                        settings.BIGQUERY_DATASET_ID,
                        settings.BIGQUERY_TABLE_ID,
                    )
                )
            else:
                logger.warning("BigQuery export skipped: missing configuration")

        # MotherDuck export (concurrent via .submit())
        if export_motherduck:
            if settings.MOTHERDUCK_DATABASE:
                # Use a default table name or make it configurable
                table_name = f"{settings.MOTHERDUCK_DATABASE}.contaminantes_horarios"
                export_futures.append(
                    export_to_motherduck_task.submit(contaminantes_horarios, table_name)
                )
            else:
                logger.warning("MotherDuck export skipped: missing configuration")

        # Wait for all concurrent export tasks to complete
        if export_futures:
            logger.info(
                f"🚀 Submitted {len(export_futures)} export tasks for concurrent execution"
            )
            wait(export_futures)  # Wait for all futures to complete
            logger.info("✅ All export tasks completed successfully")

        # Save pipeline state (only if incremental and successful)
        if incremental:
            save_pipeline_state()
            logger.info("💾 Pipeline state saved successfully")

        logger.info("✅ Pipeline execution completed successfully")
        return contaminantes_horarios

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        # Cleanup clients
        close_influxdb_client()


if __name__ == "__main__":
    # For local testing
    asyncio.run(pipeline_horario())
