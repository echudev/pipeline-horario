"""
Exporters for pipeline output
"""
import logging
import polars as pl
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import duckdb

from config.settings import get_settings

# Prefect GCP integration
try:
    from prefect_gcp import GcpCredentials
    PREFECT_GCP_AVAILABLE = True
except ImportError:
    PREFECT_GCP_AVAILABLE = False
    logging.warning("prefect-gcp not available, falling back to standard BigQuery client")

logger = logging.getLogger(__name__)


def export_to_excel(df: pl.DataFrame, output_path: str) -> None:
    """
    Export DataFrame to Excel file

    Args:
        df: DataFrame to export
        output_path: Full path to output file (including directory and filename)

    Raises:
        OSError: If the output directory cannot be created
        Exception: If the export fails
    """
    # Create output directory if it doesn't exist
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting {len(df)} rows to {output_path}")

    try:
        # Excel doesn't support timezones, so we need to remove them
        df_for_excel = df.clone()
        for col in df_for_excel.columns:
            if df_for_excel[col].dtype == pl.Datetime('us', 'UTC'):
                # Convert timezone-aware datetime to naive datetime
                df_for_excel = df_for_excel.with_columns(
                    pl.col(col).dt.replace_time_zone(None).alias(col)
                )

        df_for_excel.write_excel(output_path)
        logger.info(f"Successfully exported data to {output_path}")
    except Exception as e:
        logger.error(f"Failed to export data to {output_path}: {e}")
        raise


def export_to_bigquery(
    project_id: str | None,
    dataset_id: str | None,
    table_id: str | None,
    df: pl.DataFrame,
) -> None:
    """
    Export DataFrame to BigQuery table
    
    Args:
        df: Polars DataFrame to export
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    
    Raises:
        ValueError: If required parameters are missing or DataFrame is invalid
    """
    if df.is_empty():
        logger.warning("El DataFrame está vacío. No se exportarán datos a BigQuery.")
        return
    if not all(col in df.columns for col in ["time", "location", "metrica", "valor", "count_ok"]):
        raise ValueError("El DataFrame debe contener las columnas: time, location, metrica, valor, count_ok")
    if not project_id or not dataset_id or not table_id:
        raise ValueError("project_id, dataset_id y table_id son obligatorios para exportar a BigQuery.")
    
    TABLE_FULL_ID = f"{project_id}.{dataset_id}.{table_id}"

    # Inicializar el cliente de BigQuery usando Prefect GCP o service account key
    try:
        if PREFECT_GCP_AVAILABLE:
            # Usar GcpCredentials de Prefect GCP
            try:
                gcp_credentials = GcpCredentials.load("gcp-credentials")
                client = gcp_credentials.get_bigquery_client()
                logger.info("Using Prefect GCP credentials for BigQuery")
            except Exception as e:
                logger.warning(f"Could not load Prefect GCP credentials: {e}")
                logger.warning("Falling back to service account key file")
                # Intentar usar el archivo de service account key
                import os
                key_path = os.path.join(os.path.dirname(__file__), "..", "..", "secrets", "service-account-key.json")
                if os.path.exists(key_path):
                    client = bigquery.Client.from_service_account_json(key_path)
                    logger.info("Using service account key file for BigQuery")
                else:
                    raise Exception("Service account key file not found")
        else:
            # Fallback: intentar usar service account key directamente
            import os
            key_path = os.path.join(os.path.dirname(__file__), "..", "..", "secrets", "service-account-key.json")
            if os.path.exists(key_path):
                client = bigquery.Client.from_service_account_json(key_path)
                logger.info("Using service account key file for BigQuery")
            else:
                # Último fallback al cliente estándar
                client = bigquery.Client(project=project_id)
                logger.warning("Using standard BigQuery client (may fail without credentials)")

        # Test the client connection
        try:
            client.get_service_account_email()
        except Exception as e:
            logger.warning(f"Could not verify BigQuery credentials: {e}")
            # No fallar aquí, continuar con la exportación

    except Exception as e:
        logger.warning(f"No se pudieron inicializar las credenciales de BigQuery: {e}")
        logger.warning("La exportación a BigQuery será omitida. Verifica que las credenciales de Google Cloud estén configuradas correctamente.")
        return
    
    # Convert Polars DataFrame to pandas DataFrame
    # BigQuery's load_table_from_dataframe requires pandas
    pandas_df = df.to_pandas()
    
    # Table schema
    # BigQuery puede inferirlo, pero esto da más control.
    schema = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("metrica", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valor", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("count_ok", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    ]
    
    # Job configuration
    # Note: Using WRITE_TRUNCATE to replace the table with new schema if needed
    # Remove explicit schema to let BigQuery infer it from the data
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace the table data
        # schema=schema,  # Let BigQuery infer the schema automatically
    )
    
    logger.info(f"Cargando datos en la tabla {TABLE_FULL_ID}...")
    
    # Cargar el DataFrame a BigQuery
    job = client.load_table_from_dataframe(
        pandas_df, TABLE_FULL_ID, job_config=job_config
    )
    
    # Esperar a que el trabajo termine
    job.result()
    
    logger.info(f"¡Éxito! Se cargaron {len(pandas_df)} filas en la tabla {TABLE_FULL_ID}.")
    
    # Verificar la tabla (opcional)
    try:
        table = client.get_table(TABLE_FULL_ID)
        logger.info(f"La tabla {table.table_id} ahora tiene {table.num_rows} filas.")
    except NotFound:
        logger.warning("No se pudo verificar la tabla.")


def export_to_motherduck(
    motherduck_client: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pl.DataFrame,
) -> None:
    """
    Export DataFrame to MotherDuck table
    
    Args:
        motherduck_client: MotherDuck client connection
        table_name: MotherDuck table name
        df: Polars DataFrame to export
    
    Raises:
        Exception: If the export fails
    """
    logger.info(f"Cargando datos en la tabla {table_name}...")

    # Convertir a pandas para compatibilidad con DuckDB
    df_pandas = df.to_pandas()

    # Crear tabla si no existe
    motherduck_client.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP NOT NULL,
            location VARCHAR NOT NULL,
            metrica VARCHAR NOT NULL,
            valor FLOAT,
            count_ok INTEGER NOT NULL,
            version VARCHAR
        )
    """)

    # Registrar el DataFrame como tabla temporal y hacer la inserción
    motherduck_client.register("temp_data", df_pandas)
    motherduck_client.sql(f"INSERT INTO {table_name} SELECT * FROM temp_data")
    logger.info(f"Datos insertados exitosamente en {table_name}")

