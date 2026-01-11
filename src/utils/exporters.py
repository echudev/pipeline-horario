"""
Exporters for pipeline output to various destinations.
"""
import logging
import os
from pathlib import Path
from typing import Optional

import polars as pl
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import duckdb

from config.settings import get_settings
from .schema import OUTPUT_COLUMNS, get_bigquery_schema

logger = logging.getLogger(__name__)


class ExportError(Exception):
    """Exception raised when export operations fail."""
    pass


# =============================================================================
# Excel Export
# =============================================================================

def export_to_excel(df: pl.DataFrame, output_path: str) -> None:
    """
    Export DataFrame to Excel file.

    Args:
        df: DataFrame to export
        output_path: Full path to output file

    Raises:
        ExportError: If export fails
    """
    if df.is_empty():
        logger.warning("DataFrame is empty, skipping Excel export")
        return

    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting {len(df)} rows to {output_path}")

    try:
        # Excel doesn't support timezones, convert to naive datetime
        df_for_excel = df.clone()
        for col in df_for_excel.columns:
            if df_for_excel[col].dtype == pl.Datetime('us', 'UTC'):
                df_for_excel = df_for_excel.with_columns(
                    pl.col(col).dt.replace_time_zone(None).alias(col)
                )

        df_for_excel.write_excel(output_path)
        logger.info(f"Successfully exported to {output_path}")
    except Exception as e:
        raise ExportError(f"Failed to export to Excel: {e}") from e


# =============================================================================
# BigQuery Export
# =============================================================================

def _get_bigquery_client(project_id: str) -> bigquery.Client:
    """
    Get BigQuery client with proper credential handling.
    
    Tries in order:
    1. Prefect GCP credentials block
    2. Service account key file
    3. Default credentials
    """
    # Try Prefect GCP credentials
    try:
        from prefect_gcp import GcpCredentials
        gcp_credentials = GcpCredentials.load("gcp-credentials")
        logger.info("Using Prefect GCP credentials for BigQuery")
        return gcp_credentials.get_bigquery_client()
    except Exception as e:
        logger.debug(f"Prefect GCP credentials not available: {e}")

    # Try service account key file
    key_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "secrets", "service-account-key.json"
    )
    if os.path.exists(key_path):
        logger.info("Using service account key file for BigQuery")
        return bigquery.Client.from_service_account_json(key_path)

    # Fall back to default credentials
    logger.info("Using default credentials for BigQuery")
    return bigquery.Client(project=project_id)


def export_to_bigquery(
    project_id: Optional[str],
    dataset_id: Optional[str],
    table_id: Optional[str],
    df: pl.DataFrame,
) -> None:
    """
    Export DataFrame to BigQuery table.
    
    Args:
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        df: Polars DataFrame to export
    
    Raises:
        ExportError: If export fails
        ValueError: If required parameters are missing
    """
    # Validate inputs
    if df.is_empty():
        logger.warning("DataFrame is empty, skipping BigQuery export")
        return
    
    if not all([project_id, dataset_id, table_id]):
        raise ValueError("project_id, dataset_id, and table_id are required")
    
    # Validate schema
    missing_cols = [col for col in OUTPUT_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        client = _get_bigquery_client(project_id)
    except Exception as e:
        raise ExportError(f"Failed to initialize BigQuery client: {e}") from e

    try:
        # Convert to pandas (required by BigQuery client)
        pandas_df = df.to_pandas()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=get_bigquery_schema(),
        )
        
        logger.info(f"Loading {len(pandas_df)} rows to {table_full_id}")
        
        job = client.load_table_from_dataframe(
            pandas_df, table_full_id, job_config=job_config
        )
        job.result()  # Wait for completion
        
        logger.info(f"Successfully loaded data to {table_full_id}")
        
        # Verify
        try:
            table = client.get_table(table_full_id)
            logger.info(f"Table {table.table_id} now has {table.num_rows} rows")
        except NotFound:
            logger.warning("Could not verify table after load")
            
    except Exception as e:
        raise ExportError(f"Failed to export to BigQuery: {e}") from e


# =============================================================================
# MotherDuck Export
# =============================================================================

def export_to_motherduck(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pl.DataFrame,
) -> None:
    """
    Export DataFrame to MotherDuck table.
    
    Args:
        connection: MotherDuck connection
        table_name: Full table name (database.table)
        df: Polars DataFrame to export
    
    Raises:
        ExportError: If export fails
    """
    if df.is_empty():
        logger.warning("DataFrame is empty, skipping MotherDuck export")
        return

    logger.info(f"Loading {len(df)} rows to {table_name}")

    try:
        # Convert to pandas for DuckDB compatibility
        df_pandas = df.to_pandas()

        # Create table if not exists
        connection.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                time TIMESTAMP NOT NULL,
                location VARCHAR NOT NULL,
                metrica VARCHAR NOT NULL,
                valor FLOAT,
                count_ok INTEGER NOT NULL,
                version VARCHAR
            )
        """)

        # Insert data
        connection.register("temp_data", df_pandas)
        connection.sql(f"INSERT INTO {table_name} SELECT * FROM temp_data")
        connection.unregister("temp_data")
        
        logger.info(f"Successfully loaded data to {table_name}")
        
    except Exception as e:
        raise ExportError(f"Failed to export to MotherDuck: {e}") from e
