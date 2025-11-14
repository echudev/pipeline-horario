"""
Repository for fetching data from BigQuery
"""
from typing import Any
import polars as pl
from google.cloud import bigquery
from config import get_settings


def fetch_data(
    client: bigquery.Client,
    query: str,
    use_legacy_sql: bool = False
) -> pl.DataFrame:
    """
    Fetch data synchronously from BigQuery using a SQL query
    
    Args:
        client: BigQuery client instance
        query: SQL query string to execute
        use_legacy_sql: Whether to use legacy SQL syntax (default: False)
    
    Returns:
        pl.DataFrame: DataFrame containing the fetched data
    
    Raises:
        Exception: If the query execution fails
    """
    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(use_legacy_sql=use_legacy_sql))
        results = query_job.result()
        
        # Convert results to list of dictionaries, then to Polars DataFrame
        rows = []
        for row in results:
            rows.append(dict[Any, Any](row))
        
        if rows:
            return pl.DataFrame(rows)
        else:
            # Return empty DataFrame with schema if no results
            return pl.DataFrame()
    except Exception as e:
        raise Exception(f"Error fetching data from BigQuery: {e}")


def fetch_table_data(
    client: bigquery.Client,
    table_id: str = None,
    dataset_id: str = None,
    project_id: str = None,
    limit: int = None
) -> pl.DataFrame:
    """
    Fetch all data from a BigQuery table
    
    Args:
        client: BigQuery client instance
        table_id: Table ID (if None, uses settings.BIGQUERY_TABLE_ID)
        dataset_id: Dataset ID (if None, uses settings.BIGQUERY_DATASET_ID)
        project_id: Project ID (if None, uses settings.BIGQUERY_PROJECT_ID)
        limit: Optional limit on number of rows to fetch
    
    Returns:
        pl.DataFrame: DataFrame containing the table data
    """
    settings = get_settings()
    
    project_id = project_id or settings.BIGQUERY_PROJECT_ID
    dataset_id = dataset_id or settings.BIGQUERY_DATASET_ID
    table_id = table_id or settings.BIGQUERY_TABLE_ID
    
    if not all([project_id, dataset_id, table_id]):
        raise ValueError("Project ID, Dataset ID, and Table ID must be provided or set in settings")
    
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT * FROM `{full_table_id}` {limit_clause}"
    
    return fetch_data(client, query)

