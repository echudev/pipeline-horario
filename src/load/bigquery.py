from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import polars as pl
from config.settings import get_settings

def create_bigquery_client() -> bigquery.Client:
    """
    Create and return a BigQuery client instance using settings from config.
    
    Returns:
        bigquery.Client: Configured BigQuery client
    """
    settings = get_settings()
    return bigquery.Client(project=settings.GOOGLE_PROJECT_ID)

# Global client instance (lazy initialization)
_client: bigquery.Client | None = None

def get_bigquery_client() -> bigquery.Client:
    """
    Get or create the global BigQuery client instance.
    
    Returns:
        bigquery.Client: The global client instance of BigQuery
    """
    global _client
    if _client is None:
        _client = create_bigquery_client()
    return _client

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
        table_id: BigQuery table ID
    """
    if df.is_empty():
        print("El DataFrame está vacío. No se exportarán datos a BigQuery.")
        return
    if not all(col in df.columns for col in ["time", "location", "metrica", "valor", "count_ok"]):
        raise ValueError("El DataFrame debe contener las columnas: time, location, metrica, valor, count_ok")
    if not project_id or not dataset_id or not table_id:
        raise ValueError("project_id, dataset_id y table_id son obligatorios para exportar a BigQuery.")
    
    TABLE_FULL_ID = f"{project_id}.{dataset_id}.{table_id}"
    # Inicializar el cliente de BigQuery
    client = bigquery.Client(project=project_id)
    
    # Convert Polars DataFrame to pandas DataFrame
    # BigQuery's load_table_from_dataframe requires pandas
    pandas_df = df.to_pandas()
    
    # Table schema
    # BigQuery puede inferirlo, pero esto da más control.
    schema = [
        bigquery.SchemaField("time", "timestamp", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("metrica", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valor", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("count_ok", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("version_pipeline", "STRING", mode="NULLABLE"),
    ]
    
    # Job configuration
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # write_disposition="WRITE_TRUNCATE",  # Sobrescribe la tabla si existe
        write_disposition="WRITE_APPEND", # Append the data if the table exists
    )
    
    print(f"\nCargando datos en la tabla {TABLE_FULL_ID}...")
    
    # Cargar el DataFrame a BigQuery
    job = client.load_table_from_dataframe(
        pandas_df, TABLE_FULL_ID, job_config=job_config
    )
    
    # Esperar a que el trabajo termine
    job.result()
    
    print(f"¡Éxito! Se cargaron {len(pandas_df)} filas en la tabla {TABLE_FULL_ID}.")
    
    # Verificar la tabla (opcional)
    try:
        table = client.get_table(TABLE_FULL_ID)
        print(f"La tabla {table.table_id} ahora tiene {table.num_rows} filas.")
    except NotFound:
        print("No se pudo verificar la tabla.")
