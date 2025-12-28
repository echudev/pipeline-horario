import duckdb
import polars as pl
from config.settings import get_settings


def create_motherduck_client() -> duckdb.DuckDBPyConnection:
    """
    Create and return a MotherDuck client instance using settings from config.
    
    Returns:
        DuckDBPyConnection: Configured MotherDuck client
    """
    settings = get_settings()
    return duckdb.connect(f'md:{settings.MOTHERDUCK_DATABASE}?motherduck_token={settings.MOTHERDUCK_TOKEN}')

# Global client instance (lazy initialization)
_client: duckdb.DuckDBPyConnection | None = None

def get_motherduck_client() -> duckdb.DuckDBPyConnection:
    """
    Get or create the global MotherDuck client instance.
    
    Returns:
        DuckDBPyConnection: The global client instance of MotherDuck
    """
    global _client
    if _client is None:
        _client = create_motherduck_client()
        print(f"Connected to MotherDuck database")
    return _client


def export_to_motherduck(
    motherduck_client,
    table_name: str,
    df,
    ) -> None:
    """
    Export DataFrame to MotherDuck table
    
    Args:
        df: Polars DataFrame to export
        table_name: MotherDuck table name
    """
    print(f"\nCargando datos en la tabla {table_name}...")

    # Seleccionar orden y castear tipos explícitamente
    df = df.select([
        pl.col("time").cast(pl.Datetime("ns")).alias("time"),
        pl.col("location").cast(pl.Utf8()).alias("location"),
        pl.col("metrica").cast(pl.Utf8()).alias("metrica"),
        pl.col("valor").cast(pl.Float64()).alias("valor"),
        pl.col("count_ok").cast(pl.Int64()).alias("count_ok"),
        pl.col("version_pipeline").cast(pl.Utf8()).alias("version_pipeline"),
    ])
    
    # Convert Polars DataFrame to Arrow Table
    arrow_table = df.to_arrow()
    
    # Cargar el Arrow Table a MotherDuck
    # motherduck_client.sql("INSERT INTO my_table SELECT * FROM 'arrow_table'", arrow_table)
    motherduck_client.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP NOT NULL,
            location VARCHAR NOT NULL,
            metrica VARCHAR NOT NULL,
            valor FLOAT,
            count_ok INTEGER NOT NULL,
            version_pipeline VARCHAR
        )
    """)
    
    motherduck_client.sql(
        f"INSERT INTO {table_name} SELECT * FROM arrow_table", 
        params=arrow_table
    )
    print(f"Datos insertados exitosamente en {table_name}")
                          