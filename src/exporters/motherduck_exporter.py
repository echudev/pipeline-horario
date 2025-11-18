import polars as pl

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
                          