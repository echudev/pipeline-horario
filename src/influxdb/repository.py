"""
Repository for fetching data from InfluxDB
"""
import polars as pl
from config.pollutants import TABLE_CONFIG


async def fetch_data(client, data_type: str) -> pl.DataFrame:
    """
    Fetch data asynchronously based on data type
    
    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch (e.g., 'co', 'nox', 'pm10', 'so2', 'o3', 'meteo')
    
    Returns:
        pl.DataFrame: DataFrame containing the fetched data
    
    Raises:
        KeyError: If data_type is not in TABLE_CONFIG
    """
    if data_type not in TABLE_CONFIG:
        raise KeyError(f"Unknown data type: {data_type}. Available types: {list(TABLE_CONFIG.keys())}")
    
    config = TABLE_CONFIG[data_type]
    table_name = config['table']
    metrics = config['metrics']
    
    # Handle special case for meteo (all columns)
    if metrics == ['*']:
        columns_str = '*'
    else:
        base_columns = ['time', 'location']
        all_columns = base_columns + metrics + ['status']
        columns_str = ', '.join(all_columns)
    
    query = f"""
            SELECT {columns_str}
            FROM {table_name}
            WHERE time >= date_trunc('hour', now()) - INTERVAL '1 hour'
            AND time < date_trunc('hour', now()) 
            ORDER BY time ASC
            """
    
    return await client.query_async(query=query, mode="polars")
