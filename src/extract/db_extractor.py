"""
Repository for fetching data from InfluxDB
"""
from typing import Dict, List
from influxdb_client_3 import InfluxDBClient3
from config.settings import get_settings
from config.pollutants import TABLE_CONFIG, POLLUTANTS_TO_PROCESS


def create_client() -> InfluxDBClient3:
    """
    Create and return an InfluxDB client instance using settings from config.
    
    Returns:
        InfluxDBClient3: Configured InfluxDB client
    """
    settings = get_settings()
    return InfluxDBClient3(
        host=settings.INFLUXDB_HOST,
        token=settings.INFLUXDB_TOKEN,
        database=settings.INFLUXDB_DATABASE
    )


# Global client instance (lazy initialization)
_client: InfluxDBClient3 | None = None


def get_influxdb_client() -> InfluxDBClient3:
    """
    Get or create the global InfluxDB client instance.

    Returns:
        InfluxDBClient3: The global client instance
    """
    global _client
    if _client is None:
        _client = create_client()
    return _client


def close_influxdb_client() -> None:
    """
    Close the global InfluxDB client instance and reset it to None.
    """
    global _client
    if _client is not None:
        try:
            _client.close()
        except Exception:
            # Ignore errors during cleanup
            pass
        _client = None


async def fetch_data(client, data_type: str):
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
