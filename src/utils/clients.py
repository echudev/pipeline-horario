"""
Client management for InfluxDB, BigQuery, and MotherDuck
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional
from influxdb_client_3 import InfluxDBClient3
from google.cloud import bigquery
import duckdb

from config.settings import get_settings
from config.pollutants import TABLE_CONFIG


# Global client instances (lazy initialization)
_influxdb_client: Optional[InfluxDBClient3] = None
_bigquery_client: Optional[bigquery.Client] = None
_motherduck_client: Optional[duckdb.DuckDBPyConnection] = None


def create_influxdb_client() -> InfluxDBClient3:
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


def get_influxdb_client() -> InfluxDBClient3:
    """
    Get or create the global InfluxDB client instance.

    Returns:
        InfluxDBClient3: The global client instance
    """
    global _influxdb_client
    if _influxdb_client is None:
        _influxdb_client = create_influxdb_client()
    return _influxdb_client


def close_influxdb_client() -> None:
    """
    Close the global InfluxDB client instance and reset it to None.
    """
    global _influxdb_client
    if _influxdb_client is not None:
        try:
            _influxdb_client.close()
        except Exception:
            # Ignore errors during cleanup
            pass
        _influxdb_client = None


@contextmanager
def InfluxDBClientManager():
    """
    Context manager for InfluxDB client that ensures cleanup.
    
    Usage:
        with InfluxDBClientManager() as client:
            # use client
    """
    client = get_influxdb_client()
    try:
        yield client
    finally:
        close_influxdb_client()


async def fetch_data(
    client: InfluxDBClient3,
    data_type: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
):
    """
    Fetch data asynchronously based on data type

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch (e.g., 'co', 'nox', 'pm10', 'so2', 'o3', 'meteo')
        start_time: Start time for data range. If None, uses previous hour
        end_time: End time for data range. If None, uses current hour

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

    # Build time filter
    if start_time is None and end_time is None:
        # Legacy behavior: previous hour
        time_filter = "time >= date_trunc('hour', now()) - INTERVAL '1 hour' AND time < date_trunc('hour', now())"
    else:
        # Ensure timezone awareness
        if start_time and start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time and end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        start_iso = start_time.isoformat() if start_time else "1970-01-01T00:00:00Z"
        end_iso = end_time.isoformat() if end_time else "now()"

        time_filter = f"time >= '{start_iso}' AND time < '{end_iso}'"

    query = f"""
            SELECT {columns_str}
            FROM {table_name}
            WHERE {time_filter}
            ORDER BY time ASC
            """

    return await client.query_async(query=query, mode="polars")


async def fetch_incremental_data(client: InfluxDBClient3, data_type: str, last_processed_hour: Optional[datetime] = None):
    """
    Fetch data incrementally from the last processed hour

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch (e.g., 'co', 'nox', 'pm10', 'so2', 'o3', 'meteo')
        last_processed_hour: Last hour that was successfully processed. If None, fetches previous hour

    Returns:
        pl.DataFrame: DataFrame containing the fetched data
    """
    if last_processed_hour is None:
        # First run or no state: fetch previous hour
        return await fetch_data(client, data_type)
    else:
        # Incremental: fetch from last_processed_hour + 1 hour to current hour - 1 hour
        start_time = last_processed_hour.replace(hour=last_processed_hour.hour + 1)
        now = datetime.now(timezone.utc)
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        end_time = current_hour_start  # Up to but not including current hour

        if start_time >= end_time:
            # No new data to fetch
            import polars as pl
            return pl.DataFrame()

        return await fetch_data(client, data_type, start_time, end_time)


async def fetch_specific_hour(client: InfluxDBClient3, data_type: str, target_hour: datetime):
    """
    Fetch data for a specific hour (used for backfilling)

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch
        target_hour: The specific hour to fetch data for

    Returns:
        pl.DataFrame: DataFrame containing data for the specific hour
    """
    # Ensure timezone awareness
    if target_hour.tzinfo is None:
        target_hour = target_hour.replace(tzinfo=timezone.utc)

    start_time = target_hour
    end_time = target_hour.replace(hour=target_hour.hour + 1)

    return await fetch_data(client, data_type, start_time, end_time)


def create_bigquery_client() -> bigquery.Client:
    """
    Create and return a BigQuery client instance using settings from config.
    
    Returns:
        bigquery.Client: Configured BigQuery client
    """
    settings = get_settings()
    return bigquery.Client(project=settings.GOOGLE_PROJECT_ID)


def get_bigquery_client() -> bigquery.Client:
    """
    Get or create the global BigQuery client instance.
    
    Returns:
        bigquery.Client: The global client instance of BigQuery
    """
    global _bigquery_client
    if _bigquery_client is None:
        _bigquery_client = create_bigquery_client()
    return _bigquery_client


def create_motherduck_client() -> duckdb.DuckDBPyConnection:
    """
    Create and return a MotherDuck client instance using settings from config.
    
    Returns:
        DuckDBPyConnection: Configured MotherDuck client
    """
    settings = get_settings()
    return duckdb.connect(f'md:{settings.MOTHERDUCK_DATABASE}?motherduck_token={settings.MOTHERDUCK_TOKEN}')


def get_motherduck_client() -> duckdb.DuckDBPyConnection:
    """
    Get or create the global MotherDuck client instance.

    Returns:
        DuckDBPyConnection: The global client instance of MotherDuck
    """
    global _motherduck_client
    if _motherduck_client is None:
        _motherduck_client = create_motherduck_client()
    return _motherduck_client


# Backfilling functions
async def backfill_missing_hours(
    client: InfluxDBClient3,
    data_type: str,
    start_hour: datetime,
    end_hour: datetime
) -> list:
    """
    Backfill data for missing hours between start_hour and end_hour

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch
        start_hour: First hour to backfill (inclusive)
        end_hour: Last hour to backfill (exclusive)

    Returns:
        List of tuples: [(hour, dataframe), ...] for each successfully fetched hour
    """
    results = []

    # Ensure timezone awareness
    if start_hour.tzinfo is None:
        start_hour = start_hour.replace(tzinfo=timezone.utc)
    if end_hour.tzinfo is None:
        end_hour = end_hour.replace(tzinfo=timezone.utc)

    current_hour = start_hour
    while current_hour < end_hour:
        try:
            df = await fetch_specific_hour(client, data_type, current_hour)
            if len(df) > 0:  # Only include hours with data
                results.append((current_hour, df))
        except Exception as e:
            print(f"Warning: Failed to fetch data for {data_type} at {current_hour}: {e}")
            # Continue with next hour

        current_hour = current_hour.replace(hour=current_hour.hour + 1)

    return results


def find_missing_hours(last_processed_hour: datetime, current_time: datetime) -> list:
    """
    Find hours that are missing between last processed and current time

    Args:
        last_processed_hour: Last hour that was successfully processed
        current_time: Current time

    Returns:
        List of datetime objects representing missing hours
    """
    if last_processed_hour.tzinfo is None:
        last_processed_hour = last_processed_hour.replace(tzinfo=timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    missing_hours = []
    current_hour = last_processed_hour.replace(hour=last_processed_hour.hour + 1)
    current_hour_start = current_time.replace(minute=0, second=0, microsecond=0)

    while current_hour < current_hour_start:
        missing_hours.append(current_hour)
        current_hour = current_hour.replace(hour=current_hour.hour + 1)

    return missing_hours

