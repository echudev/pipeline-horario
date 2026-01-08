"""
Client management for InfluxDB, BigQuery, and MotherDuck.

Uses context managers for proper resource cleanup and dependency injection pattern.
"""
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import Optional, List, Tuple, AsyncIterator, Iterator
import logging

from influxdb_client_3 import InfluxDBClient3
from google.cloud import bigquery
import duckdb
import polars as pl

from config.settings import get_settings
from config.pollutants import TABLE_CONFIG
from .datetime_utils import ensure_utc, add_hours, get_previous_hour_start, iter_hours

logger = logging.getLogger(__name__)


class ClientError(Exception):
    """Exception raised when client operations fail."""
    pass


# =============================================================================
# InfluxDB Client
# =============================================================================

def create_influxdb_client() -> InfluxDBClient3:
    """
    Create a new InfluxDB client instance.
    
    Returns:
        InfluxDBClient3: Configured InfluxDB client
    
    Raises:
        ClientError: If client cannot be created
    """
    try:
        settings = get_settings()
        return InfluxDBClient3(
            host=settings.INFLUXDB_HOST,
            token=settings.INFLUXDB_TOKEN,
            database=settings.INFLUXDB_DATABASE
        )
    except Exception as e:
        raise ClientError(f"Failed to create InfluxDB client: {e}") from e


@asynccontextmanager
async def influxdb_client() -> AsyncIterator[InfluxDBClient3]:
    """
    Async context manager for InfluxDB client with automatic cleanup.
    
    Usage:
        async with influxdb_client() as client:
            df = await fetch_data(client, 'co')
    
    Yields:
        InfluxDBClient3: Configured client instance
    """
    client = create_influxdb_client()
    try:
        yield client
    finally:
        try:
            client.close()
        except Exception as e:
            logger.warning(f"Error closing InfluxDB client: {e}")


async def fetch_data(
    client: InfluxDBClient3,
    data_type: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> pl.DataFrame:
    """
    Fetch data from InfluxDB based on data type.

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch (e.g., 'co', 'nox', 'pm10')
        start_time: Start time for data range (UTC)
        end_time: End time for data range (UTC)

    Returns:
        pl.DataFrame: DataFrame containing the fetched data

    Raises:
        KeyError: If data_type is not in TABLE_CONFIG
        ClientError: If query fails
    """
    if data_type not in TABLE_CONFIG:
        raise KeyError(
            f"Unknown data type: {data_type}. "
            f"Available types: {list(TABLE_CONFIG.keys())}"
        )

    config = TABLE_CONFIG[data_type]
    table_name = config['table']
    metrics = config['metrics']

    # Build column selection
    if metrics == ['*']:
        columns_str = '*'
    else:
        base_columns = ['time', 'location']
        all_columns = base_columns + metrics + ['status']
        columns_str = ', '.join(all_columns)

    # Build time filter
    time_filter = _build_time_filter(start_time, end_time)

    query = f"""
        SELECT {columns_str}
        FROM {table_name}
        WHERE {time_filter}
        ORDER BY time ASC
    """

    try:
        return await client.query_async(query=query, mode="polars")
    except Exception as e:
        raise ClientError(f"Failed to fetch {data_type} data: {e}") from e


def _build_time_filter(
    start_time: Optional[datetime], 
    end_time: Optional[datetime]
) -> str:
    """Build SQL time filter clause."""
    if start_time is None and end_time is None:
        # Default: previous hour
        return (
            "time >= date_trunc('hour', now()) - INTERVAL '1 hour' "
            "AND time < date_trunc('hour', now())"
        )
    
    start_time = ensure_utc(start_time)
    end_time = ensure_utc(end_time)
    
    start_iso = start_time.isoformat() if start_time else "1970-01-01T00:00:00Z"
    end_iso = end_time.isoformat() if end_time else "now()"
    
    return f"time >= '{start_iso}' AND time < '{end_iso}'"


async def fetch_incremental_data(
    client: InfluxDBClient3, 
    data_type: str, 
    last_processed_hour: Optional[datetime] = None
) -> pl.DataFrame:
    """
    Fetch data incrementally from the last processed hour.

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch
        last_processed_hour: Last hour that was successfully processed

    Returns:
        pl.DataFrame: DataFrame containing the fetched data (may be empty)
    """
    if last_processed_hour is None:
        return await fetch_data(client, data_type)
    
    start_time = add_hours(ensure_utc(last_processed_hour), 1)
    end_time = get_previous_hour_start()
    
    # Check if there's new data to fetch
    if start_time >= end_time:
        logger.info(f"No new data for {data_type}: start={start_time} >= end={end_time}")
        return pl.DataFrame()
    
    return await fetch_data(client, data_type, start_time, end_time)


async def fetch_specific_hour(
    client: InfluxDBClient3, 
    data_type: str, 
    target_hour: datetime
) -> pl.DataFrame:
    """
    Fetch data for a specific hour (used for backfilling).

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch
        target_hour: The specific hour to fetch data for

    Returns:
        pl.DataFrame: DataFrame containing data for the specific hour
    """
    target_hour = ensure_utc(target_hour)
    start_time = target_hour
    end_time = add_hours(target_hour, 1)
    
    return await fetch_data(client, data_type, start_time, end_time)


async def backfill_hours(
    client: InfluxDBClient3,
    data_type: str,
    start_hour: datetime,
    end_hour: datetime
) -> List[Tuple[datetime, pl.DataFrame]]:
    """
    Backfill data for hours between start_hour and end_hour.

    Args:
        client: InfluxDB client instance
        data_type: Type of data to fetch
        start_hour: First hour to backfill (inclusive)
        end_hour: Last hour to backfill (exclusive)

    Returns:
        List of tuples: [(hour, dataframe), ...] for each hour with data
    """
    results = []
    
    for hour in iter_hours(start_hour, end_hour):
        try:
            df = await fetch_specific_hour(client, data_type, hour)
            if len(df) > 0:
                results.append((hour, df))
                logger.debug(f"Backfilled {data_type} at {hour}: {len(df)} rows")
        except Exception as e:
            logger.warning(f"Failed to fetch {data_type} at {hour}: {e}")
            continue
    
    return results


def find_missing_hours(
    last_processed_hour: datetime, 
    current_time: datetime
) -> List[datetime]:
    """
    Find hours that are missing between last processed and current time.

    Args:
        last_processed_hour: Last hour that was successfully processed
        current_time: Current time

    Returns:
        List of datetime objects representing missing hours
    """
    start = add_hours(ensure_utc(last_processed_hour), 1)
    end = ensure_utc(current_time).replace(minute=0, second=0, microsecond=0)
    
    return list(iter_hours(start, end))


# =============================================================================
# BigQuery Client
# =============================================================================

@contextmanager
def bigquery_client() -> Iterator[bigquery.Client]:
    """
    Context manager for BigQuery client.
    
    Yields:
        bigquery.Client: Configured BigQuery client
    """
    settings = get_settings()
    client = bigquery.Client(project=settings.GOOGLE_PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# =============================================================================
# MotherDuck Client
# =============================================================================

@contextmanager
def motherduck_client() -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Context manager for MotherDuck client.
    
    Yields:
        DuckDBPyConnection: Configured MotherDuck connection
    """
    settings = get_settings()
    conn = duckdb.connect(
        f'md:{settings.MOTHERDUCK_DATABASE}?motherduck_token={settings.MOTHERDUCK_TOKEN}'
    )
    try:
        yield conn
    finally:
        conn.close()


# =============================================================================
# Legacy compatibility (deprecated - use context managers instead)
# =============================================================================

_influxdb_client: Optional[InfluxDBClient3] = None


def get_influxdb_client() -> InfluxDBClient3:
    """
    Get or create global InfluxDB client instance.
    
    DEPRECATED: Use `async with influxdb_client()` context manager instead.
    """
    global _influxdb_client
    if _influxdb_client is None:
        _influxdb_client = create_influxdb_client()
    return _influxdb_client


def close_influxdb_client() -> None:
    """
    Close the global InfluxDB client instance.
    
    DEPRECATED: Use `async with influxdb_client()` context manager instead.
    """
    global _influxdb_client
    if _influxdb_client is not None:
        try:
            _influxdb_client.close()
        except Exception as e:
            logger.warning(f"Error closing InfluxDB client: {e}")
        _influxdb_client = None


def get_bigquery_client() -> bigquery.Client:
    """
    Create BigQuery client.
    
    DEPRECATED: Use `with bigquery_client()` context manager instead.
    """
    settings = get_settings()
    return bigquery.Client(project=settings.GOOGLE_PROJECT_ID)


def get_motherduck_client() -> duckdb.DuckDBPyConnection:
    """
    Create MotherDuck client.
    
    DEPRECATED: Use `with motherduck_client()` context manager instead.
    """
    settings = get_settings()
    return duckdb.connect(
        f'md:{settings.MOTHERDUCK_DATABASE}?motherduck_token={settings.MOTHERDUCK_TOKEN}'
    )


# Legacy alias
InfluxDBClientManager = influxdb_client
