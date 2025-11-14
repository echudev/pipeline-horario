from influxdb_client_3 import InfluxDBClient3
from config import get_settings


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
