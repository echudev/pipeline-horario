"""
Pytest configuration and shared fixtures
"""
import pytest
import polars as pl
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from config.settings import Settings
from src.extract import db_extractor


@pytest.fixture
def sample_pollutant_data():
    """Create sample pollutant data for testing"""
    base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # Create time series data
    times = [base_time + timedelta(minutes=i*10) for i in range(6)]

    return pl.DataFrame({
        'time': times,
        'location': ['Centro'] * 6,
        'status': ['k'] * 6,
        'co_mean': [0.5, 1.2, 2.1, 0.8, 1.5, 1.8],
        'no_mean': [25.3, 45.7, 12.1, 78.9, 34.2, 56.1],
        'no2_mean': [15.2, 28.7, 8.1, 45.9, 22.2, 31.1],
        'nox_mean': [40.5, 74.4, 20.2, 124.8, 56.4, 87.2],
        'pm10_mean': [15.2, 28.7, 45.1, 12.3, 33.8, 22.4],
        'so2_mean': [3.2, 7.8, 1.5, 12.3, 5.6, 8.9],
        'o3_mean': [45.2, 67.8, 23.1, 89.4, 34.7, 52.3]
    })


@pytest.fixture
def mock_settings():
    """Create mock settings for testing"""
    settings = Mock(spec=Settings)
    settings.INFLUXDB_HOST = "http://test-influx:8086"
    settings.INFLUXDB_TOKEN = "test-token"
    settings.INFLUXDB_DATABASE = "test-db"
    settings.GOOGLE_PROJECT_ID = "test-project"
    settings.BIGQUERY_DATASET_ID = "test-dataset"
    settings.BIGQUERY_TABLE_ID = "test-table"
    settings.PIPELINE_VERSION = "test-v1.0"
    settings.LOG_LEVEL = "INFO"
    return settings


@pytest.fixture
def transformed_sample_data():
    """Create sample transformed data"""
    base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    return pl.DataFrame({
        'time': [base_time],
        'location': ['Centro'],
        'metrica': ['co'],
        'valor': [1.2],
        'count_ok': [6],
        'version_pipeline': ['test-v1.0']
    })


@pytest.fixture(autouse=True)
def cleanup_influxdb_client():
    """Automatically clean up InfluxDB client after each test"""
    yield
    # Clean up after each test
    db_extractor.close_influxdb_client()
