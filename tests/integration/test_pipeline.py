"""
Integration tests for the Prefect pipeline
"""
import pytest
import polars as pl
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

from flows.pipeline_horario import (
    pipeline_horario,
    fetch_pollutant_data,
    transform_pollutant_data,
    export_to_excel_task,
)


@pytest.fixture
def mock_influxdb_client():
    """Mock InfluxDB client"""
    with patch('src.utils.clients.get_influxdb_client') as mock_get:
        mock_client = Mock()
        mock_get.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_settings():
    """Mock settings for testing"""
    with patch('config.settings.get_settings') as mock_get:
        settings = Mock()
        settings.PIPELINE_VERSION = "test-v1.0"
        settings.OUTPUT_DIR = "output"
        settings.OUTPUT_FILENAME = "test_output.xlsx"
        settings.GOOGLE_PROJECT_ID = "test-project"
        settings.BIGQUERY_DATASET_ID = "test-dataset"
        settings.BIGQUERY_TABLE_ID = "test-table"
        settings.MOTHERDUCK_DATABASE = "test-db"
        settings.output_path = "output/test_output.xlsx"
        mock_get.return_value = settings
        yield settings


@pytest.mark.asyncio
async def test_fetch_pollutant_data(mock_influxdb_client):
    """Test fetching pollutant data task"""
    # Mock the fetch_data function
    with patch('src.utils.clients.fetch_data') as mock_fetch:
        mock_df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k'],
            'co_mean': [1.0]
        })
        mock_fetch.return_value = mock_df
        
        result = await fetch_pollutant_data("co")
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        mock_fetch.assert_called_once()


def test_transform_pollutant_data():
    """Test transforming pollutant data task"""
    df = pl.DataFrame({
        'time': [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
        'location': ['test'],
        'status': ['k'],
        'co_mean': [1.0]
    })
    
    result = transform_pollutant_data("co", df, "test-v1.0")
    
    assert isinstance(result, pl.DataFrame)
    assert 'metrica' in result.columns
    assert 'valor' in result.columns
    assert 'version_pipeline' in result.columns


def test_export_to_excel_task(mock_settings, tmp_path):
    """Test Excel export task"""
    df = pl.DataFrame({
        'time': [datetime.now(timezone.utc)],
        'location': ['test'],
        'metrica': ['co'],
        'valor': [1.0],
        'count_ok': [1],
        'version_pipeline': ['test-v1.0']
    })
    
    output_path = str(tmp_path / "test_output.xlsx")
    
    with patch('src.utils.exporters.export_to_excel') as mock_export:
        export_to_excel_task(df, output_path)
        mock_export.assert_called_once_with(df, output_path)


@pytest.mark.asyncio
async def test_pipeline_horario_flow(mock_settings, mock_influxdb_client):
    """Test the main pipeline flow"""
    # Mock fetch_data to return sample data
    with patch('src.utils.clients.fetch_data') as mock_fetch:
        mock_df = pl.DataFrame({
            'time': [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
            'location': ['test'],
            'status': ['k'],
            'co_mean': [1.0]
        })
        mock_fetch.return_value = mock_df
        
        # Mock exporters
        with patch('flows.pipeline_horario.export_to_excel_task') as mock_excel:
            with patch('src.utils.clients.close_influxdb_client'):
                result = await pipeline_horario(
                    pollutants=['co'],
                    export_excel=True,
                    export_bigquery=False,
                    export_motherduck=False
                )
                
                assert isinstance(result, pl.DataFrame)
                mock_excel.assert_called_once()
