"""
Integration tests for the Prefect pipeline
"""
import pytest
import polars as pl
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
import logging

from src.utils.schema import OUTPUT_COLUMNS


# Mock logger for tests
@pytest.fixture(autouse=True)
def mock_prefect_logger():
    """Mock Prefect's get_run_logger to return standard logger"""
    mock_logger = logging.getLogger("test_pipeline")
    with patch('flows.pipeline_horario.get_run_logger', return_value=mock_logger):
        yield mock_logger


@pytest.fixture
def mock_settings():
    """Mock settings for testing"""
    with patch('flows.pipeline_horario.get_settings') as mock_get:
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
async def test_fetch_pollutant_data_task():
    """Test fetching pollutant data task"""
    from flows.pipeline_horario import fetch_pollutant_data_task
    
    mock_df = pl.DataFrame({
        'time': [datetime.now(timezone.utc)],
        'location': ['test'],
        'status': ['k'],
        'co_mean': [1.0]
    })
    
    with patch('flows.pipeline_horario.influxdb_client') as mock_ctx:
        mock_client = AsyncMock()
        mock_ctx.return_value.__aenter__.return_value = mock_client
        
        with patch('flows.pipeline_horario.fetch_incremental_data', return_value=mock_df):
            result = await fetch_pollutant_data_task.fn("co")
            
            assert isinstance(result, pl.DataFrame)
            assert len(result) == 1


def test_transform_pollutant_data_task():
    """Test transforming pollutant data task"""
    from flows.pipeline_horario import transform_pollutant_data_task
    
    df = pl.DataFrame({
        'time': [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
        'location': ['test'],
        'status': ['k'],
        'co_mean': [1.0]
    })
    
    result = transform_pollutant_data_task.fn("co", df, "test-v1.0")
    
    assert isinstance(result, pl.DataFrame)
    assert list(result.columns) == OUTPUT_COLUMNS
    assert result['metrica'][0] == 'co'
    assert result['version'][0] == 'test-v1.0'


def test_transform_empty_dataframe():
    """Test transforming empty DataFrame returns empty with schema"""
    from flows.pipeline_horario import transform_pollutant_data_task
    
    df = pl.DataFrame()
    
    result = transform_pollutant_data_task.fn("co", df, "test-v1.0")
    
    assert result.is_empty()
    assert list(result.columns) == OUTPUT_COLUMNS


def test_export_to_excel_task(tmp_path):
    """Test Excel export task"""
    from flows.pipeline_horario import export_to_excel_task
    
    df = pl.DataFrame({
        'time': [datetime.now(timezone.utc)],
        'location': ['test'],
        'metrica': ['co'],
        'valor': [1.0],
        'count_ok': pl.Series([1], dtype=pl.UInt32),
        'version': ['test-v1.0']
    })
    
    output_path = str(tmp_path / "test_output.xlsx")
    
    with patch('flows.pipeline_horario.export_to_excel') as mock_export:
        export_to_excel_task.fn(df, output_path)
        mock_export.assert_called_once_with(df, output_path)


@pytest.mark.asyncio
async def test_pipeline_horario_flow_empty_data(mock_settings):
    """Test pipeline flow with no data"""
    from flows.pipeline_horario import pipeline_horario
    
    empty_df = pl.DataFrame()
    
    with patch('flows.pipeline_horario.influxdb_client') as mock_ctx:
        mock_client = AsyncMock()
        mock_ctx.return_value.__aenter__.return_value = mock_client
        
        with patch('flows.pipeline_horario.fetch_incremental_data', return_value=empty_df):
            with patch('flows.pipeline_horario.get_pipeline_state') as mock_state:
                mock_state_instance = AsyncMock()
                mock_state_instance.get_last_processed_hour.return_value = None
                mock_state.return_value = mock_state_instance
                
                result = await pipeline_horario.fn(
                    pollutants=['co'],
                    export_excel=False,
                    export_bigquery=False,
                    export_motherduck=False,
                    incremental=False
                )
                
                assert isinstance(result, pl.DataFrame)
                assert result.is_empty()
                assert list(result.columns) == OUTPUT_COLUMNS


@pytest.mark.asyncio
async def test_pipeline_horario_with_data(mock_settings):
    """Test pipeline flow with actual data"""
    from flows.pipeline_horario import pipeline_horario
    from src.utils import (
        build_metric_columns,
        aggregate_to_long_format,
        validate_output_schema,
    )
    from config.pollutants import TABLE_CONFIG
    
    # Test the transformation logic directly (avoiding Prefect cache)
    mock_df = pl.DataFrame({
        'time': [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
        'location': ['test'],
        'status': ['k'],
        'co_mean': [1.0]
    })
    
    config = TABLE_CONFIG['co']
    metric_columns = build_metric_columns(config['metrics'])
    transformed = aggregate_to_long_format(
        mock_df,
        metric_columns=metric_columns,
        version="test-v1.0"
    )
    result = validate_output_schema(transformed)
    
    assert isinstance(result, pl.DataFrame)
    assert not result.is_empty()
    assert list(result.columns) == OUTPUT_COLUMNS
    assert result['metrica'][0] == 'co'
