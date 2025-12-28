"""
Integration tests for the complete pipeline
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import polars as pl
from datetime import datetime, timezone

from src.orchestation.pipeline import PipelineOrchestrator


class TestPipelineIntegration:
    """Integration tests for the complete ETL pipeline"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing"""
        mock = Mock()
        mock.PIPELINE_VERSION = "test-v1.0"
        mock.LOG_LEVEL = "INFO"
        return mock

    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for CO testing"""
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        return pl.DataFrame({
            'time': [
                base_time,
                base_time.replace(minute=10),
                base_time.replace(minute=20)
            ],
            'location': ['loc1', 'loc1', 'loc1'],
            'status': ['k', 'k', 'k'],
            'co_mean': [1.0, 1.5, 2.0]
        })
    
    @pytest.fixture
    def sample_dataframes_by_pollutant(self):
        """Create sample DataFrames for each pollutant"""
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        times = [
            base_time,
            base_time.replace(minute=10),
            base_time.replace(minute=20)
        ]
        common_data = {
            'time': times,
            'location': ['loc1', 'loc1', 'loc1'],
            'status': ['k', 'k', 'k']
        }
        
        return {
            'co': pl.DataFrame({**common_data, 'co_mean': [1.0, 1.5, 2.0]}),
            'nox': pl.DataFrame({**common_data, 'no_mean': [10.0, 15.0, 20.0], 'no2_mean': [5.0, 7.5, 10.0], 'nox_mean': [15.0, 22.5, 30.0]}),
            'pm10': pl.DataFrame({**common_data, 'pm10_mean': [25.0, 30.0, 35.0]}),
            'so2': pl.DataFrame({**common_data, 'so2_mean': [5.0, 6.0, 7.0]}),
            'o3': pl.DataFrame({**common_data, 'o3_mean': [40.0, 45.0, 50.0]}),
            'meteo': pl.DataFrame({**common_data, 'dv_mean': [180.0, 185.0, 190.0], 'vv_mean': [5.0, 6.0, 7.0], 'temp_mean': [20.0, 21.0, 22.0], 'hr_mean': [65.0, 67.0, 70.0], 'pa_mean': [1013.0, 1012.0, 1014.0], 'uv_mean': [3.0, 4.0, 5.0], 'lluvia_mean': [0.0, 0.1, 0.2], 'rs_mean': [200.0, 250.0, 300.0]})
        }

    @pytest.mark.asyncio
    async def test_fetch_all_data_success(self, mock_settings, sample_dataframes_by_pollutant):
        """Test successful data fetching for all pollutants"""
        with patch('src.orchestation.pipeline.get_settings', return_value=mock_settings), \
             patch('src.orchestation.pipeline.bigquery.get_bigquery_client', return_value=Mock()), \
             patch('src.orchestation.pipeline.influxdb.fetch_data', new_callable=AsyncMock) as mock_fetch:

            # Setup mocks - return appropriate dataframe based on pollutant
            async def fetch_side_effect(client, pollutant):
                return sample_dataframes_by_pollutant[pollutant]
            
            mock_fetch.side_effect = fetch_side_effect

            orchestrator = PipelineOrchestrator()

            # Test fetch_all_data
            result = await orchestrator.fetch_all_data()

            # Verify results
            assert len(result) == 6  # Should fetch for all pollutants (including meteo)
            assert all(isinstance(df, pl.DataFrame) for df in result)

            # Verify fetch_data was called for each pollutant
            assert mock_fetch.call_count == 6

    def test_transform_all_data_success(self, mock_settings, sample_dataframes_by_pollutant):
        """Test successful data transformation"""
        with patch('src.orchestation.pipeline.get_settings', return_value=mock_settings), \
             patch('src.orchestation.pipeline.bigquery.get_bigquery_client'):

            orchestrator = PipelineOrchestrator()

            # Use proper dataframes for each pollutant
            pollutants = ['co', 'nox', 'pm10', 'so2', 'o3']
            dataframes = [sample_dataframes_by_pollutant[p] for p in pollutants]

            # Test transform_all_data
            result = orchestrator.transform_all_data(pollutants, dataframes)

            # Verify results
            assert isinstance(result, pl.DataFrame)
            assert result.height > 0

            # Check required columns
            required_cols = ['time', 'location', 'metrica', 'valor', 'count_ok', 'version_pipeline']
            for col in required_cols:
                assert col in result.columns

    @pytest.mark.asyncio
    async def test_pipeline_run_success(self, mock_settings, sample_dataframes_by_pollutant):
        """Test complete pipeline run"""
        with patch('src.orchestation.pipeline.get_settings', return_value=mock_settings), \
             patch('src.orchestation.pipeline.influxdb.get_influxdb_client'), \
             patch('src.orchestation.pipeline.bigquery.get_bigquery_client'), \
             patch('src.orchestation.pipeline.influxdb.fetch_data', new_callable=AsyncMock) as mock_fetch, \
             patch('src.orchestation.pipeline.bigquery.export_to_bigquery') as mock_export:

            # Setup mocks - return appropriate dataframe based on pollutant
            async def fetch_side_effect(client, pollutant):
                return sample_dataframes_by_pollutant[pollutant]
            
            mock_fetch.side_effect = fetch_side_effect

            orchestrator = PipelineOrchestrator()

            # Run pipeline
            await orchestrator.run()

            # Verify export was called
            mock_export.assert_called_once()

            # Verify it was called with correct parameters
            call_args = mock_export.call_args
            assert call_args[1]['df'].height > 0  # DataFrame was passed

    @pytest.mark.asyncio
    async def test_pipeline_run_with_fetch_error(self, mock_settings):
        """Test pipeline handles fetch errors gracefully"""
        with patch('src.orchestation.pipeline.get_settings', return_value=mock_settings), \
             patch('src.orchestation.pipeline.influxdb.get_influxdb_client'), \
             patch('src.orchestation.pipeline.bigquery.get_bigquery_client'), \
             patch('src.orchestation.pipeline.influxdb.fetch_data', new_callable=AsyncMock) as mock_fetch:

            # Setup mock to raise exception
            mock_fetch.side_effect = Exception("Database connection failed")

            orchestrator = PipelineOrchestrator()

            # Run pipeline and expect it to raise
            with pytest.raises(Exception, match="Database connection failed"):
                await orchestrator.run()

    def test_transform_all_data_with_invalid_data(self, mock_settings):
        """Test transformation raises exception on invalid data"""
        with patch('src.orchestation.pipeline.get_settings', return_value=mock_settings), \
             patch('src.orchestation.pipeline.bigquery.get_bigquery_client'):

            orchestrator = PipelineOrchestrator()

            # Create invalid dataframe (missing required columns)
            invalid_df = pl.DataFrame({
                'invalid_col': [1, 2, 3]
            })

            dataframes = [invalid_df]
            pollutants = ['co']

            # Should raise exception when transforming invalid data
            with pytest.raises(Exception):
                orchestrator.transform_all_data(pollutants, dataframes)
