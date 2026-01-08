"""
Unit tests for data transformers
"""
import pytest
import polars as pl
from datetime import datetime, timezone

from src.utils.transformers import (
    build_metric_columns,
    aggregate_to_long_format
)
from src.utils.schema import OUTPUT_COLUMNS


class TestBuildMetricColumns:
    """Test metric column building functions"""

    def test_single_metric(self):
        metrics = ['co_mean']
        result = build_metric_columns(metrics)
        
        assert result == {'co_mean': 'co'}

    def test_multiple_metrics(self):
        metrics = ['no_mean', 'no2_mean', 'nox_mean']
        result = build_metric_columns(metrics)
        
        expected = {
            'no_mean': 'no',
            'no2_mean': 'no2',
            'nox_mean': 'nox'
        }
        assert result == expected

    def test_empty_list(self):
        result = build_metric_columns([])
        assert result == {}


class TestAggregateToLongFormat:
    """Test data aggregation to long format"""

    def test_no_metric_columns_raises_error(self):
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k']
        })

        with pytest.raises(ValueError, match="metric_columns cannot be None or empty"):
            aggregate_to_long_format(df)

    def test_empty_dataframe_returns_empty_with_schema(self):
        df = pl.DataFrame()
        metric_columns = {'co_mean': 'co'}
        
        result = aggregate_to_long_format(df, metric_columns=metric_columns)
        
        assert result.is_empty()
        assert list(result.columns) == OUTPUT_COLUMNS

    def test_basic_aggregation(self):
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [
                base_time,
                base_time.replace(minute=10),
                base_time.replace(minute=20),
                base_time.replace(minute=30)
            ],
            'location': ['loc1'] * 4,
            'status': ['k'] * 4,
            'co_mean': [1.0, 1.5, 2.0, 2.5]
        })

        result = aggregate_to_long_format(
            df,
            metric_columns={'co_mean': 'co'},
            version="test-v1.0"
        )

        # Check schema
        assert list(result.columns) == OUTPUT_COLUMNS
        
        # Check values
        assert result['metrica'][0] == 'co'
        assert result['version'][0] == 'test-v1.0'
        assert result['time'].dtype == pl.Datetime('us', 'UTC')

    def test_filters_invalid_status(self):
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [
                base_time,
                base_time.replace(minute=10),
                base_time.replace(minute=20)
            ],
            'location': ['loc1'] * 3,
            'status': ['k', 'v', 'k'],  # Only 2 valid
            'co_mean': [1.0, 1.5, 2.0]
        })

        result = aggregate_to_long_format(
            df,
            metric_columns={'co_mean': 'co'},
            version="test"
        )

        assert result.height == 1
        assert result['count_ok'][0] == 2

    def test_multiple_metrics(self):
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [base_time, base_time.replace(minute=10)],
            'location': ['loc1', 'loc1'],
            'status': ['k', 'k'],
            'co_mean': [1.0, 1.5],
            'no_mean': [10.0, 15.0]
        })

        result = aggregate_to_long_format(
            df,
            metric_columns={'co_mean': 'co', 'no_mean': 'no'},
            version="test"
        )

        assert result.height == 2
        metrics = result['metrica'].unique().sort().to_list()
        assert metrics == ['co', 'no']

    def test_all_invalid_status_returns_empty(self):
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [base_time],
            'location': ['loc1'],
            'status': ['v'],  # Invalid
            'co_mean': [1.0]
        })

        result = aggregate_to_long_format(
            df,
            metric_columns={'co_mean': 'co'},
            version="test"
        )

        assert result.is_empty()
        assert list(result.columns) == OUTPUT_COLUMNS
