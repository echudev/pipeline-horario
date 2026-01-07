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


class TestBuildMetricColumns:
    """Test metric column building functions"""

    def test_build_metric_columns_single_metric(self):
        """Test building metric columns for single metric"""
        metrics = ['co_mean']
        result = build_metric_columns(metrics)

        expected = {'co_mean': 'co'}
        assert result == expected

    def test_build_metric_columns_multiple_metrics(self):
        """Test building metric columns for multiple metrics"""
        metrics = ['no_mean', 'no2_mean', 'nox_mean']
        result = build_metric_columns(metrics)

        expected = {
            'no_mean': 'no',
            'no2_mean': 'no2',
            'nox_mean': 'nox'
        }
        assert result == expected

    def test_build_metric_columns_empty_list(self):
        """Test building metric columns for empty list"""
        metrics = []
        result = build_metric_columns(metrics)

        assert result == {}


class TestAggregateToLongFormat:
    """Test data aggregation to long format"""

    def test_aggregate_to_long_format_no_metric_columns_raises_error(self):
        """Test that empty metric_columns raises ValueError"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k']
        })

        with pytest.raises(ValueError, match="metric_columns cannot be None or empty"):
            aggregate_to_long_format(df)

    def test_aggregate_to_long_format_basic_aggregation(self):
        """Test basic aggregation to long format"""
        # Create test data
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [
                base_time,
                base_time.replace(minute=10),
                base_time.replace(minute=20),
                base_time.replace(minute=30)
            ],
            'location': ['loc1', 'loc1', 'loc1', 'loc1'],
            'status': ['k', 'k', 'k', 'k'],
            'co_mean': [1.0, 1.5, 2.0, 2.5]
        })

        metric_columns = {'co_mean': 'co'}

        result = aggregate_to_long_format(
            df,
            metric_columns=metric_columns,
            version="test-v1.0"
        )

        # Check result structure
        assert 'time' in result.columns
        assert 'location' in result.columns
        assert 'metrica' in result.columns
        assert 'valor' in result.columns
        assert 'count_ok' in result.columns
        assert 'version_pipeline' in result.columns

        # Check that time is truncated to hour
        assert result['time'].dtype == pl.Datetime

        # Check metric name
        assert (result['metrica'] == 'co').all()

        # Check version
        assert (result['version_pipeline'] == 'test-v1.0').all()

    def test_aggregate_to_long_format_filters_invalid_status(self):
        """Test that only 'k' status records are included"""
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [
                base_time,
                base_time.replace(minute=10),
                base_time.replace(minute=20)
            ],
            'location': ['loc1', 'loc1', 'loc1'],
            'status': ['k', 'v', 'k'],  # Mixed status
            'co_mean': [1.0, 1.5, 2.0]
        })

        metric_columns = {'co_mean': 'co'}

        result = aggregate_to_long_format(
            df,
            metric_columns=metric_columns,
            version="test-v1.0"
        )

        # Should only include records with status 'k'
        # Since we group by hour, we get one record with count_ok = 2
        assert result.height == 1
        assert result['count_ok'][0] == 2

    def test_aggregate_to_long_format_multiple_metrics(self):
        """Test aggregation with multiple metrics"""
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        df = pl.DataFrame({
            'time': [base_time, base_time.replace(minute=10)],
            'location': ['loc1', 'loc1'],
            'status': ['k', 'k'],
            'co_mean': [1.0, 1.5],
            'no_mean': [10.0, 15.0]
        })

        metric_columns = {'co_mean': 'co', 'no_mean': 'no'}

        result = aggregate_to_long_format(
            df,
            metric_columns=metric_columns,
            version="test-v1.0"
        )

        # Should have 2 rows (one per metric)
        assert result.height == 2

        # Check both metrics are present
        metrics = result['metrica'].unique().sort()
        assert metrics.to_list() == ['co', 'no']
