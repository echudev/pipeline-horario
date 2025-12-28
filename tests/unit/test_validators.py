"""
Unit tests for data validators
"""
import pytest
import polars as pl
from datetime import datetime, timezone

from src.extract.validators import (
    validate_dataframe,
    validate_time_column,
    validate_status_column,
    validate_data_quality_score,
    DataValidationError
)


class TestValidateDataFrame:
    """Test DataFrame validation functions"""

    def test_validate_dataframe_empty_raises_error(self):
        """Test that empty DataFrame raises validation error"""
        df = pl.DataFrame()

        with pytest.raises(DataValidationError, match="DataFrame is empty"):
            validate_dataframe(df)

    def test_validate_dataframe_missing_columns_raises_error(self):
        """Test that DataFrame with missing required columns raises error"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test']
            # Missing 'status' column
        })

        with pytest.raises(DataValidationError, match="Missing required columns"):
            validate_dataframe(df)

    def test_validate_dataframe_valid_passes(self):
        """Test that valid DataFrame passes validation"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k']
        })

        result = validate_dataframe(df)
        assert result is True

    def test_validate_dataframe_with_pollutant_type(self):
        """Test DataFrame validation with pollutant type"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k'],
            'co_mean': [1.5]
        })

        result = validate_dataframe(df, pollutant_type='co')
        assert result is True


class TestValidateTimeColumn:
    """Test time column validation"""

    def test_validate_time_column_with_nulls_raises_error(self):
        """Test that time column with nulls raises error"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc), None]
        })

        with pytest.raises(DataValidationError, match="null values"):
            validate_time_column(df)

    def test_validate_time_column_valid_passes(self):
        """Test that valid time column passes validation"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc), datetime.now(timezone.utc)]
        })

        # Should not raise any exception
        validate_time_column(df)


class TestValidateStatusColumn:
    """Test status column validation"""

    def test_validate_status_column_with_nulls_raises_error(self):
        """Test that status column with nulls raises error"""
        df = pl.DataFrame({
            'status': ['k', None]
        })

        with pytest.raises(DataValidationError, match="null values"):
            validate_status_column(df)

    def test_validate_status_column_valid_passes(self):
        """Test that valid status column passes validation"""
        df = pl.DataFrame({
            'status': ['k', 'v', 's', 'm']
        })

        # Should not raise any exception
        validate_status_column(df)


class TestValidateDataQualityScore:
    """Test data quality scoring"""

    def test_validate_data_quality_score_empty_dataframe(self):
        """Test quality score for empty DataFrame"""
        df = pl.DataFrame()

        score = validate_data_quality_score(df)
        assert score == 0.0

    def test_validate_data_quality_score_perfect_data(self):
        """Test quality score for perfect data"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            'location': ['loc1', 'loc2'],
            'status': ['k', 'k'],
            'value': [1.0, 2.0]
        })

        score = validate_data_quality_score(df)
        assert score == 1.0

    def test_validate_data_quality_score_with_nulls(self):
        """Test quality score with null values"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            'location': ['loc1', None],
            'status': ['k', 'k'],
            'value': [1.0, None]
        })

        score = validate_data_quality_score(df)
        assert 0.0 < score < 1.0
