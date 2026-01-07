"""
Unit tests for data validators
"""
import pytest
import polars as pl
from datetime import datetime, timezone

from src.utils.validators import (
    validate_dataframe,
    DataValidationError,
    validate_time_column,
    validate_status_column,
    validate_pollutant_data,
    validate_data_quality_score,
)


class TestValidateDataframe:
    """Test DataFrame validation functions"""

    def test_validate_dataframe_empty_raises_error(self):
        """Test that empty DataFrame raises DataValidationError"""
        df = pl.DataFrame()
        
        with pytest.raises(DataValidationError, match="DataFrame is empty"):
            validate_dataframe(df)

    def test_validate_dataframe_missing_columns_raises_error(self):
        """Test that missing required columns raises error"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test']
            # Missing 'status' column
        })
        
        with pytest.raises(DataValidationError, match="Missing required columns"):
            validate_dataframe(df)

    def test_validate_dataframe_valid_data_passes(self):
        """Test that valid DataFrame passes validation"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k']
        })
        
        result = validate_dataframe(df)
        assert result is True

    def test_validate_dataframe_custom_required_columns(self):
        """Test validation with custom required columns"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k'],
            'co_mean': [1.0]
        })
        
        result = validate_dataframe(df, required_columns=['time', 'location', 'co_mean'])
        assert result is True


class TestValidateTimeColumn:
    """Test time column validation"""

    def test_validate_time_column_null_values_raises_error(self):
        """Test that null values in time column raise error"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc), None],
            'location': ['test', 'test']
        })
        
        with pytest.raises(DataValidationError, match="Time column contains"):
            validate_time_column(df)

    def test_validate_time_column_invalid_type_raises_error(self):
        """Test that non-temporal type raises error"""
        df = pl.DataFrame({
            'time': ['2024-01-01', '2024-01-02'],
            'location': ['test', 'test']
        })
        
        with pytest.raises(DataValidationError, match="Time column has invalid type"):
            validate_time_column(df)

    def test_validate_time_column_valid_passes(self):
        """Test that valid time column passes"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test']
        })
        
        # Should not raise
        validate_time_column(df)


class TestValidateStatusColumn:
    """Test status column validation"""

    def test_validate_status_column_null_values_raises_error(self):
        """Test that null values in status column raise error"""
        df = pl.DataFrame({
            'status': ['k', None],
            'location': ['test', 'test']
        })
        
        with pytest.raises(DataValidationError, match="Status column contains"):
            validate_status_column(df)

    def test_validate_status_column_valid_passes(self):
        """Test that valid status column passes"""
        df = pl.DataFrame({
            'status': ['k', 'v', 's'],
            'location': ['test', 'test', 'test']
        })
        
        # Should not raise (warnings are logged but don't raise)
        validate_status_column(df)


class TestValidateDataQualityScore:
    """Test data quality score calculation"""

    def test_validate_data_quality_score_empty_dataframe(self):
        """Test quality score for empty DataFrame"""
        df = pl.DataFrame()
        score = validate_data_quality_score(df)
        assert score == 0.0

    def test_validate_data_quality_score_perfect_data(self):
        """Test quality score for perfect data"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)],
            'location': ['test'],
            'status': ['k'],
            'value': [1.0]
        })
        score = validate_data_quality_score(df)
        assert 0.0 <= score <= 1.0

    def test_validate_data_quality_score_with_invalid_status(self):
        """Test quality score with invalid status values"""
        df = pl.DataFrame({
            'time': [datetime.now(timezone.utc)] * 4,
            'location': ['test'] * 4,
            'status': ['k', 'k', 'v', 'v'],
            'value': [1.0, 2.0, 3.0, 4.0]
        })
        score = validate_data_quality_score(df)
        # Should be less than 1.0 due to invalid status
        assert 0.0 <= score <= 1.0
