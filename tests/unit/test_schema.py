"""
Unit tests for schema utilities
"""
import pytest
import polars as pl
from datetime import datetime, timezone

from src.utils.schema import (
    OUTPUT_SCHEMA,
    OUTPUT_COLUMNS,
    create_empty_output_dataframe,
    validate_output_schema,
)


class TestOutputSchema:
    """Test output schema constants"""

    def test_output_columns_order(self):
        expected = ["time", "location", "metrica", "valor", "count_ok", "version"]
        assert OUTPUT_COLUMNS == expected

    def test_output_schema_types(self):
        assert OUTPUT_SCHEMA["time"] == pl.Datetime("us", "UTC")
        assert OUTPUT_SCHEMA["location"] == pl.Utf8
        assert OUTPUT_SCHEMA["metrica"] == pl.Utf8
        assert OUTPUT_SCHEMA["valor"] == pl.Float64
        assert OUTPUT_SCHEMA["count_ok"] == pl.UInt32
        assert OUTPUT_SCHEMA["version"] == pl.Utf8


class TestCreateEmptyOutputDataframe:
    """Test empty dataframe creation"""

    def test_creates_empty_with_correct_schema(self):
        df = create_empty_output_dataframe()
        
        assert df.is_empty()
        assert list(df.columns) == OUTPUT_COLUMNS
        
        for col, dtype in OUTPUT_SCHEMA.items():
            assert df[col].dtype == dtype


class TestValidateOutputSchema:
    """Test schema validation"""

    def test_empty_dataframe_returns_empty_with_schema(self):
        df = pl.DataFrame()
        result = validate_output_schema(df)
        
        assert result.is_empty()
        assert list(result.columns) == OUTPUT_COLUMNS

    def test_adds_missing_columns(self):
        df = pl.DataFrame({
            "time": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
            "location": ["test"],
            "metrica": ["co"],
        })
        
        result = validate_output_schema(df)
        
        assert list(result.columns) == OUTPUT_COLUMNS
        assert result["valor"].is_null().all()
        assert result["count_ok"].is_null().all()
        assert result["version"].is_null().all()

    def test_reorders_columns(self):
        df = pl.DataFrame({
            "version": ["v1"],
            "metrica": ["co"],
            "location": ["test"],
            "time": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
            "valor": [1.0],
            "count_ok": [5],
        })
        
        result = validate_output_schema(df)
        
        assert list(result.columns) == OUTPUT_COLUMNS

    def test_casts_types(self):
        df = pl.DataFrame({
            "time": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
            "location": ["test"],
            "metrica": ["co"],
            "valor": [1],  # Int instead of Float
            "count_ok": [5],
            "version": ["v1"],
        })
        
        result = validate_output_schema(df)
        
        assert result["valor"].dtype == pl.Float64
