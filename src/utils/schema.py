"""
Centralized schema definitions for the pipeline
"""
import polars as pl
from typing import Dict, List, Any


# Standard output schema for transformed data
OUTPUT_SCHEMA: Dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "location": pl.Utf8,
    "metrica": pl.Utf8,
    "valor": pl.Float64,
    "count_ok": pl.UInt32,
    "version": pl.Utf8,
}

# Column names in order
OUTPUT_COLUMNS: List[str] = list(OUTPUT_SCHEMA.keys())

# Required columns for validation
REQUIRED_INPUT_COLUMNS: List[str] = ["time", "location", "status"]


def create_empty_output_dataframe() -> pl.DataFrame:
    """
    Create an empty DataFrame with the standard output schema.
    
    Returns:
        Empty polars DataFrame with correct schema
    """
    return pl.DataFrame({
        col: pl.Series(dtype=dtype) 
        for col, dtype in OUTPUT_SCHEMA.items()
    })


def validate_output_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Validate and normalize DataFrame to match output schema.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        DataFrame with correct schema and column order
    
    Raises:
        ValueError: If required columns are missing
    """
    if df.is_empty():
        return create_empty_output_dataframe()
    
    # Add missing columns with null values
    for col, dtype in OUTPUT_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
    
    # Ensure correct types
    cast_exprs = []
    for col, dtype in OUTPUT_SCHEMA.items():
        if col in df.columns and df[col].dtype != dtype:
            cast_exprs.append(pl.col(col).cast(dtype).alias(col))
    
    if cast_exprs:
        df = df.with_columns(cast_exprs)
    
    # Select columns in correct order
    return df.select(OUTPUT_COLUMNS)


def get_bigquery_schema() -> List[Dict[str, Any]]:
    """
    Get BigQuery schema definition for output data.
    
    Returns:
        List of BigQuery SchemaField-compatible dicts
    """
    from google.cloud import bigquery
    
    return [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("metrica", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valor", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("count_ok", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    ]
