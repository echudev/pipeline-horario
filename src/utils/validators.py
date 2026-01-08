"""
Data validation utilities for extracted data.
"""
import logging
from typing import List, Optional

import polars as pl

from .schema import REQUIRED_INPUT_COLUMNS

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Exception raised when data validation fails."""
    pass


def validate_dataframe(
    df: pl.DataFrame,
    required_columns: Optional[List[str]] = None,
    pollutant_type: Optional[str] = None
) -> bool:
    """
    Validate DataFrame structure and content.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names (defaults to REQUIRED_INPUT_COLUMNS)
        pollutant_type: Type of pollutant data for specific validation

    Returns:
        bool: True if validation passes

    Raises:
        DataValidationError: If validation fails
    """
    if df.is_empty():
        raise DataValidationError("DataFrame is empty")

    required = required_columns or REQUIRED_INPUT_COLUMNS

    # Check required columns
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise DataValidationError(f"Missing required columns: {missing}")

    # Validate specific columns
    if 'time' in df.columns:
        validate_time_column(df)

    if 'status' in df.columns:
        validate_status_column(df)

    if pollutant_type:
        validate_pollutant_data(df, pollutant_type)

    logger.info(f"DataFrame validation passed: {len(df)} rows")
    return True


def validate_time_column(df: pl.DataFrame) -> None:
    """
    Validate time column has valid timestamps.

    Args:
        df: DataFrame with time column

    Raises:
        DataValidationError: If time validation fails
    """
    time_col = df['time']

    null_count = time_col.null_count()
    if null_count > 0:
        raise DataValidationError(f"Time column contains {null_count} null values")

    if not time_col.dtype.is_temporal():
        raise DataValidationError(f"Time column has invalid type: {time_col.dtype}")


def validate_status_column(df: pl.DataFrame) -> None:
    """
    Validate status column contains valid status values.

    Args:
        df: DataFrame with status column

    Raises:
        DataValidationError: If status validation fails
    """
    status_col = df['status']

    null_count = status_col.null_count()
    if null_count > 0:
        raise DataValidationError(f"Status column contains {null_count} null values")

    valid_statuses = {'k', 'v', 's', 'm'}
    unique_statuses = set(status_col.unique().to_list())
    
    invalid = unique_statuses - valid_statuses
    if invalid:
        logger.warning(f"Found invalid status values: {invalid}")


def validate_pollutant_data(df: pl.DataFrame, pollutant_type: str) -> None:
    """
    Validate pollutant-specific data requirements.

    Args:
        df: DataFrame with pollutant data
        pollutant_type: Type of pollutant

    Raises:
        DataValidationError: If pollutant validation fails
    """
    # Define expected ranges per pollutant
    ranges = {
        'co': {'co_mean': (0, 100)},
        'nox': {'no_mean': (0, 500), 'no2_mean': (0, 500), 'nox_mean': (0, 1000)},
        'pm10': {'pm10_mean': (0, 1000)},
        'so2': {'so2_mean': (0, 500)},
        'o3': {'o3_mean': (0, 500)},
    }
    
    if pollutant_type not in ranges:
        return
    
    for col, (min_val, max_val) in ranges[pollutant_type].items():
        if col not in df.columns:
            continue
            
        col_data = df[col].drop_nulls()
        if col_data.is_empty():
            continue
            
        actual_min = col_data.min()
        actual_max = col_data.max()
        
        if actual_min < min_val or actual_max > max_val:
            logger.warning(
                f"{pollutant_type}.{col} values outside expected range "
                f"[{min_val}, {max_val}]: actual [{actual_min}, {actual_max}]"
            )


def validate_data_quality_score(df: pl.DataFrame) -> float:
    """
    Calculate data quality score based on completeness and validity.

    Args:
        df: DataFrame to score

    Returns:
        float: Quality score between 0 and 1
    """
    if df.is_empty():
        return 0.0

    total_rows = len(df)

    # Completeness score
    completeness_scores = []
    for col in df.columns:
        if col != 'time':
            null_count = df[col].null_count()
            completeness_scores.append((total_rows - null_count) / total_rows)

    avg_completeness = (
        sum(completeness_scores) / len(completeness_scores) 
        if completeness_scores else 1.0
    )

    # Validity score (based on status column)
    if 'status' in df.columns:
        valid_rows = df.filter(pl.col('status') == 'k').height
        validity_score = valid_rows / total_rows
    else:
        validity_score = 1.0

    quality_score = (avg_completeness + validity_score) / 2

    logger.info(f"Data quality score: {quality_score:.2f}")
    return quality_score
