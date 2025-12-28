"""
Data validation utilities for extracted data
"""
import logging
from typing import List, Optional
import polars as pl

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Exception raised when data validation fails"""
    pass


def validate_dataframe(
    df: pl.DataFrame,
    required_columns: Optional[List[str]] = None,
    pollutant_type: Optional[str] = None
) -> bool:
    """
    Validate DataFrame structure and content

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        pollutant_type: Type of pollutant data for specific validation

    Returns:
        bool: True if validation passes

    Raises:
        DataValidationError: If validation fails
    """
    if df.is_empty():
        raise DataValidationError("DataFrame is empty")

    # Default required columns for pollutant data
    if required_columns is None:
        required_columns = ['time', 'location', 'status']

    # Check required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise DataValidationError(f"Missing required columns: {missing_columns}")

    # Validate time column
    if 'time' in df.columns:
        validate_time_column(df)

    # Validate status column
    if 'status' in df.columns:
        validate_status_column(df)

    # Validate pollutant-specific columns
    if pollutant_type:
        validate_pollutant_data(df, pollutant_type)

    logger.info(f"DataFrame validation passed for {len(df)} rows")
    return True


def validate_time_column(df: pl.DataFrame) -> None:
    """
    Validate time column has valid timestamps

    Args:
        df: DataFrame with time column

    Raises:
        DataValidationError: If time validation fails
    """
    time_col = df['time']

    # Check for null values
    null_count = time_col.null_count()
    if null_count > 0:
        raise DataValidationError(f"Time column contains {null_count} null values")

    # Check data type
    if not time_col.dtype.is_temporal():
        raise DataValidationError(f"Time column has invalid type: {time_col.dtype}")


def validate_status_column(df: pl.DataFrame) -> None:
    """
    Validate status column contains valid status values

    Args:
        df: DataFrame with status column

    Raises:
        DataValidationError: If status validation fails
    """
    status_col = df['status']

    # Check for null values
    null_count = status_col.null_count()
    if null_count > 0:
        raise DataValidationError(f"Status column contains {null_count} null values")

    # Check valid status values (assuming 'k' is valid, others are invalid)
    valid_statuses = ['k', 'v', 's', 'm']  # Example valid statuses
    unique_statuses = status_col.unique().to_list()

    invalid_statuses = [s for s in unique_statuses if s not in valid_statuses]
    if invalid_statuses:
        logger.warning(f"Found invalid status values: {invalid_statuses}")


def validate_pollutant_data(df: pl.DataFrame, pollutant_type: str) -> None:
    """
    Validate pollutant-specific data requirements

    Args:
        df: DataFrame with pollutant data
        pollutant_type: Type of pollutant

    Raises:
        DataValidationError: If pollutant validation fails
    """
    # Basic validation - pollutant columns should have reasonable value ranges
    # This is a placeholder for more specific validations per pollutant type

    # For now, just check that numeric columns don't have extreme outliers
    numeric_columns = [col for col in df.columns if df[col].dtype.is_numeric() and col not in ['time']]

    for col in numeric_columns:
        col_data = df[col]
        if col_data.dtype.is_float():
            # Check for reasonable value ranges (example thresholds)
            min_val = col_data.min()
            max_val = col_data.max()

            # Example: CO should be between 0 and 50 ppm
            if pollutant_type == 'co' and col == 'co_mean':
                if min_val < 0 or max_val > 100:
                    logger.warning(f"CO values outside expected range: {min_val} - {max_val}")


def validate_data_quality_score(df: pl.DataFrame) -> float:
    """
    Calculate data quality score based on completeness and validity

    Args:
        df: DataFrame to score

    Returns:
        float: Quality score between 0 and 1
    """
    total_rows = len(df)
    if total_rows == 0:
        return 0.0

    # Completeness score (percentage of non-null values)
    completeness_scores = []
    for col in df.columns:
        if col != 'time':  # Time should always be complete
            null_count = df[col].null_count()
            completeness_scores.append((total_rows - null_count) / total_rows)

    avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 1.0

    # Validity score (based on status column)
    if 'status' in df.columns:
        valid_rows = df.filter(pl.col('status') == 'k').height
        validity_score = valid_rows / total_rows
    else:
        validity_score = 1.0

    # Combined score
    quality_score = (avg_completeness + validity_score) / 2

    logger.info(f"Data quality score: {quality_score:.2f}")
    return quality_score
