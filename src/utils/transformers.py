"""
Transformers for pollutant data processing
"""
import polars as pl
from typing import Dict, List, Optional

from .schema import OUTPUT_SCHEMA, OUTPUT_COLUMNS, create_empty_output_dataframe


def build_metric_columns(metrics: List[str]) -> Dict[str, str]:
    """
    Convert a list of metrics to a column mapping dictionary.
    
    Args:
        metrics: List of metric names (e.g., ['co_mean', 'no_mean'])
    
    Returns:
        Dict with {original_column: metric_name} (e.g., {'co_mean': 'co'})
    
    Examples:
        >>> build_metric_columns(['co_mean'])
        {'co_mean': 'co'}
        >>> build_metric_columns(['no_mean', 'no2_mean', 'nox_mean'])
        {'no_mean': 'no', 'no2_mean': 'no2', 'nox_mean': 'nox'}
    """
    metric_columns = {}
    for metric in metrics:
        metric_name = metric.replace('_mean', '')
        metric_columns[metric] = metric_name
    return metric_columns


def aggregate_to_long_format(
    df: pl.DataFrame,
    time_col: str = "time",
    location_col: str = "location",
    metric_columns: Optional[Dict[str, str]] = None,
    version: Optional[str] = None
) -> pl.DataFrame:
    """
    Convert data from wide to long format and aggregate by hour.
    
    Args:
        df: Polars DataFrame in wide format
        time_col: Name of the time column
        location_col: Name of the location column
        metric_columns: Dict with {original_column: metric_name}
        version: Pipeline version string
    
    Returns:
        pl.DataFrame: Transformed DataFrame in long format with standard schema
    
    Raises:
        ValueError: If metric_columns is None or empty
    """
    if not metric_columns:
        raise ValueError("metric_columns cannot be None or empty")

    # Handle empty DataFrames
    if df.is_empty() or len(df) == 0:
        return create_empty_output_dataframe()

    # Filter only valid status records
    filtered_df = df.filter(pl.col("status") == "k")
    
    if filtered_df.is_empty():
        return create_empty_output_dataframe()

    frames = []
    for col_name, metric_name in metric_columns.items():
        df_metric = (
            filtered_df.group_by_dynamic(
                index_column=time_col,
                every="1h",
                group_by=location_col
            )
            .agg([
                pl.lit(metric_name).alias("metrica"),
                pl.col(col_name).mean().alias("valor"),
                pl.col("status").eq("k").sum().cast(pl.UInt32).alias("count_ok"),
                pl.lit(version).alias("version")
            ])
        )
        frames.append(df_metric)

    result = pl.concat(frames)

    # Ensure consistent schema
    result = result.with_columns(
        pl.col(time_col).cast(pl.Datetime('us', 'UTC')).alias(time_col)
    )
    
    # Ensure column order matches schema
    return result.select(OUTPUT_COLUMNS)
