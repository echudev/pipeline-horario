"""
Transformers for pollutant data processing
"""
import polars as pl
from typing import Dict, List, Optional


def build_metric_columns(metrics: List[str]) -> Dict[str, str]:
    """
    Convierte una lista de métricas a un diccionario de columnas.
    
    Args:
        metrics: Lista de nombres de métricas (e.g., ['co_mean', 'no_mean'])
    
    Returns:
        Dict con {columna_original: nombre_metrica} (e.g., {'co_mean': 'co'})
    
    Examples:
        >>> build_metric_columns(['co_mean'])
        {'co_mean': 'co'}
        >>> build_metric_columns(['no_mean', 'no2_mean', 'nox_mean'])
        {'no_mean': 'no', 'no2_mean': 'no2', 'nox_mean': 'nox'}
    """
    metric_columns = {}
    for metric in metrics:
        # Remueve el sufijo '_mean' para obtener el nombre de la métrica
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
    Convierte datos de formato wide a long y agrega por hora
    
    Args:
        df: DataFrame de polars
        time_col: nombre de la columna de tiempo
        location_col: nombre de la columna de ubicación
        metric_columns: dict con {columna_original: nombre_metrica} ej: {"co_mean": "co"}
        version: versión del pipeline
    
    Returns:
        pl.DataFrame: DataFrame transformado en formato long
    
    Raises:
        ValueError: Si metric_columns es None o está vacío
    """
    if not metric_columns:
        raise ValueError("metric_columns cannot be None or empty")

    # Handle empty DataFrames (no new data in incremental mode)
    if df.is_empty() or len(df) == 0:
        # Return empty DataFrame with expected schema (consistent timezone)
        # Include all columns that would be present with data
        return pl.DataFrame({
            time_col: pl.Series(dtype=pl.Datetime('us', 'UTC')),
            location_col: pl.Series(dtype=pl.Utf8),
            "metrica": pl.Series(dtype=pl.Utf8),
            "valor": pl.Series(dtype=pl.Float64),
            "count_ok": pl.Series(dtype=pl.UInt32),
            "version_pipeline": pl.Series(dtype=pl.Utf8)
        }).rename({"version_pipeline": "version"})

    filtered_df = df.filter(pl.col("status") == "k")

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
                pl.col("status").eq("k").sum().alias("count_ok"),
                pl.lit(version).alias("version")
            ])
        )
        frames.append(df_metric)

    result = pl.concat(frames)

    # Ensure consistent timestamp format across all DataFrames
    if time_col in result.columns:
        result = result.with_columns(
            pl.col(time_col).cast(pl.Datetime('us', 'UTC')).alias(time_col)
        )

    return result

