"""
Exporters for pipeline output
"""
import logging
import polars as pl
from pathlib import Path

logger = logging.getLogger(__name__)


def export_to_excel(df: pl.DataFrame, output_path: str) -> None:
    """
    Export DataFrame to Excel file
    
    Args:
        df: DataFrame to export
        output_path: Full path to output file (including directory and filename)
    
    Raises:
        OSError: If the output directory cannot be created
        Exception: If the export fails
    """
    # Create output directory if it doesn't exist
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Exporting {len(df)} rows to {output_path}")
    
    try:
        df.write_excel(output_path)
        logger.info(f"Successfully exported data to {output_path}")
    except Exception as e:
        logger.error(f"Failed to export data to {output_path}: {e}")
        raise

