"""
Utility modules for the pipeline
"""
from .clients import (
    # Context managers (preferred)
    influxdb_client,
    bigquery_client,
    motherduck_client,
    # Data fetching
    fetch_data,
    fetch_incremental_data,
    fetch_specific_hour,
    backfill_hours,
    find_missing_hours,
    ClientError,
    # Legacy (deprecated)
    get_influxdb_client,
    close_influxdb_client,
    get_bigquery_client,
    get_motherduck_client,
    InfluxDBClientManager,
)
from .transformers import (
    build_metric_columns,
    aggregate_to_long_format,
)
from .validators import (
    validate_dataframe,
    DataValidationError,
    validate_time_column,
    validate_status_column,
    validate_pollutant_data,
    validate_data_quality_score,
)
from .exporters import (
    export_to_excel,
    export_to_bigquery,
    export_to_motherduck,
    ExportError,
)
from .state import (
    PipelineState,
    PipelineStateError,
    get_pipeline_state,
)
from .datetime_utils import (
    ensure_utc,
    add_hours,
    truncate_to_hour,
    get_current_hour_start,
    get_previous_hour_start,
    iter_hours,
)
from .schema import (
    OUTPUT_SCHEMA,
    OUTPUT_COLUMNS,
    REQUIRED_INPUT_COLUMNS,
    create_empty_output_dataframe,
    validate_output_schema,
)

__all__ = [
    # Clients - context managers
    "influxdb_client",
    "bigquery_client", 
    "motherduck_client",
    # Clients - data fetching
    "fetch_data",
    "fetch_incremental_data",
    "fetch_specific_hour",
    "backfill_hours",
    "find_missing_hours",
    "ClientError",
    # Clients - legacy
    "get_influxdb_client",
    "close_influxdb_client",
    "get_bigquery_client",
    "get_motherduck_client",
    "InfluxDBClientManager",
    # Transformers
    "build_metric_columns",
    "aggregate_to_long_format",
    # Validators
    "validate_dataframe",
    "DataValidationError",
    "validate_time_column",
    "validate_status_column",
    "validate_pollutant_data",
    "validate_data_quality_score",
    # Exporters
    "export_to_excel",
    "export_to_bigquery",
    "export_to_motherduck",
    "ExportError",
    # State
    "PipelineState",
    "PipelineStateError",
    "get_pipeline_state",
    # Datetime utilities
    "ensure_utc",
    "add_hours",
    "truncate_to_hour",
    "get_current_hour_start",
    "get_previous_hour_start",
    "iter_hours",
    # Schema
    "OUTPUT_SCHEMA",
    "OUTPUT_COLUMNS",
    "REQUIRED_INPUT_COLUMNS",
    "create_empty_output_dataframe",
    "validate_output_schema",
]
