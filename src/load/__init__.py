"""
Data loading module
"""

from .bigquery import (
    create_bigquery_client,
    get_bigquery_client,
    export_to_bigquery
)
from .excel import export_to_excel
from .motherduck import (
    create_motherduck_client,
    get_motherduck_client,
    export_to_motherduck
)

__all__ = [
    "create_bigquery_client",
    "get_bigquery_client",
    "export_to_bigquery",
    "export_to_excel",
    "create_motherduck_client",
    "get_motherduck_client",
    "export_to_motherduck",
]
