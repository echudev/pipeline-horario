from config import get_settings
from google.cloud import bigquery

def create_bigquery_client() -> bigquery.Client:
    """
    Create and return a BigQuery client instance using settings from config.
    
    Returns:
        bigquery.Client: Configured BigQuery client
    """
    settings = get_settings()
    return bigquery.Client(project=settings.GOOGLE_PROJECT_ID)

# Global client instance (lazy initialization)
_client: bigquery.Client | None = None

def get_bigquery_client() -> bigquery.Client:
    """
    Get or create the global BigQuery client instance.
    
    Returns:
        bigquery.Client: The global client instance of BigQuery
    """
    global _client
    if _client is None:
        _client = create_bigquery_client()
    return _client
