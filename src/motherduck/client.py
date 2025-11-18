import duckdb
from duckdb import DuckDBPyConnection
from config import get_settings

def create_motherduck_client() -> DuckDBPyConnection:
    """
    Create and return a MotherDuck client instance using settings from config.
    
    Returns:
        DuckDBPyConnection: Configured MotherDuck client
    """
    settings = get_settings()
    return duckdb.connect(f'md:{settings.MOTHERDUCK_DATABASE}?motherduck_token={settings.MOTHERDUCK_TOKEN}')

# Global client instance (lazy initialization)
_client: DuckDBPyConnection | None = None

def get_motherduck_client() -> DuckDBPyConnection:
    """
    Get or create the global MotherDuck client instance.
    
    Returns:
        DuckDBPyConnection: The global client instance of MotherDuck
    """
    global _client
    if _client is None:
        _client = create_motherduck_client()
        print(f"Connected to MotherDuck database")
    return _client