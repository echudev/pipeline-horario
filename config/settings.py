"""
Application settings with Prefect Blocks support for secrets
"""
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load .env file from project root as fallback
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


def _load_secret_from_block(block_name: str, secret_key: str) -> Optional[str]:
    """
    Load a secret from a Prefect Secret block
    
    Args:
        block_name: Name of the Prefect Secret block
        secret_key: Key within the secret block (optional, defaults to 'value')
    
    Returns:
        Secret value or None if block doesn't exist
    """
    try:
        from prefect.blocks.system import Secret
        secret_block = Secret.load(block_name)
        # Secret blocks can store a single value or a dict
        if secret_key == 'value':
            return secret_block.get()
        else:
            secret_dict = secret_block.get()
            if isinstance(secret_dict, dict):
                return secret_dict.get(secret_key)
            return None
    except Exception:
        # Block doesn't exist or Prefect not available
        return None


def _get_secret(block_name: str, env_var: str, secret_key: str = 'value') -> Optional[str]:
    """
    Get secret from Prefect Block first, fallback to environment variable
    
    Args:
        block_name: Name of the Prefect Secret block
        env_var: Environment variable name as fallback
        secret_key: Key within the secret block (defaults to 'value')
    
    Returns:
        Secret value or None
    """
    # Try Prefect Block first
    value = _load_secret_from_block(block_name, secret_key)
    if value:
        return value
    
    # Fallback to environment variable
    return os.getenv(env_var)


class Settings:
    """Application settings loaded from environment variables and Prefect Blocks"""
    
    # InfluxDB Configuration
    INFLUXDB_HOST: str = os.getenv("INFLUXDB_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com/")
    INFLUXDB_DATABASE: str = os.getenv("INFLUXDB_DATABASE", "minutales")
    # Secret: loaded from Prefect Block or env var
    INFLUXDB_TOKEN: Optional[str] = _get_secret("influxdb-token", "INFLUXDB_TOKEN")
    
    # BigQuery Configuration
    GOOGLE_PROJECT_ID: Optional[str] = _get_secret("google-project-id", "GOOGLE_PROJECT_ID", "project_id") or os.getenv("GOOGLE_PROJECT_ID", "pruebas-477910")
    BIGQUERY_DATASET_ID: str = os.getenv("BIGQUERY_DATASET_ID", "test_dataset")
    BIGQUERY_TABLE_ID: str = os.getenv("BIGQUERY_TABLE_ID", "promedios_horarios")
    
    # Pipeline Configuration
    PIPELINE_VERSION: str = os.getenv("PIPELINE_VERSION", "v0.8.6")
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "output")
    OUTPUT_FILENAME: str = os.getenv("OUTPUT_FILENAME", "")
    
    # MotherDuck Configuration
    MOTHERDUCK_DATABASE: Optional[str] = os.getenv("MOTHERDUCK_DATABASE")
    # Secret: loaded from Prefect Block or env var
    MOTHERDUCK_TOKEN: Optional[str] = _get_secret("motherduck-token", "MOTHERDUCK_TOKEN")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    def __init__(self):
        """Validate required settings"""
        # Validate INFLUXDB_TOKEN (required)
        if not self.INFLUXDB_TOKEN:
            raise ValueError(
                "INFLUXDB_TOKEN is required. "
                "Please set it in a Prefect Secret block named 'influxdb-token' "
                "or set the INFLUXDB_TOKEN environment variable."
            )
        
        # Generate output filename if not provided
        if not self.OUTPUT_FILENAME:
            import time
            self.OUTPUT_FILENAME = f"{time.strftime('%Y%m%d%H')}_contaminantes_horarios.xlsx"
    
    @property
    def output_path(self) -> str:
        """Get full output file path"""
        return os.path.join(self.OUTPUT_DIR, self.OUTPUT_FILENAME)


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
