import os
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
import time

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Settings:
    """Application settings loaded from environment variables"""
    
    # InfluxDB Configuration
    INFLUXDB_HOST: str | None = os.getenv("INFLUXDB_HOST")
    INFLUXDB_TOKEN: str | None = os.getenv("INFLUXDB_TOKEN")
    INFLUXDB_DATABASE: str | None = os.getenv("INFLUXDB_DATABASE")
    
    # BigQuery Configuration
    GOOGLE_PROJECT_ID: str | None = os.getenv("GOOGLE_PROJECT_ID")
    BIGQUERY_DATASET_ID: str | None = os.getenv("BIGQUERY_DATASET_ID")
    BIGQUERY_TABLE_ID: str | None = os.getenv("BIGQUERY_TABLE_ID")

    # Pipeline Configuration
    PIPELINE_VERSION: str = os.getenv("PIPELINE_VERSION", "v1.0.0")
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "output")
    OUTPUT_FILENAME: str = os.getenv("OUTPUT_FILENAME", f"{time.strftime('%Y%m%d%H')}_contaminantes_horarios.xlsx")

    # MotherDuck Configuration
    MOTHERDUCK_TOKEN: str | None = os.getenv("MOTHERDUCK_TOKEN")
    MOTHERDUCK_DATABASE: str | None = os.getenv("MOTHERDUCK_DATABASE")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    def __init__(self):
        """Validate required settings"""
        if not self.INFLUXDB_TOKEN:
            raise ValueError(
                "INFLUXDB_TOKEN environment variable is required. "
                "Please set it in your .env file or environment."
            )
    
    @property
    def output_path(self) -> str:
        """Get full output file path"""
        return os.path.join(self.OUTPUT_DIR, self.OUTPUT_FILENAME)


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()

