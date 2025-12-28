"""
Pipeline orchestrator - coordinates data fetching, transformation, and export
"""
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import List

import polars as pl

from config import get_settings
from src.extract import db_extractor as influxdb
from src.load import bigquery
from src.transform import transform_metrics as transformers
import atexit

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the entire pipeline execution"""

    def __init__(self):
        self.settings = get_settings()
        self.influxdb_client = influxdb.get_influxdb_client()
        self.bigquery_client = bigquery.get_bigquery_client()
        # Register cleanup on exit
        atexit.register(self.cleanup)

    def cleanup(self) -> None:
        """Clean up resources"""
        try:
            influxdb.close_influxdb_client()
        except Exception:
            # Ignore cleanup errors
            pass
    
    async def fetch_all_data(self) -> List[pl.DataFrame]:
        """
        Fetch data for all pollutants in parallel
        
        Returns:
            List of DataFrames, one per pollutant
        """
        logger.info(f"Fetching data for {len(influxdb.POLLUTANTS_TO_PROCESS)} pollutants")
        
        try:
            dataframes = await asyncio.gather(*[
                influxdb.fetch_data(self.influxdb_client, pollutant) 
                for pollutant in influxdb.POLLUTANTS_TO_PROCESS
            ])
            logger.info(f"Successfully fetched data for all pollutants")
            return dataframes
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise
    
    def transform_all_data(
        self, 
        pollutants: List[str], 
        dataframes: List[pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Transform all dataframes from wide to long format
        
        Args:
            pollutants: List of pollutant names
            dataframes: List of DataFrames to transform
        
        Returns:
            Concatenated DataFrame with all transformed data
        """
        logger.info(f"Transforming {len(pollutants)} dataframes")
        
        transformed_dataframes = []
        for pollutant, df in zip(pollutants, dataframes):
            try:
                config = influxdb.TABLE_CONFIG[pollutant]
                metrics = config['metrics']
                
                # Build metric columns mapping
                metric_columns = transformers.build_metric_columns(metrics)
                
                # Transform the dataframe
                transformed_df = transformers.aggregate_to_long_format(
                    df,
                    metric_columns=metric_columns,
                    version=self.settings.PIPELINE_VERSION
                )
                transformed_dataframes.append(transformed_df)
                logger.debug(f"Transformed {pollutant}: {len(transformed_df)} rows")
            except Exception as e:
                logger.error(f"Error transforming {pollutant}: {e}")
                raise
        
        # Concatenate all transformed dataframes
        result = pl.concat(transformed_dataframes)
        logger.info(f"Total rows after transformation: {len(result)}")
        return result
    
    async def run(self) -> None:
        """
        Execute the complete pipeline: fetch -> transform -> export
        """
        start_time = time.time()
        logger.info("Starting pipeline execution")
        
        try:
            # Step 1: Fetch all data in parallel
            dataframes = await self.fetch_all_data()
            
            # Step 2: Transform all dataframes
            contaminantes_horarios = self.transform_all_data(
                influxdb.POLLUTANTS_TO_PROCESS,
                dataframes
            )
            
            # Step 3: EXPORTERS

            # Export to Excel
            #output_path = self.settings.output_path
            #export_to_excel(contaminantes_horarios, output_path)

            # Export to BigQuery
            bigquery.export_to_bigquery(
                project_id=self.settings.GOOGLE_PROJECT_ID,
                dataset_id=self.settings.BIGQUERY_DATASET_ID,
                table_id=self.settings.BIGQUERY_TABLE_ID,
                df=contaminantes_horarios
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"Pipeline completed successfully in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Pipeline failed after {elapsed_time:.2f} seconds: {e}")
            raise


def setup_logging(log_level: str = "INFO"):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


async def main():
    """Main entry point"""
    # Setup logging first with default level
    setup_logging()
    logger = logging.getLogger(__name__)

    orchestrator = None
    try:
        # Get settings (will validate configuration)
        settings = get_settings()
        # Update logging level from settings if available
        setup_logging(settings.LOG_LEVEL)
        logger = logging.getLogger(__name__)

        orchestrator = PipelineOrchestrator()
        await orchestrator.run()
    except ValueError as e:
        # Configuration error
        logger.error(f"Configuration error: {e}")
        logger.error("Please set INFLUXDB_TOKEN in your .env file or environment variables")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Ensure cleanup happens
        if orchestrator:
            orchestrator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

