"""
Pipeline orchestrator - coordinates data fetching, transformation, and export
"""
import asyncio
import logging
import time
from typing import List

import polars as pl

from config import get_settings, TABLE_CONFIG, POLLUTANTS_TO_PROCESS
from influxdb import get_influxdb_client, fetch_data
from bigquery import get_bigquery_client
from transformers import aggregate_to_long_format, build_metric_columns
from exporters import export_to_bigquery

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the entire pipeline execution"""
    
    def __init__(self):
        self.settings = get_settings()
        self.influxdb_client = get_influxdb_client()
        self.bigquery_client = get_bigquery_client()
    
    async def fetch_all_data(self) -> List[pl.DataFrame]:
        """
        Fetch data for all pollutants in parallel
        
        Returns:
            List of DataFrames, one per pollutant
        """
        logger.info(f"Fetching data for {len(POLLUTANTS_TO_PROCESS)} pollutants")
        
        try:
            dataframes = await asyncio.gather(*[
                fetch_data(self.influxdb_client, pollutant) 
                for pollutant in POLLUTANTS_TO_PROCESS
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
                config = TABLE_CONFIG[pollutant]
                metrics = config['metrics']
                
                # Build metric columns mapping
                metric_columns = build_metric_columns(metrics)
                
                # Transform the dataframe
                transformed_df = aggregate_to_long_format(
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
                POLLUTANTS_TO_PROCESS,
                dataframes
            )
            
            # Step 3: EXPORTERS

            # Export to Excel
            #output_path = self.settings.output_path
            #export_to_excel(contaminantes_horarios, output_path)

            # Export to BigQuery
            export_to_bigquery(
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

