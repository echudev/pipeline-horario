#!/usr/bin/env python3
"""
Backfilling script for pipeline-horario

This script allows manual backfilling of missing hours in the pipeline.
"""
import asyncio
import argparse
from datetime import datetime, timezone
from typing import List
import polars as pl

from config import get_settings, POLLUTANTS_TO_PROCESS, TABLE_CONFIG
from src.utils.clients import (
    get_influxdb_client,
    close_influxdb_client,
    backfill_missing_hours
)
from src.utils.transformers import (
    build_metric_columns,
    aggregate_to_long_format,
)
from src.utils.exporters import export_to_excel
from src.utils.state import get_pipeline_state


async def backfill_hours(
    start_hour: datetime,
    end_hour: datetime,
    pollutants: List[str] = None,
    export_excel: bool = True
) -> None:
    """
    Backfill missing hours for specified pollutants

    Args:
        start_hour: Start hour for backfilling (inclusive)
        end_hour: End hour for backfilling (exclusive)
        pollutants: List of pollutants to backfill. If None, uses all pollutants
        export_excel: Whether to export results to Excel
    """
    settings = get_settings()
    pollutants_to_process = pollutants or POLLUTANTS_TO_PROCESS

    print(f"Starting backfill from {start_hour} to {end_hour}")
    print(f"Pollutants to process: {pollutants_to_process}")

    client = get_influxdb_client()

    try:
        all_backfilled_data = []

        # Backfill each pollutant
        for pollutant in pollutants_to_process:
            print(f"Backfilling {pollutant}...")
            backfilled_hours = await backfill_missing_hours(
                client, pollutant, start_hour, end_hour
            )

            if backfilled_hours:
                print(f"Found {len(backfilled_hours)} hours with data for {pollutant}")

                # Transform each hour's data
                for hour, df in backfilled_hours:
                    try:
                        # Build metric columns mapping
                        config = TABLE_CONFIG[pollutant]
                        metrics = config['metrics']
                        metric_columns = build_metric_columns(metrics)

                        # Transform the dataframe
                        transformed_df = aggregate_to_long_format(
                            df,
                            metric_columns=metric_columns,
                            version=settings.PIPELINE_VERSION
                        )

                        all_backfilled_data.append(transformed_df)
                        print(f"  Processed hour {hour}: {len(transformed_df)} rows")

                    except Exception as e:
                        print(f"  Error processing hour {hour} for {pollutant}: {e}")
                        continue
            else:
                print(f"No data found for {pollutant} in the specified range")

        # Combine all backfilled data
        if all_backfilled_data:
            combined_df = pl.concat(all_backfilled_data)
            print(f"Total backfilled rows: {len(combined_df)}")

            # Export to Excel if requested
            if export_excel and len(combined_df) > 0:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                output_path = f"output/backfill_{timestamp}.xlsx"
                export_to_excel(combined_df, output_path)
                print(f"Backfilled data exported to: {output_path}")
        else:
            print("No data was backfilled")

    except Exception as e:
        print(f"Error during backfilling: {e}")
        raise
    finally:
        close_influxdb_client()


async def backfill_missing_from_state(state_block_name: str = "pipeline-state") -> None:
    """
    Backfill missing hours based on current pipeline state stored in Prefect Blocks

    Args:
        state_block_name: Name of the Prefect block containing pipeline state
    """
    print(f"Checking for missing hours based on pipeline state in block '{state_block_name}'...")

    state_manager = get_pipeline_state(state_block_name)
    last_processed = state_manager.get_last_processed_hour()

    if last_processed is None:
        print("No pipeline state found. Run the pipeline first to establish a baseline.")
        return

    current_time = datetime.now(timezone.utc)
    current_hour_start = current_time.replace(minute=0, second=0, microsecond=0)

    # Find missing hours
    from src.utils.clients import find_missing_hours
    missing_hours = find_missing_hours(last_processed, current_time)

    if not missing_hours:
        print("No missing hours found. Pipeline is up to date.")
        return

    print(f"Found {len(missing_hours)} missing hours:")
    for hour in missing_hours:
        print(f"  - {hour}")

    # Backfill from the first missing hour to current hour
    start_hour = missing_hours[0]
    end_hour = current_hour_start

    await backfill_hours(start_hour, end_hour)

    # Update pipeline state after successful backfill
    print("Updating pipeline state after successful backfill...")
    try:
        # Set the state to the last backfilled hour (current hour - 1)
        last_backfilled_hour = current_hour_start.replace(hour=current_hour_start.hour - 1)
        state_manager.set_last_processed_hour(last_backfilled_hour)
        state_manager.update_metadata("last_backfill", current_time.isoformat())
        print(f"Pipeline state updated: last processed hour = {last_backfilled_hour}")
    except Exception as e:
        print(f"Warning: Could not update pipeline state: {e}")


def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string in ISO format"""
    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main():
    parser = argparse.ArgumentParser(description="Backfill missing hours in pipeline-horario")
    parser.add_argument(
        "--start",
        type=parse_datetime,
        help="Start hour for backfilling (ISO format: 2024-01-01T10:00:00)"
    )
    parser.add_argument(
        "--end",
        type=parse_datetime,
        help="End hour for backfilling (ISO format: 2024-01-01T15:00:00)"
    )
    parser.add_argument(
        "--pollutants",
        nargs="*",
        choices=POLLUTANTS_TO_PROCESS,
        help="Pollutants to backfill (default: all)"
    )
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Backfill missing hours based on pipeline state"
    )
    parser.add_argument(
        "--state-block",
        default="pipeline-state",
        help="Name of the Prefect block containing pipeline state (default: pipeline-state)"
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Don't export results to Excel"
    )

    args = parser.parse_args()

    if args.missing:
        # Backfill missing hours based on state
        asyncio.run(backfill_missing_from_state(args.state_block))
    elif args.start and args.end:
        # Backfill specific range
        export_excel = not args.no_export
        asyncio.run(backfill_hours(
            args.start,
            args.end,
            args.pollutants,
            export_excel
        ))
    else:
        parser.error("Either --missing or both --start and --end must be specified")


if __name__ == "__main__":
    main()
