#!/usr/bin/env python3
"""
Backfilling script for pipeline-horario.

This script allows manual backfilling of missing hours in the pipeline.
All operations are async-consistent with the main pipeline.
"""
import asyncio
import argparse
from datetime import datetime, timezone
from typing import List, Optional

import polars as pl

from config import get_settings, POLLUTANTS_TO_PROCESS
from config.pollutants import TABLE_CONFIG
from src.utils import (
    influxdb_client,
    backfill_hours,
    find_missing_hours,
    build_metric_columns,
    aggregate_to_long_format,
    export_to_excel,
    get_pipeline_state,
    ensure_utc,
    get_current_hour_start,
    get_previous_hour_start,
)


async def run_backfill(
    start_hour: datetime,
    end_hour: datetime,
    pollutants: Optional[List[str]] = None,
    export_excel: bool = True
) -> None:
    """
    Backfill missing hours for specified pollutants.

    Args:
        start_hour: Start hour for backfilling (inclusive)
        end_hour: End hour for backfilling (exclusive)
        pollutants: List of pollutants to backfill (None = all)
        export_excel: Whether to export results to Excel
    """
    settings = get_settings()
    pollutants_to_process = pollutants or POLLUTANTS_TO_PROCESS

    print(f"Starting backfill from {start_hour} to {end_hour}")
    print(f"Pollutants: {pollutants_to_process}")

    all_data: List[pl.DataFrame] = []

    async with influxdb_client() as client:
        for pollutant in pollutants_to_process:
            print(f"\nBackfilling {pollutant}...")
            
            backfilled = await backfill_hours(
                client, pollutant, start_hour, end_hour
            )

            if not backfilled:
                print(f"  No data found for {pollutant}")
                continue

            print(f"  Found {len(backfilled)} hours with data")

            config = TABLE_CONFIG[pollutant]
            metric_columns = build_metric_columns(config['metrics'])

            for hour, df in backfilled:
                try:
                    transformed = aggregate_to_long_format(
                        df,
                        metric_columns=metric_columns,
                        version=settings.PIPELINE_VERSION
                    )
                    if not transformed.is_empty():
                        all_data.append(transformed)
                        print(f"    {hour}: {len(transformed)} rows")
                except Exception as e:
                    print(f"    {hour}: ERROR - {e}")

    if all_data:
        combined = pl.concat(all_data)
        print(f"\nTotal backfilled rows: {len(combined)}")

        if export_excel and len(combined) > 0:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_path = f"output/backfill_{timestamp}.xlsx"
            export_to_excel(combined, output_path)
            print(f"Exported to: {output_path}")
    else:
        print("\nNo data was backfilled")


async def backfill_from_state(state_block_name: str = "pipeline-state") -> None:
    """
    Backfill missing hours based on current pipeline state.

    Args:
        state_block_name: Name of the Prefect block containing pipeline state
    """
    print(f"Checking for missing hours (block: {state_block_name})...")

    state_manager = get_pipeline_state(state_block_name)
    last_processed = await state_manager.get_last_processed_hour()

    if last_processed is None:
        print("No pipeline state found. Run the pipeline first.")
        return

    current_time = datetime.now(timezone.utc)
    missing = find_missing_hours(last_processed, current_time)

    if not missing:
        print("No missing hours. Pipeline is up to date.")
        return

    print(f"Found {len(missing)} missing hours:")
    for hour in missing[:5]:
        print(f"  - {hour}")
    if len(missing) > 5:
        print(f"  ... and {len(missing) - 5} more")

    # Backfill
    start_hour = missing[0]
    end_hour = get_current_hour_start()
    
    await run_backfill(start_hour, end_hour)

    # Update state
    print("\nUpdating pipeline state...")
    try:
        last_backfilled = get_previous_hour_start()
        await state_manager.set_last_processed_hour(last_backfilled)
        await state_manager.update_metadata("last_backfill", current_time.isoformat())
        print(f"State updated: last_processed_hour={last_backfilled}")
    except Exception as e:
        print(f"Warning: Could not update state: {e}")


def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string in ISO format."""
    dt = datetime.fromisoformat(date_str)
    return ensure_utc(dt)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing hours in pipeline-horario"
    )
    parser.add_argument(
        "--start",
        type=parse_datetime,
        help="Start hour (ISO format: 2024-01-01T10:00:00)"
    )
    parser.add_argument(
        "--end",
        type=parse_datetime,
        help="End hour (ISO format: 2024-01-01T15:00:00)"
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
        help="Prefect block name for state (default: pipeline-state)"
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Don't export results to Excel"
    )

    args = parser.parse_args()

    if args.missing:
        asyncio.run(backfill_from_state(args.state_block))
    elif args.start and args.end:
        asyncio.run(run_backfill(
            args.start,
            args.end,
            args.pollutants,
            not args.no_export
        ))
    else:
        parser.error("Either --missing or both --start and --end must be specified")


if __name__ == "__main__":
    main()
