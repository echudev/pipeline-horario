#!/usr/bin/env python3
"""
Script para crear Prefect Blocks para secrets y estado del pipeline.

Este script ayuda a crear los Secret blocks y PipelineStateBlock necesarios
en Prefect Cloud/Server para almacenar de forma segura las credenciales
y el estado incremental del pipeline.

Uso:
    python scripts/create_blocks.py
"""
import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


async def create_secret_blocks():
    """Create Prefect Secret blocks for pipeline secrets."""
    try:
        from prefect.blocks.system import Secret
        from src.utils.blocks import PipelineStateBlock
    except ImportError as e:
        print(f"[ERROR] Import failed: {e}")
        print("Make sure Prefect is installed and src.utils.blocks is available")
        return

    # Try to import GCP credentials
    try:
        from prefect_gcp import GcpCredentials
        PREFECT_GCP_AVAILABLE = True
    except ImportError:
        PREFECT_GCP_AVAILABLE = False
        print("[WARN] prefect-gcp not available, skipping GCP credentials block")

    print("Creating Prefect Secret blocks...")
    print("=" * 60)

    # InfluxDB Token
    influxdb_token = os.getenv("INFLUXDB_TOKEN")
    if influxdb_token:
        try:
            secret = Secret(value=influxdb_token)
            await secret.save("influxdb-token", overwrite=True)
            print("[OK] Block 'influxdb-token' created")
        except Exception as e:
            print(f"[ERROR] Failed to create 'influxdb-token': {e}")
    else:
        print("[WARN] INFLUXDB_TOKEN not set, skipping...")

    # MotherDuck Token
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if motherduck_token:
        try:
            secret = Secret(value=motherduck_token)
            await secret.save("motherduck-token", overwrite=True)
            print("[OK] Block 'motherduck-token' created")
        except Exception as e:
            print(f"[ERROR] Failed to create 'motherduck-token': {e}")
    else:
        print("[WARN] MOTHERDUCK_TOKEN not set, skipping...")

    # Google Project ID
    google_project_id = os.getenv("GOOGLE_PROJECT_ID")
    if google_project_id:
        try:
            secret = Secret(value={"project_id": google_project_id})
            await secret.save("google-project-id", overwrite=True)
            print("[OK] Block 'google-project-id' created")
        except Exception as e:
            print(f"[ERROR] Failed to create 'google-project-id': {e}")
    else:
        print("[WARN] GOOGLE_PROJECT_ID not set, skipping...")

    # GCP Credentials Block
    if PREFECT_GCP_AVAILABLE:
        print("\nCreating GcpCredentials block for BigQuery...")
        
        key_path = Path(__file__).parent.parent / "secrets" / "service-account-key.json"
        
        if key_path.exists():
            try:
                with open(key_path, 'r') as f:
                    service_account_info = json.load(f)
                gcp_credentials = GcpCredentials(service_account_info=service_account_info)
                await gcp_credentials.save("gcp-credentials", overwrite=True)
                print("[OK] Block 'gcp-credentials' created from key file")
            except Exception as e:
                print(f"[ERROR] Failed to create 'gcp-credentials': {e}")
        else:
            gcp_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
            if gcp_json:
                try:
                    service_account_info = json.loads(gcp_json)
                    gcp_credentials = GcpCredentials(service_account_info=service_account_info)
                    await gcp_credentials.save("gcp-credentials", overwrite=True)
                    print("[OK] Block 'gcp-credentials' created from env var")
                except Exception as e:
                    print(f"[ERROR] Failed to create 'gcp-credentials': {e}")
            else:
                print("[WARN] No GCP credentials found, skipping...")

    # Pipeline State Block
    print("\nCreating PipelineStateBlock for incremental state...")
    try:
        try:
            existing = await PipelineStateBlock.load("pipeline-state")
            print("[OK] Block 'pipeline-state' already exists")
        except ValueError:
            state_block = PipelineStateBlock()
            await state_block.save("pipeline-state", overwrite=False)
            print("[OK] Block 'pipeline-state' created")
    except Exception as e:
        print(f"[ERROR] Failed to create 'pipeline-state': {e}")

    print("=" * 60)
    print("Done!")
    print("\nBlocks can also be created from the Prefect UI:")
    print("  - influxdb-token (Secret)")
    print("  - motherduck-token (Secret)")
    print("  - google-project-id (Secret)")
    print("  - gcp-credentials (GcpCredentials)")
    print("  - pipeline-state (PipelineStateBlock)")


def main():
    asyncio.run(create_secret_blocks())


if __name__ == "__main__":
    main()
