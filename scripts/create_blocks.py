"""
Script para crear Prefect Blocks para secrets y estado del pipeline

Este script ayuda a crear los Secret blocks y PipelineStateBlock necesarios
en Prefect Cloud/Server para almacenar de forma segura las credenciales
y el estado incremental del pipeline.

Uso:
    python scripts/create_blocks.py

O crear los blocks manualmente desde la UI de Prefect o CLI:
    prefect block register -m prefect.blocks.system Secret
    # Luego usar la UI para crear los blocks con los nombres:
    # - influxdb-token
    # - motherduck-token
    # - google-project-id (opcional)
    # - pipeline-state (PipelineStateBlock)
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


def create_secret_blocks():
    """
    Create Prefect Secret blocks for pipeline secrets

    This function will prompt for values or use environment variables
    """
    try:
        from prefect.blocks.system import Secret
        from src.utils.blocks import PipelineStateBlock
        import json  # Import json here for GCP credentials handling
        # Try to import GCP credentials
        try:
            from prefect_gcp import GcpCredentials
            PREFECT_GCP_AVAILABLE = True
        except ImportError:
            PREFECT_GCP_AVAILABLE = False
            print("[WARN] prefect-gcp not available, skipping GCP credentials block creation")
    except ImportError as e:
        print(f"Error: Import failed. Make sure Prefect is installed and src.utils.blocks is available: {e}")
        return
    
    print("Creando Prefect Secret blocks para el pipeline...")
    print("=" * 60)
    
    # InfluxDB Token
    influxdb_token = os.getenv("INFLUXDB_TOKEN")
    if not influxdb_token:
        influxdb_token = input("Ingresa el INFLUXDB_TOKEN (o presiona Enter para omitir): ").strip()
    
    if influxdb_token:
        try:
            secret = Secret(value=influxdb_token)
            secret.save("influxdb-token", overwrite=True)
            print("[OK] Block 'influxdb-token' creado exitosamente")
        except Exception as e:
            print(f"[ERROR] Error creando block 'influxdb-token': {e}")
    else:
        print("[WARN] INFLUXDB_TOKEN no proporcionado, omitiendo...")
    
    # MotherDuck Token
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        motherduck_token = input("Ingresa el MOTHERDUCK_TOKEN (o presiona Enter para omitir): ").strip()
    
    if motherduck_token:
        try:
            secret = Secret(value=motherduck_token)
            secret.save("motherduck-token", overwrite=True)
            print("[OK] Block 'motherduck-token' creado exitosamente")
        except Exception as e:
            print(f"[ERROR] Error creando block 'motherduck-token': {e}")
    else:
        print("[WARN] MOTHERDUCK_TOKEN no proporcionado, omitiendo...")
    
    # Google Project ID (opcional, puede ser no sensible)
    google_project_id = os.getenv("GOOGLE_PROJECT_ID")
    if google_project_id:
        try:
            # Store as a dict in case we need more GCP config later
            secret = Secret(value={"project_id": google_project_id})
            secret.save("google-project-id", overwrite=True)
            print("[OK] Block 'google-project-id' creado exitosamente")
        except Exception as e:
            print(f"[ERROR] Error creando block 'google-project-id': {e}")
    else:
        print("[WARN] GOOGLE_PROJECT_ID no proporcionado, omitiendo...")

    # GCP Credentials Block (for BigQuery)
    if PREFECT_GCP_AVAILABLE:
        print("\nCreando GcpCredentials block para BigQuery...")

        # First try to use the service account key file if it exists
        key_file_path = os.path.join(os.path.dirname(__file__), "..", "secrets", "service-account-key.json")
        gcp_credentials_created = False

        if os.path.exists(key_file_path):
            try:
                print(f"[INFO] Usando archivo de credenciales existente: {key_file_path}")
                with open(key_file_path, 'r') as f:
                    service_account_info = json.load(f)
                gcp_credentials = GcpCredentials(service_account_info=service_account_info)
                gcp_credentials.save("gcp-credentials", overwrite=True)
                print("[OK] Block 'gcp-credentials' creado exitosamente desde archivo existente")
                gcp_credentials_created = True
            except Exception as e:
                print(f"[ERROR] Error creando block desde archivo existente: {e}")

        # If file doesn't exist or failed, ask for manual input
        if not gcp_credentials_created:
            gcp_service_account = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
            if not gcp_service_account:
                # Since we're running in non-interactive mode, skip the input
                print("[WARN] No hay archivo de credenciales ni variable de entorno. Omitiendo creación del block GCP.")
            else:
                try:
                    service_account_info = json.loads(gcp_service_account)
                    gcp_credentials = GcpCredentials(service_account_info=service_account_info)
                    gcp_credentials.save("gcp-credentials", overwrite=True)
                    print("[OK] Block 'gcp-credentials' creado exitosamente")
                except Exception as e:
                    print(f"[ERROR] Error creando block 'gcp-credentials': {e}")
    else:
        print("[WARN] prefect-gcp no disponible, omitiendo creación de GCP credentials block")

    # Pipeline State Block
    print("\nCreando PipelineStateBlock para estado incremental...")
    try:
        # Check if block already exists
        try:
            existing_block = PipelineStateBlock.load("pipeline-state")
            print("[OK] Block 'pipeline-state' ya existe")
        except Exception:
            # Create new block with initial state
            state_block = PipelineStateBlock()
            state_block.save("pipeline-state", overwrite=True)
            print("[OK] Block 'pipeline-state' creado exitosamente")
    except Exception as e:
        print(f"[ERROR] Error creando block 'pipeline-state': {e}")

    print("=" * 60)
    print("Completado!")
    print("\nNota: Los blocks también pueden crearse desde la UI de Prefect:")
    print("  1. Ve a Blocks en la UI")
    print("  2. Crea nuevos blocks con los nombres:")
    print("     - influxdb-token (Secret)")
    print("     - motherduck-token (Secret)")
    print("     - google-project-id (Secret, opcional)")
    print("     - gcp-credentials (GcpCredentials, para BigQuery)")
    print("     - pipeline-state (PipelineStateBlock)")


if __name__ == "__main__":
    create_secret_blocks()

