# Pipeline Horario - Procesamiento de Datos de Contaminantes

Pipeline ETL para procesamiento de datos horarios de contaminantes atmosféricos usando Prefect 3.

## Estructura del Proyecto

```
pipeline-horario/
├── flows/                          # Flujos Prefect
│   ├── __init__.py
│   └── pipeline_horario.py         # Flow principal del pipeline
├── src/
│   └── utils/                      # Utilidades consolidadas
│       ├── __init__.py
│       ├── clients.py               # Clientes (InfluxDB, BigQuery, MotherDuck)
│       ├── transformers.py          # Transformaciones de datos
│       ├── validators.py            # Validación de datos
│       └── exporters.py             # Exportación a diferentes destinos
├── config/                         # Configuraciones
│   ├── __init__.py
│   ├── settings.py                  # Settings con soporte para Prefect Blocks
│   └── pollutants.py                # Configuración de contaminantes
├── tests/                          # Tests
│   ├── unit/                        # Tests unitarios
│   └── integration/                # Tests de integración
├── scripts/                        # Scripts auxiliares
│   └── create_blocks.py             # Script para crear Prefect Blocks
├── secrets/                        # Credenciales (gitignorado)
│   └── service-account-key.json
├── output/                         # Archivos de salida generados
├── prefect.yaml                    # Configuración de deployments
├── prefect.toml                    # Configuración global de Prefect
└── pyproject.toml                  # Dependencias del proyecto
```

## Características

- **Orquestación con Prefect 3**: Flujos y tareas estructurados con Prefect
- **Procesamiento paralelo**: Extracción de datos de múltiples contaminantes en paralelo
- **Transformación de datos**: Conversión de formato wide a long con agregaciones horarias
- **Múltiples destinos**: Exportación a Excel, BigQuery y MotherDuck
- **Gestión segura de secrets**: Uso de Prefect Blocks para credenciales sensibles

## Requisitos

- Python 3.10 o superior
- Prefect 3.x
- Acceso a InfluxDB con datos de contaminantes
- (Opcional) Cuenta de Google Cloud para BigQuery
- (Opcional) Cuenta de MotherDuck

## Dependencias

El pipeline requiere las siguientes librerías principales:

- **prefect** (>=3.0.0): Framework de orquestación
- **prefect-gcp** (>=0.5.0): Integración con Google Cloud (BigQuery)
- **polars** (>=1.35.0): Procesamiento de datos
- **pandas** (>=2.0.0): Necesario para BigQuery
- **influxdb3-python** (>=0.16.0): Cliente InfluxDB
- **google-cloud-bigquery** (>=3.0.0): Cliente BigQuery (fallback)
- **duckdb** (>=1.0.0): Cliente MotherDuck
- **xlsxwriter** (>=3.0.0): Exportación a Excel
- **python-dotenv** (>=1.0.0): Configuración

Ver `DEPENDENCIES.md` para detalles completos.

### Prefect Cloud Serverless

**Importante**: Prefect Cloud Serverless **NO** tiene estas dependencias preinstaladas. Las instala en runtime.

El `prefect.yaml` ya está configurado para instalar dependencias desde `pyproject.toml` automáticamente usando `pip install -e .`. Si usas Prefect Managed Execution, las dependencias se instalarán cada vez que se ejecute el flow.

## Instalación

1. Clonar el repositorio:
```bash
git clone <repository-url>
cd pipeline-horario
```

2. Instalar dependencias usando `uv`:
```bash
uv sync
```

O usando `pip`:
```bash
pip install -e .
```

3. Configurar variables de entorno o Prefect Blocks (ver sección de Configuración)

## Configuración

### Opción 1: Usando Prefect Blocks (Recomendado)

Los secrets se almacenan de forma segura en Prefect Cloud/Server usando Blocks.

#### Crear Blocks desde la UI de Prefect:

1. Ve a la UI de Prefect (Cloud o Server)
2. Navega a **Blocks**
3. Crea los siguientes blocks:

**Secret Blocks:**
   - `influxdb-token`: Token de acceso a InfluxDB
   - `motherduck-token`: Token de acceso a MotherDuck (opcional)
   - `google-project-id`: ID del proyecto de Google Cloud (opcional)

**GCP Credentials Block (para BigQuery):**
   - `gcp-credentials`: Credenciales de Google Cloud Platform usando `prefect-gcp`

**Custom Blocks:**
   - `pipeline-state`: Block personalizado para estado incremental

#### Crear Blocks usando el script:

```bash
python scripts/create_blocks.py
```

Este script te guiará para crear los blocks necesarios usando valores de variables de entorno o entrada manual.

#### Crear Blocks desde la CLI:

```bash
# Crear block para InfluxDB token
prefect block register -m prefect.blocks.system Secret
# Luego usar la UI o Python SDK para guardar el valor
```

### Opción 2: Variables de Entorno (Fallback)

Si no usas Prefect Blocks, puedes usar variables de entorno. Crea un archivo `.env` en la raíz del proyecto:

```env
# InfluxDB
INFLUXDB_HOST=https://us-east-1-1.aws.cloud2.influxdata.com/
INFLUXDB_TOKEN=tu-token-aqui
INFLUXDB_DATABASE=minutales

# BigQuery (opcional)
GOOGLE_PROJECT_ID=tu-proyecto-id
BIGQUERY_DATASET_ID=test_dataset
BIGQUERY_TABLE_ID=promedios_horarios

# MotherDuck (opcional)
MOTHERDUCK_TOKEN=tu-token-aqui
MOTHERDUCK_DATABASE=tu-database

# Pipeline
PIPELINE_VERSION=v0.8.6
OUTPUT_DIR=output
LOG_LEVEL=INFO
```

#### Configuración de Credenciales GCP para BigQuery:

Para usar BigQuery, necesitas configurar credenciales de Google Cloud:

1. **Crear una Service Account** en Google Cloud Console
2. **Descargar el JSON** de la service account key
3. **Crear el block** `gcp-credentials` usando el script o UI

El código automáticamente detectará si `prefect-gcp` está disponible y usará `GcpCredentials` de Prefect, o caerá de vuelta al cliente estándar de BigQuery.

**Nota**: El código intentará cargar desde Prefect Blocks primero, y si no están disponibles, usará variables de entorno como fallback.

## Uso

### Ejecutar el Flow Localmente

```bash
# Ejecutar directamente
python flows/pipeline_horario.py

# O usando Prefect CLI
prefect run flows/pipeline_horario.py:pipeline_horario
```

### Crear y Ejecutar un Deployment

1. **Crear el deployment**:
```bash
prefect deploy
```

O editar `prefect.yaml` y ejecutar:
```bash
prefect deploy --name pipeline-horario-deployment
```

2. **Ejecutar el deployment**:
```bash
prefect deployment run pipeline-horario/pipeline-horario-deployment
```

### Parámetros del Flow

El flow `pipeline_horario` acepta los siguientes parámetros:

- `pollutants` (List[str] | None): Lista de contaminantes a procesar. Si es None, usa todos los configurados.
- `export_excel` (bool): Si True, exporta a Excel (default: True)
- `export_bigquery` (bool): Si True, exporta a BigQuery (default: False)
- `export_motherduck` (bool): Si True, exporta a MotherDuck (default: False)

Ejemplo:
```python
from flows.pipeline_horario import pipeline_horario
import asyncio

# Ejecutar solo para CO y NOx, exportar a Excel y BigQuery
asyncio.run(pipeline_horario(
    pollutants=['co', 'nox'],
    export_excel=True,
    export_bigquery=True,
    export_motherduck=False
))
```

## Testing

Ejecutar todos los tests:
```bash
pytest
```

Ejecutar solo tests unitarios:
```bash
pytest tests/unit/
```

Ejecutar solo tests de integración:
```bash
pytest tests/integration/
```

Ejecutar con cobertura:
```bash
pytest --cov=src --cov=flows --cov-report=html
```

## Estructura del Pipeline

El pipeline ejecuta las siguientes etapas:

1. **Extract (Extracción)**: 
   - Obtiene datos de InfluxDB para múltiples contaminantes en paralelo
   - Cada contaminante se procesa como una task independiente

2. **Transform (Transformación)**:
   - Convierte datos de formato wide a long
   - Agrega datos por hora
   - Filtra registros con status válido ('k')
   - Agrega metadatos (versión del pipeline)

3. **Load (Carga)**:
   - Exporta a Excel (por defecto)
   - Exporta a BigQuery (opcional)
   - Exporta a MotherDuck (opcional)

## Contaminantes Soportados

El pipeline procesa los siguientes contaminantes (configurados en `config/pollutants.py`):

- `co`: Monóxido de carbono
- `nox`: Óxidos de nitrógeno (NO, NO2, NOx)
- `pm10`: Material particulado 10
- `so2`: Dióxido de azufre
- `o3`: Ozono
- `meteo`: Datos meteorológicos

## Desarrollo

### Agregar un Nuevo Contaminante

1. Editar `config/pollutants.py`:
```python
TABLE_CONFIG['nuevo_contaminante'] = {
    'table': 'nombre_tabla_influxdb',
    'metrics': ['metrica1_mean', 'metrica2_mean']
}
```

2. El contaminante se incluirá automáticamente en `POLLUTANTS_TO_PROCESS`

### Agregar un Nuevo Destino de Exportación

1. Agregar función en `src/utils/exporters.py`
2. Crear una task en `flows/pipeline_horario.py`
3. Agregar parámetro al flow principal
4. Actualizar `prefect.yaml` si es necesario

## Troubleshooting

### Error: "INFLUXDB_TOKEN is required"

- Verifica que hayas creado el Prefect Block `influxdb-token` o
- Configura la variable de entorno `INFLUXDB_TOKEN`

### Error al conectar a InfluxDB

- Verifica que `INFLUXDB_HOST` y `INFLUXDB_DATABASE` estén correctos
- Verifica que el token tenga permisos de lectura

### Tests fallan

- Asegúrate de tener todas las dependencias instaladas: `uv sync` o `pip install -e .`
- Verifica que los mocks estén configurados correctamente

## Contribuir

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Agrega nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Abre un Pull Request

## Licencia

[Especificar licencia]

## Contacto

[Información de contacto]
