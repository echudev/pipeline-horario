# Arquitectura de Pipeline ETL Profesional

## 1. Estructura del Proyecto

```text
etl-pipeline/
│
├── config/
│   ├── __init__.py
│   ├── settings.py              # Configuraciones generales
│   ├── database.yaml            # Configs de bases de datos
│   ├── storage.yaml             # Configs de buckets/storage
│   └── logging.yaml             # Configuración de logs
│
├── src/
│   ├── __init__.py
│   │
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── base_extractor.py   # Clase abstracta
│   │   ├── db_extractor.py     # Extractor de DB origen
│   │   └── validators.py       # Validación de datos extraídos
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── base_transformer.py
│   │   ├── data_cleaner.py     # Limpieza de datos
│   │   ├── data_enricher.py    # Enriquecimiento
│   │   └── business_rules.py   # Reglas de negocio
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   ├── base_loader.py
│   │   ├── db_loader.py        # Carga a DB destino
│   │   ├── bucket_loader.py    # Carga a bucket
│   │   └── batch_processor.py  # Procesamiento por lotes
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── connection_manager.py
│   │   ├── logger.py
│   │   ├── exceptions.py
│   │   └── helpers.py
│   │
│   └── orchestration/
│       ├── __init__.py
│       ├── pipeline.py         # Orquestador principal (punto de entrada)
│       └── scheduler.py        # Programación de tareas
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_extract.py
│   │   ├── test_transform.py
│   │   └── test_load.py
│   ├── integration/
│   │   └── test_pipeline.py
│   └── fixtures/
│       └── sample_data.json
│
├── scripts/
│   ├── setup_databases.py
│   ├── run_etl.py
│   └── health_check.py
│
├── monitoring/
│   ├── metrics.py
│   ├── alerts.py
│   └── dashboard_config.json
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── runbook.md
│
├── .env.example
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## 2. Diagrama de Arquitectura

```text
┌─────────────────────────────────────────────────────────────────┐
│                         ETL PIPELINE                             │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│              │         │              │         │              │
│  EXTRACT     │────────>│  TRANSFORM   │────────>│    LOAD      │
│              │         │              │         │              │
└──────────────┘         └──────────────┘         └──────────────┘
       │                        │                      │      │
       │                        │                      │      │
       v                        v                      v      v
┌──────────────┐         ┌──────────────┐    ┌──────────┐ ┌─────┐
│              │         │              │    │          │ │     │
│  PostgreSQL  │         │  - Limpieza  │    │  Redshift│ │ S3  │
│  (Origen)    │         │  - Validación│    │  (Destino│ │Bucket│
│              │         │  - Agregación│    │          │ │     │
└──────────────┘         │  - Conversión│    └──────────┘ └─────┘
                         └──────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    COMPONENTES TRANSVERSALES                     │
├─────────────────────────────────────────────────────────────────┤
│  • Logging centralizado (ELK Stack / CloudWatch)                │
│  • Monitoreo (Prometheus / Grafana)                             │
│  • Alertas (PagerDuty / Email)                                  │
│  • Retry mechanism con backoff exponencial                      │
│  • Circuit breaker para servicios externos                      │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Flujo de Datos Detallado

```text
1. EXTRACT
   ├─> Conexión a DB origen
   ├─> Query con paginación
   ├─> Validación de esquema
   ├─> Checkpoint (último ID procesado)
   └─> Almacenamiento temporal en memoria/disco

2. TRANSFORM
   ├─> Limpieza (nulls, duplicados)
   ├─> Normalización de formatos
   ├─> Aplicación de reglas de negocio
   ├─> Agregaciones y cálculos
   ├─> Validación de calidad de datos
   └─> Preparación para carga

3. LOAD
   ├─> Carga paralela a DB destino (batch upsert)
   ├─> Exportación a formato Parquet
   ├─> Carga a bucket (particionado por fecha)
   ├─> Actualización de metadatos
   └─> Registro de auditoría
```

## 4. Ejemplo de Código: Pipeline Principal

```python
# src/orchestration/pipeline.py

from datetime import datetime, timezone
from src.extract.db_extractor import DatabaseExtractor
from src.transform.data_cleaner import DataCleaner
from src.load.db_loader import DatabaseLoader
from src.load.bucket_loader import BucketLoader
from src.utils.logger import setup_logger
from src.utils.connection_manager import ConnectionManager

class ETLPipeline:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logger(__name__)
        self.conn_manager = ConnectionManager(config)
        
    def run(self, execution_date=None):
        """Ejecuta el pipeline ETL completo"""
        execution_date = execution_date or datetime.now(timezone.utc)
        
        try:
            self.logger.info(f"Iniciando pipeline: {execution_date}")
            
            # Extract
            data = self._extract()
            
            # Transform
            transformed_data = self._transform(data)
            
            # Load
            self._load(transformed_data, execution_date)
            
            self.logger.info("Pipeline completado exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error en pipeline: {str(e)}")
            raise
            
        finally:
            self.conn_manager.close_all()
    
    def _extract(self):
        extractor = DatabaseExtractor(self.config.source_db)
        return extractor.extract()
    
    def _transform(self, data):
        cleaner = DataCleaner(self.config.transform_rules)
        return cleaner.clean_and_transform(data)
    
    def _load(self, data, execution_date):
        # Carga a DB
        db_loader = DatabaseLoader(self.config.target_db)
        db_loader.load(data)
        
        # Carga a bucket
        bucket_loader = BucketLoader(self.config.bucket)
        bucket_loader.upload(data, execution_date)
```

## 5. Configuración (config/database.yaml)

```yaml
source_database:
  type: postgresql
  host: source-db.example.com
  port: 5432
  database: production
  user: ${DB_USER}
  password: ${DB_PASSWORD}
  pool_size: 10
  
target_database:
  type: redshift
  host: warehouse.example.com
  port: 5439
  database: analytics
  user: ${DW_USER}
  password: ${DW_PASSWORD}
  
storage:
  provider: aws
  bucket: data-lake-production
  region: us-east-1
  path_pattern: data/{year}/{month}/{day}/
```

## 6. Estrategias Profesionales

### Manejo de Errores

- Retry con backoff exponencial
- Dead letter queue para registros fallidos
- Alertas automáticas en Slack/Email

### Performance

- Procesamiento paralelo con multiprocessing/async
- Batch loading (bulk insert)
- Compresión de datos (Parquet con Snappy)
- Connection pooling

### Calidad de Datos

- Validación de esquemas (Great Expectations)
- Data profiling
- Métricas de calidad por ejecución

### Observabilidad

- Logs estructurados (JSON)
- Métricas: registros procesados, tiempo de ejecución, errores
- Tracing distribuido (OpenTelemetry)

### Idempotencia

- Upserts en lugar de inserts
- Particiones por fecha en bucket
- Checkpoints para reanudar ejecuciones

## 7. Ejecución del Proyecto

### Punto de Entrada Principal

El punto de entrada principal del pipeline es `src/orchestration/pipeline.py`:

```bash
# Ejecutar el pipeline completo
python src/orchestration/pipeline.py
```

### Variables de Entorno

Crear un archivo `.env` basado en `.env.example`:

```bash
cp .env.example .env
# Editar .env con tus credenciales
```

### Ejecutar Tests

```bash
# Ejecutar todos los tests
pytest

# Ejecutar solo tests unitarios
pytest tests/unit/

# Ejecutar solo tests de integración
pytest tests/integration/

# Ejecutar con cobertura
pytest --cov=src --cov-report=html
```

## 8. Herramientas Comunes

**Orquestación:** Apache Airflow, Prefect, Dagster
**Procesamiento:** Pandas, Polars, Dask
**DBs:** PostgreSQL, MySQL, Redshift, Snowflake, BigQuery
**Storage:** AWS S3, Azure Blob, GCS
**Monitoreo:** Datadog, New Relic, Prometheus + Grafana
**Testing:** Pytest, Great Expectations
**CI/CD:** GitHub Actions, GitLab CI, Jenkins
