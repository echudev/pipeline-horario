# Arquitectura Completa: Blocks, State y Pipeline

## Diagrama de Arquitectura General

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PREFECT CLOUD/SERVER                            │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Blocks Storage (Persistencia)                                    │  │
│  │                                                                   │  │
│  │ ┌─────────────────────────────────────────────────────────────┐ │  │
│  │ │ pipeline-state (PipelineStateBlock)                         │ │  │
│  │ │ ├─ last_processed_hour: 2024-01-15T11:00:00+00:00          │ │  │
│  │ │ ├─ pipeline_version: v0.8.6                                │ │  │
│  │ │ └─ metadata: {...}                                         │ │  │
│  │ └─────────────────────────────────────────────────────────────┘ │  │
│  │                                                                   │  │
│  │ ┌─────────────────────────────────────────────────────────────┐ │  │
│  │ │ influxdb-token (Secret)                                     │ │  │
│  │ │ motherduck-token (Secret)                                   │ │  │
│  │ │ gcp-credentials (GcpCredentials)                            │ │  │
│  │ └─────────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
                    await load() / await save()
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                      Tu Aplicación Python                               │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ flows/pipeline_horario.py (Flow Principal)                       │  │
│  │                                                                   │  │
│  │  @flow(name="pipeline-horario")                                  │  │
│  │  async def pipeline_horario():                                   │  │
│  │    # 1. Obtener estado                                           │  │
│  │    state_manager = get_pipeline_state("pipeline-state")          │  │
│  │    last_hour = await state_manager.get_last_processed_hour()     │  │
│  │                                                                   │  │
│  │    # 2. Procesar datos                                           │  │
│  │    async with influxdb_client() as client:                       │  │
│  │      df = await fetch_incremental_data(client, "co", last_hour)  │  │
│  │                                                                   │  │
│  │    # 3. Guardar estado                                           │  │
│  │    await state_manager.set_last_processed_hour(processed_hour)   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ src/utils/state.py (Wrapper Async)                              │  │
│  │                                                                   │  │
│  │  class PipelineState:                                            │  │
│  │    async def _load_or_create_block():                            │  │
│  │      # Carga desde Prefect Cloud o crea nuevo                   │  │
│  │      self._block = await PipelineStateBlock.load(...)            │  │
│  │                                                                   │  │
│  │    async def set_last_processed_hour(hour):                      │  │
│  │      block = await self._load_or_create_block()                  │  │
│  │      block.last_processed_hour = hour                            │  │
│  │      await block.save(...)  # Guarda en Prefect Cloud            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ src/utils/blocks.py (Objeto Prefect Block)                      │  │
│  │                                                                   │  │
│  │  class PipelineStateBlock(Block):                                │  │
│  │    last_processed_hour: Optional[datetime] = None                │  │
│  │    pipeline_version: str = "v1.0.0"                              │  │
│  │    metadata: Dict[str, Any] = {}                                 │  │
│  │                                                                   │  │
│  │    def _save_block(self):                                        │  │
│  │      self.save(name=self._block_document_name, overwrite=True)   │  │
│  │      # Envía a Prefect Cloud                                     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ src/utils/clients.py (Clientes de Datos)                        │  │
│  │                                                                   │  │
│  │  async with influxdb_client() as client:                         │  │
│  │    df = await fetch_incremental_data(client, "co", last_hour)    │  │
│  │    # Obtiene solo datos nuevos desde last_hour                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
                    Conexión a InfluxDB, BigQuery, MotherDuck
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS                                │
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐     │
│  │   InfluxDB       │  │   BigQuery       │  │   MotherDuck     │     │
│  │                  │  │                  │  │                  │     │
│  │ Datos minutales  │  │ Datos agregados  │  │ Datos agregados  │     │
│  │ de contaminantes │  │ por hora         │  │ por hora         │     │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Flujo de Datos: Paso a Paso

### 1. Inicialización (Primera Ejecución)

```
┌─────────────────────────────────────────────────────────────┐
│ Ejecución 1: Primera vez                                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ 1. Crear block en Prefect Cloud                             │
│    $ python scripts/create_blocks.py                        │
│    → Crea "pipeline-state" con last_processed_hour = None   │
│                                                              │
│ 2. Ejecutar pipeline                                        │
│    $ prefect deployment run pipeline-horario/...            │
│                                                              │
│ 3. Cargar estado                                            │
│    state_manager = get_pipeline_state("pipeline-state")     │
│    last_hour = await state_manager.get_last_processed_hour()│
│    → Carga desde Prefect Cloud: None                        │
│                                                              │
│ 4. Calcular rango                                           │
│    next_hour = await state_manager.get_next_hour_to_process()
│    → Como es None, retorna: previous_hour_start()           │
│    → Ej: 2024-01-15 10:00:00 UTC                            │
│                                                              │
│ 5. Procesar datos                                           │
│    df = await fetch_incremental_data(client, "co", None)    │
│    → Obtiene datos de 2024-01-15 10:00 a 11:00              │
│                                                              │
│ 6. Guardar estado                                           │
│    await state_manager.set_last_processed_hour(             │
│        datetime(2024, 1, 15, 10, 0, 0)                      │
│    )                                                         │
│    → Guarda en Prefect Cloud ✅                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 2. Ejecución Incremental (Siguientes Ejecuciones)

```
┌─────────────────────────────────────────────────────────────┐
│ Ejecución 2: Una hora después                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ 1. Cargar estado                                            │
│    last_hour = await state_manager.get_last_processed_hour()│
│    → Carga desde Prefect Cloud: 2024-01-15 10:00:00 ✅     │
│                                                              │
│ 2. Calcular rango                                           │
│    next_hour = await state_manager.get_next_hour_to_process()
│    → last_hour + 1 = 2024-01-15 11:00:00                    │
│                                                              │
│ 3. Procesar datos (SOLO NUEVOS)                             │
│    df = await fetch_incremental_data(                       │
│        client, "co", 2024-01-15 10:00:00                    │
│    )                                                         │
│    → Obtiene datos de 2024-01-15 11:00 a 12:00 (nuevos)     │
│    → NO reprocesa datos antiguos ✅                         │
│                                                              │
│ 4. Guardar estado                                           │
│    await state_manager.set_last_processed_hour(             │
│        datetime(2024, 1, 15, 11, 0, 0)                      │
│    )                                                         │
│    → Actualiza en Prefect Cloud ✅                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 3. Recuperación ante Fallos

```
┌─────────────────────────────────────────────────────────────┐
│ Ejecución 3: Fallo durante procesamiento                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ 1. Cargar estado                                            │
│    last_hour = await state_manager.get_last_processed_hour()│
│    → Carga desde Prefect Cloud: 2024-01-15 11:00:00 ✅     │
│                                                              │
│ 2. Calcular rango                                           │
│    next_hour = 2024-01-15 12:00:00                          │
│                                                              │
│ 3. Procesar datos                                           │
│    df = await fetch_incremental_data(...)                   │
│    → Obtiene datos de 2024-01-15 12:00 a 13:00              │
│                                                              │
│ 4. ❌ ERROR: Falla al exportar a BigQuery                   │
│    export_to_bigquery(...)  # ← Excepción                   │
│                                                              │
│ 5. Estado NO se actualiza                                   │
│    → last_processed_hour sigue siendo 2024-01-15 11:00:00   │
│    → Guardado en Prefect Cloud (sin cambios)                │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Ejecución 4: Recuperación automática                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ 1. Cargar estado                                            │
│    last_hour = await state_manager.get_last_processed_hour()│
│    → Carga desde Prefect Cloud: 2024-01-15 11:00:00 ✅     │
│    → (El estado anterior se mantiene, no se perdió)         │
│                                                              │
│ 2. Calcular rango                                           │
│    next_hour = 2024-01-15 12:00:00                          │
│                                                              │
│ 3. Procesar datos (REPROCESA LA MISMA HORA)                 │
│    df = await fetch_incremental_data(...)                   │
│    → Obtiene datos de 2024-01-15 12:00 a 13:00 (nuevamente) │
│                                                              │
│ 4. Exportar (esta vez sin errores)                          │
│    export_to_bigquery(...)  # ✅ Éxito                      │
│                                                              │
│ 5. Guardar estado                                           │
│    await state_manager.set_last_processed_hour(             │
│        datetime(2024, 1, 15, 12, 0, 0)                      │
│    )                                                         │
│    → Actualiza en Prefect Cloud ✅                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Componentes Clave

### 1. PipelineStateBlock (src/utils/blocks.py)

```python
class PipelineStateBlock(Block):
    """
    Objeto Prefect que se almacena en Prefect Cloud/Server
    
    Campos:
    - last_processed_hour: Última hora procesada (UTC)
    - pipeline_version: Versión del pipeline
    - metadata: Información adicional
    
    Métodos:
    - save(): Persiste en Prefect Cloud
    - load(): Carga desde Prefect Cloud
    - get_last_processed_hour(): Obtiene la última hora
    - set_last_processed_hour(): Actualiza la última hora
    - get_next_hour_to_process(): Calcula siguiente hora
    """
```

### 2. PipelineState (src/utils/state.py)

```python
class PipelineState:
    """
    Wrapper async que facilita el uso de PipelineStateBlock
    
    Responsabilidades:
    - Cargar/crear block desde Prefect Cloud
    - Manejar errores de conexión
    - Proporcionar interfaz async
    - Gestionar ciclo de vida del block
    """
```

### 3. Integración en Pipeline (flows/pipeline_horario.py)

```python
@flow(name="pipeline-horario")
async def pipeline_horario():
    """
    Flow principal que usa PipelineState para incremental loading
    
    Flujo:
    1. Obtener estado desde Prefect Cloud
    2. Procesar datos nuevos
    3. Guardar estado en Prefect Cloud
    """
```

## Ventajas de esta Arquitectura

| Aspecto | Ventaja |
|--------|---------|
| **Persistencia** | Estado guardado en Prefect Cloud, no en memoria |
| **Recuperación** | Ante fallos, se recupera el estado anterior |
| **Escalabilidad** | Múltiples instancias pueden acceder al mismo estado |
| **Auditoría** | Prefect Cloud mantiene historial de cambios |
| **Seguridad** | Credenciales encriptadas en Prefect Cloud |
| **Sincronización** | Automática entre ejecuciones |
| **Mantenibilidad** | Código limpio y separado por responsabilidades |

## Resumen

- **PipelineStateBlock** = Objeto que se almacena en Prefect Cloud
- **PipelineState** = Wrapper async para facilitar su uso
- **pipeline_horario** = Flow que usa PipelineState para incremental loading
- **Persistencia** = En Prefect Cloud, no en memoria local
- **Recuperación** = Automática ante fallos
- **Escalabilidad** = Funciona con múltiples instancias
