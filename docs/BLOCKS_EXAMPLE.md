# Ejemplo Práctico: PipelineStateBlock en Acción

## Escenario: Pipeline Horario de Contaminantes

### Configuración Inicial

```bash
# 1. Crear el block en Prefect Cloud
python scripts/create_blocks.py

# Esto crea un block llamado "pipeline-state" en Prefect Cloud
# Inicialmente: last_processed_hour = None
```

### Ejecución 1: Primera Vez (Sin Estado Previo)

**Hora actual: 2024-01-15 11:30 UTC**

```python
# flows/pipeline_horario.py
async def pipeline_horario():
    state_manager = get_pipeline_state("pipeline-state")
    
    # 1. Cargar estado desde Prefect Cloud
    last_processed = await state_manager.get_last_processed_hour()
    print(f"Last processed: {last_processed}")  # Output: Last processed: None
    
    # 2. Calcular siguiente hora a procesar
    # Como es None, retorna: 2024-01-15 10:00 UTC (hora anterior)
    next_hour = await state_manager.get_next_hour_to_process()
    print(f"Next hour to process: {next_hour}")  # Output: 2024-01-15 10:00:00+00:00
    
    # 3. Procesar datos de InfluxDB
    async with influxdb_client() as client:
        df = await fetch_incremental_data(client, "co", None)
        # Obtiene datos de 2024-01-15 10:00 a 2024-01-15 11:00
        print(f"Fetched {len(df)} rows")  # Output: Fetched 1250 rows
    
    # 4. Transformar y exportar
    transformed = aggregate_to_long_format(df, ...)
    export_to_excel(transformed, "output/contaminantes.xlsx")
    
    # 5. Guardar estado en Prefect Cloud
    processed_hour = get_previous_hour_start()  # 2024-01-15 10:00 UTC
    await state_manager.set_last_processed_hour(processed_hour)
    print(f"State saved: {processed_hour}")  # Output: State saved: 2024-01-15 10:00:00+00:00
```

**Resultado en Prefect Cloud:**
```
Block: pipeline-state
├─ last_processed_hour: 2024-01-15 10:00:00+00:00 ✅
├─ pipeline_version: v0.8.6
└─ metadata:
   ├─ last_execution: 2024-01-15T11:30:45.123456
   └─ pipeline_version: v0.8.6
```

---

### Ejecución 2: Una Hora Después

**Hora actual: 2024-01-15 12:30 UTC**

```python
async def pipeline_horario():
    state_manager = get_pipeline_state("pipeline-state")
    
    # 1. Cargar estado desde Prefect Cloud
    last_processed = await state_manager.get_last_processed_hour()
    print(f"Last processed: {last_processed}")  
    # Output: Last processed: 2024-01-15 10:00:00+00:00 ✅ (cargado desde Prefect Cloud)
    
    # 2. Calcular siguiente hora a procesar
    # Como existe, retorna: last_processed + 1 hora = 2024-01-15 11:00 UTC
    next_hour = await state_manager.get_next_hour_to_process()
    print(f"Next hour to process: {next_hour}")  # Output: 2024-01-15 11:00:00+00:00
    
    # 3. Procesar datos de InfluxDB (SOLO NUEVOS)
    async with influxdb_client() as client:
        df = await fetch_incremental_data(client, "co", last_processed)
        # Obtiene datos de 2024-01-15 11:00 a 2024-01-15 12:00 (solo nuevos)
        print(f"Fetched {len(df)} rows")  # Output: Fetched 1200 rows (nuevos)
    
    # 4. Transformar y exportar
    transformed = aggregate_to_long_format(df, ...)
    export_to_excel(transformed, "output/contaminantes.xlsx")
    
    # 5. Guardar estado en Prefect Cloud
    processed_hour = get_previous_hour_start()  # 2024-01-15 11:00 UTC
    await state_manager.set_last_processed_hour(processed_hour)
    print(f"State saved: {processed_hour}")  # Output: State saved: 2024-01-15 11:00:00+00:00
```

**Resultado en Prefect Cloud:**
```
Block: pipeline-state
├─ last_processed_hour: 2024-01-15 11:00:00+00:00 ✅ (ACTUALIZADO)
├─ pipeline_version: v0.8.6
└─ metadata:
   ├─ last_execution: 2024-01-15T12:30:45.123456
   └─ pipeline_version: v0.8.6
```

---

### Ejecución 3: Fallo y Recuperación

**Hora actual: 2024-01-15 13:30 UTC**
**Problema: El proceso falla a mitad de la ejecución**

```python
async def pipeline_horario():
    state_manager = get_pipeline_state("pipeline-state")
    
    # 1. Cargar estado desde Prefect Cloud
    last_processed = await state_manager.get_last_processed_hour()
    print(f"Last processed: {last_processed}")  
    # Output: Last processed: 2024-01-15 11:00:00+00:00 ✅
    # (El estado anterior se mantiene porque NO fue actualizado)
    
    # 2. Calcular siguiente hora a procesar
    next_hour = await state_manager.get_next_hour_to_process()
    print(f"Next hour to process: {next_hour}")  # Output: 2024-01-15 12:00:00+00:00
    
    # 3. Procesar datos de InfluxDB
    async with influxdb_client() as client:
        df = await fetch_incremental_data(client, "co", last_processed)
        # Obtiene datos de 2024-01-15 12:00 a 2024-01-15 13:00
        print(f"Fetched {len(df)} rows")  # Output: Fetched 1150 rows
    
    # 4. Transformar
    transformed = aggregate_to_long_format(df, ...)
    
    # ❌ ERROR: Falla al exportar a BigQuery
    try:
        export_to_bigquery(project_id, dataset_id, table_id, transformed)
    except Exception as e:
        print(f"ERROR: {e}")
        # El estado NO fue guardado, así que la próxima ejecución
        # volverá a procesar la misma hora (2024-01-15 12:00)
        raise
```

**Resultado en Prefect Cloud:**
```
Block: pipeline-state
├─ last_processed_hour: 2024-01-15 11:00:00+00:00 ✅ (SIN CAMBIOS)
├─ pipeline_version: v0.8.6
└─ metadata:
   ├─ last_execution: 2024-01-15T12:30:45.123456 (sin actualizar)
   └─ pipeline_version: v0.8.6
```

**Ejecución 4: Recuperación Automática**

```python
# El pipeline se reinicia automáticamente (por Prefect scheduler)
async def pipeline_horario():
    state_manager = get_pipeline_state("pipeline-state")
    
    # 1. Cargar estado desde Prefect Cloud
    last_processed = await state_manager.get_last_processed_hour()
    print(f"Last processed: {last_processed}")  
    # Output: Last processed: 2024-01-15 11:00:00+00:00 ✅
    # (Recupera el estado anterior, no se perdió nada)
    
    # 2. Calcular siguiente hora a procesar
    next_hour = await state_manager.get_next_hour_to_process()
    print(f"Next hour to process: {next_hour}")  # Output: 2024-01-15 12:00:00+00:00
    
    # 3. Procesar datos de InfluxDB (REPROCESA LA MISMA HORA)
    async with influxdb_client() as client:
        df = await fetch_incremental_data(client, "co", last_processed)
        # Obtiene datos de 2024-01-15 12:00 a 2024-01-15 13:00 (nuevamente)
        print(f"Fetched {len(df)} rows")  # Output: Fetched 1150 rows
    
    # 4. Transformar y exportar (esta vez sin errores)
    transformed = aggregate_to_long_format(df, ...)
    export_to_bigquery(project_id, dataset_id, table_id, transformed)  # ✅ Éxito
    
    # 5. Guardar estado en Prefect Cloud
    processed_hour = get_previous_hour_start()  # 2024-01-15 12:00 UTC
    await state_manager.set_last_processed_hour(processed_hour)
    print(f"State saved: {processed_hour}")  # Output: State saved: 2024-01-15 12:00:00+00:00
```

**Resultado en Prefect Cloud:**
```
Block: pipeline-state
├─ last_processed_hour: 2024-01-15 12:00:00+00:00 ✅ (ACTUALIZADO)
├─ pipeline_version: v0.8.6
└─ metadata:
   ├─ last_execution: 2024-01-15T13:45:30.654321
   └─ pipeline_version: v0.8.6
```

---

## Comparación: Con vs Sin Blocks

### ❌ SIN Prefect Blocks (Archivo Local)

```python
# Guardar estado en archivo local
import json

def save_state(last_hour):
    with open("state.json", "w") as f:
        json.dump({"last_processed_hour": last_hour.isoformat()}, f)

def load_state():
    try:
        with open("state.json", "r") as f:
            data = json.load(f)
            return datetime.fromisoformat(data["last_processed_hour"])
    except FileNotFoundError:
        return None

# Problemas:
# ❌ Si el archivo se corrompe, se pierde el estado
# ❌ No funciona en múltiples máquinas (cada una tiene su archivo)
# ❌ No hay versionado ni auditoría
# ❌ Credenciales en texto plano
# ❌ Difícil de sincronizar en Kubernetes/Docker
```

### ✅ CON Prefect Blocks (Prefect Cloud)

```python
# Guardar estado en Prefect Cloud
state_manager = get_pipeline_state("pipeline-state")
await state_manager.set_last_processed_hour(processed_hour)

# Ventajas:
# ✅ Almacenamiento centralizado y seguro
# ✅ Funciona en múltiples máquinas
# ✅ Auditoría y versionado automático
# ✅ Credenciales encriptadas
# ✅ Sincronización automática
# ✅ Recuperación ante fallos
# ✅ Interfaz web en Prefect Cloud
```

---

## Flujo de Datos Detallado

```
┌─────────────────────────────────────────────────────────────────┐
│                    Prefect Cloud/Server                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Blocks Storage (Base de datos)                         │    │
│  │                                                         │    │
│  │ pipeline-state:                                        │    │
│  │ {                                                       │    │
│  │   "last_processed_hour": "2024-01-15T11:00:00+00:00", │    │
│  │   "pipeline_version": "v0.8.6",                        │    │
│  │   "metadata": {                                        │    │
│  │     "last_execution": "2024-01-15T12:30:45.123456"    │    │
│  │   }                                                     │    │
│  │ }                                                       │    │
│  └────────────────────────────────────────────────────────┘    │
│                          ▲                                       │
│                          │                                       │
│                    await save()                                  │
│                    await load()                                  │
│                          │                                       │
└──────────────────────────┼───────────────────────────────────────┘
                           │
                           │
┌──────────────────────────┼───────────────────────────────────────┐
│                          │                                        │
│                    Tu Aplicación Python                          │
│                                                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │ PipelineState (Wrapper async)                          │     │
│  │                                                         │     │
│  │ async def set_last_processed_hour(hour):               │     │
│  │   block = await self._load_or_create_block()           │     │
│  │   block.last_processed_hour = hour                     │     │
│  │   await block.save("pipeline-state", overwrite=True)   │     │
│  │   # ↑ Envía a Prefect Cloud                            │     │
│  │                                                         │     │
│  │ async def get_last_processed_hour():                   │     │
│  │   block = await self._load_or_create_block()           │     │
│  │   return block.get_last_processed_hour()               │     │
│  │   # ↑ Carga desde Prefect Cloud                        │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │ PipelineStateBlock (Objeto Prefect)                    │     │
│  │                                                         │     │
│  │ class PipelineStateBlock(Block):                       │     │
│  │   last_processed_hour: Optional[datetime] = None       │     │
│  │   pipeline_version: str = "v1.0.0"                     │     │
│  │   metadata: Dict[str, Any] = {}                        │     │
│  │                                                         │     │
│  │   def _save_block(self):                               │     │
│  │     self.save(name=self._block_document_name, ...)     │     │
│  │     # ↑ Guarda en Prefect Cloud                        │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## Resumen de Conceptos Clave

| Concepto | Explicación |
|----------|-------------|
| **PipelineStateBlock** | Clase que hereda de `prefect.blocks.abstract.Block` |
| **last_processed_hour** | Campo que almacena la última hora procesada |
| **save()** | Persiste el estado en Prefect Cloud |
| **load()** | Carga el estado desde Prefect Cloud |
| **Incremental Loading** | Solo procesa datos nuevos desde `last_processed_hour` |
| **Fault Tolerance** | Si falla, el estado anterior se mantiene y se reprocesa |
| **Distributed** | Múltiples instancias pueden acceder al mismo estado |
