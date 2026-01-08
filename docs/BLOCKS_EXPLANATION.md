# Explicación: PipelineStateBlock y Prefect Blocks

## ¿Qué es un Prefect Block?

Un **Prefect Block** es un objeto persistente almacenado en **Prefect Cloud/Server** (no en memoria local). Es como una base de datos pequeña para guardar configuración, credenciales y estado.

```
┌─────────────────────────────────────────────────────────────┐
│                    PREFECT CLOUD/SERVER                     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Blocks Storage (Base de datos persistente)          │  │
│  │                                                       │  │
│  │  - influxdb-token (Secret)                           │  │
│  │  - motherduck-token (Secret)                         │  │
│  │  - gcp-credentials (GcpCredentials)                  │  │
│  │  - pipeline-state (PipelineStateBlock) ← NUESTRO    │  │
│  │                                                       │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## ¿Dónde se guarda el estado?

### ❌ NO en memoria local
```python
# Esto NO es persistente:
state = PipelineStateBlock()
state.last_processed_hour = datetime(2024, 1, 1, 10, 0, 0)
# Si el proceso muere, se pierde todo
```

### ✅ SÍ en Prefect Blocks (persistente)
```python
# Esto SÍ es persistente:
state = await PipelineStateBlock.load("pipeline-state")  # Carga desde Prefect Cloud
state.last_processed_hour = datetime(2024, 1, 1, 10, 0, 0)
await state.save("pipeline-state", overwrite=True)  # Guarda en Prefect Cloud
```

## Flujo de Datos: Cómo Funciona

### 1️⃣ Primera Ejecución (Sin Estado Previo)

```
┌─────────────────────────────────────────────────────────────┐
│ Pipeline Execution #1                                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Cargar estado:                                          │
│     state = await PipelineStateBlock.load("pipeline-state") │
│     → No existe → last_processed_hour = None                │
│                                                              │
│  2. Calcular rango a procesar:                              │
│     next_hour = state.get_next_hour_to_process()            │
│     → Como es None, retorna: previous_hour_start()          │
│     → Ej: 2024-01-01 09:00:00 UTC                           │
│                                                              │
│  3. Procesar datos de InfluxDB:                             │
│     fetch_incremental_data(client, "co", None)              │
│     → Obtiene datos de la hora anterior                     │
│                                                              │
│  4. Guardar estado:                                         │
│     state.last_processed_hour = 2024-01-01 09:00:00         │
│     await state.save("pipeline-state", overwrite=True)      │
│     → Guarda en Prefect Cloud ✅                            │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 2️⃣ Segunda Ejecución (Con Estado Previo)

```
┌─────────────────────────────────────────────────────────────┐
│ Pipeline Execution #2 (1 hora después)                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Cargar estado:                                          │
│     state = await PipelineStateBlock.load("pipeline-state") │
│     → Existe en Prefect Cloud                               │
│     → last_processed_hour = 2024-01-01 09:00:00             │
│                                                              │
│  2. Calcular rango a procesar:                              │
│     next_hour = state.get_next_hour_to_process()            │
│     → last_processed_hour + 1 hora                          │
│     → Ej: 2024-01-01 10:00:00 UTC                           │
│                                                              │
│  3. Procesar datos de InfluxDB:                             │
│     fetch_incremental_data(client, "co", 2024-01-01 09:00)  │
│     → Obtiene SOLO datos nuevos desde las 10:00             │
│     → Evita reprocesar datos antiguos ✅                    │
│                                                              │
│  4. Guardar estado:                                         │
│     state.last_processed_hour = 2024-01-01 10:00:00         │
│     await state.save("pipeline-state", overwrite=True)      │
│     → Actualiza en Prefect Cloud ✅                         │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Arquitectura: Memory vs Persistent Storage

```
┌──────────────────────────────────────────────────────────────────┐
│                    Tu Aplicación Python                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Memory (RAM) - Temporal                                 │    │
│  │                                                          │    │
│  │  state = PipelineStateBlock()                           │    │
│  │  state.last_processed_hour = datetime(...)  ← En RAM   │    │
│  │                                                          │    │
│  │  ⚠️ Se pierde si el proceso muere                       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Prefect Cloud/Server - Persistente                      │    │
│  │                                                          │    │
│  │  await state.save("pipeline-state", overwrite=True)     │    │
│  │  → Guarda en base de datos de Prefect ✅               │    │
│  │                                                          │    │
│  │  await state.load("pipeline-state")                     │    │
│  │  → Carga desde base de datos de Prefect ✅             │    │
│  │                                                          │    │
│  │  ✅ Persiste entre ejecuciones                          │    │
│  │  ✅ Accesible desde cualquier máquina                   │    │
│  │  ✅ Recuperable si el proceso falla                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

## Código: Cómo se Usa en el Pipeline

### En `src/utils/state.py` (Wrapper async)

```python
class PipelineState:
    async def _load_or_create_block(self) -> PipelineStateBlock:
        """Carga o crea el block en Prefect Cloud"""
        try:
            # Intenta cargar desde Prefect Cloud
            self._block = await PipelineStateBlock.load(self._block_name)
        except ValueError:
            # No existe, crea uno nuevo
            self._block = PipelineStateBlock()
            await self._block.save(self._block_name, overwrite=False)
            # Recarga para obtener la referencia correcta
            self._block = await PipelineStateBlock.load(self._block_name)

    async def set_last_processed_hour(self, hour: datetime) -> None:
        """Guarda la última hora procesada en Prefect Cloud"""
        block = await self._load_or_create_block()
        hour = ensure_utc(hour)
        
        # Actualiza en memoria
        block.last_processed_hour = hour
        
        # Guarda en Prefect Cloud
        await block.save(self._block_name, overwrite=True)
```

### En `flows/pipeline_horario.py` (Uso en el pipeline)

```python
@flow(name="pipeline-horario")
async def pipeline_horario(...):
    # Obtener estado del pipeline
    state_manager = get_pipeline_state("pipeline-state")
    
    # Cargar última hora procesada desde Prefect Cloud
    last_processed_hour = await state_manager.get_last_processed_hour()
    # → Si es la primera vez: None
    # → Si no: datetime(2024, 1, 1, 10, 0, 0)
    
    # Procesar datos
    df = await fetch_incremental_data(client, "co", last_processed_hour)
    
    # Guardar estado en Prefect Cloud
    processed_hour = get_previous_hour_start()
    await state_manager.set_last_processed_hour(processed_hour)
    # → Guarda en Prefect Cloud para la próxima ejecución
```

## Ciclo de Vida Completo

```
Ejecución 1:
├─ Cargar: last_processed_hour = None (no existe)
├─ Procesar: Datos de 2024-01-01 09:00
├─ Guardar: last_processed_hour = 2024-01-01 09:00 → Prefect Cloud ✅
└─ Fin

Ejecución 2 (1 hora después):
├─ Cargar: last_processed_hour = 2024-01-01 09:00 ← Prefect Cloud ✅
├─ Procesar: Datos de 2024-01-01 10:00 (solo nuevos)
├─ Guardar: last_processed_hour = 2024-01-01 10:00 → Prefect Cloud ✅
└─ Fin

Ejecución 3 (1 hora después):
├─ Cargar: last_processed_hour = 2024-01-01 10:00 ← Prefect Cloud ✅
├─ Procesar: Datos de 2024-01-01 11:00 (solo nuevos)
├─ Guardar: last_processed_hour = 2024-01-01 11:00 → Prefect Cloud ✅
└─ Fin
```

## Ventajas de Usar Prefect Blocks

| Aspecto | Sin Blocks | Con Blocks |
|--------|-----------|-----------|
| **Almacenamiento** | Archivo local (.json) | Prefect Cloud/Server |
| **Persistencia** | ⚠️ Depende del filesystem | ✅ Garantizada |
| **Acceso** | Solo máquina local | ✅ Desde cualquier máquina |
| **Recuperación** | ⚠️ Manual | ✅ Automática |
| **Seguridad** | ⚠️ Credenciales en texto | ✅ Encriptadas |
| **Escalabilidad** | ⚠️ Problemas con múltiples instancias | ✅ Sincronizado |

## Resumen

- **PipelineStateBlock** es un objeto que hereda de `prefect.blocks.abstract.Block`
- **NO guarda en memoria local** - guarda en **Prefect Cloud/Server**
- **Cada `save()`** persiste los datos en la base de datos de Prefect
- **Cada `load()`** recupera los datos desde Prefect Cloud
- **Permite incremental loading** - el pipeline solo procesa datos nuevos
- **Es thread-safe y distribuido** - múltiples instancias pueden acceder al mismo estado
