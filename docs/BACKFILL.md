# Backfill Script: Guía de Uso

## ¿Qué es el Backfill?

El script `backfill.py` permite **reprocesar datos históricos** que el pipeline no procesó en su momento. Esto es útil cuando:

- El pipeline estuvo caído por varias horas
- Hubo errores en ejecuciones anteriores
- Necesitas reprocesar un rango de fechas específico
- Quieres recuperar datos faltantes automáticamente

---

## Modos de Uso

### 1. Backfill Automático (--missing)

Detecta automáticamente las horas faltantes basándose en el estado del pipeline almacenado en Prefect Blocks.

```bash
python backfill.py --missing
```

**Flujo:**
```
┌─────────────────────────────────────────────────────────────┐
│ 1. Cargar estado desde Prefect Cloud                        │
│    last_processed_hour = 2024-01-15 10:00:00               │
├─────────────────────────────────────────────────────────────┤
│ 2. Calcular horas faltantes                                 │
│    current_time = 2024-01-15 15:30:00                      │
│    missing = [11:00, 12:00, 13:00, 14:00]                  │
├─────────────────────────────────────────────────────────────┤
│ 3. Backfill cada hora faltante                              │
│    - Obtener datos de InfluxDB                              │
│    - Transformar a formato long                             │
│    - Agregar al resultado                                   │
├─────────────────────────────────────────────────────────────┤
│ 4. Exportar a Excel                                         │
│    output/backfill_20240115153045.xlsx                     │
├─────────────────────────────────────────────────────────────┤
│ 5. Actualizar estado en Prefect Cloud                       │
│    last_processed_hour = 2024-01-15 14:00:00               │
└─────────────────────────────────────────────────────────────┘
```

**Ejemplo de salida:**
```
Checking for missing hours (block: pipeline-state)...
Found 4 missing hours:
  - 2024-01-15 11:00:00+00:00
  - 2024-01-15 12:00:00+00:00
  - 2024-01-15 13:00:00+00:00
  - 2024-01-15 14:00:00+00:00

Backfilling co...
  Found 4 hours with data
    2024-01-15 11:00:00+00:00: 125 rows
    2024-01-15 12:00:00+00:00: 130 rows
    2024-01-15 13:00:00+00:00: 128 rows
    2024-01-15 14:00:00+00:00: 122 rows

Total backfilled rows: 505
Exported to: output/backfill_20240115153045.xlsx

Updating pipeline state...
State updated: last_processed_hour=2024-01-15 14:00:00+00:00
```

---

### 2. Backfill Manual (--start / --end)

Especifica un rango de fechas exacto para reprocesar.

```bash
python backfill.py --start 2024-01-10T08:00:00 --end 2024-01-10T18:00:00
```

**Parámetros:**
- `--start`: Hora de inicio (inclusive) en formato ISO
- `--end`: Hora de fin (exclusive) en formato ISO

**Ejemplo:**
```bash
# Backfill de 8:00 a 18:00 del 10 de enero
python backfill.py --start 2024-01-10T08:00:00 --end 2024-01-10T18:00:00
```

**Salida:**
```
Starting backfill from 2024-01-10 08:00:00+00:00 to 2024-01-10 18:00:00+00:00
Pollutants: ['co', 'nox', 'pm10', 'so2', 'o3', 'meteo']

Backfilling co...
  Found 10 hours with data
    2024-01-10 08:00:00+00:00: 125 rows
    2024-01-10 09:00:00+00:00: 130 rows
    ...

Total backfilled rows: 1250
Exported to: output/backfill_20240115160000.xlsx
```

---

### 3. Backfill de Contaminantes Específicos

Puedes especificar qué contaminantes procesar.

```bash
# Solo CO y NOx
python backfill.py --start 2024-01-10T08:00:00 --end 2024-01-10T18:00:00 --pollutants co nox

# Solo PM10
python backfill.py --missing --pollutants pm10
```

**Contaminantes disponibles:**
- `co` - Monóxido de carbono
- `nox` - Óxidos de nitrógeno
- `pm10` - Material particulado
- `so2` - Dióxido de azufre
- `o3` - Ozono
- `meteo` - Datos meteorológicos

---

### 4. Sin Exportar a Excel

Si solo quieres procesar sin generar archivo Excel:

```bash
python backfill.py --missing --no-export
```

---

### 5. Usar un Block de Estado Diferente

Si tienes múltiples pipelines con diferentes blocks de estado:

```bash
python backfill.py --missing --state-block my-custom-state
```

---

## Diagrama de Flujo

```
┌─────────────────────────────────────────────────────────────────────┐
│                         backfill.py                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Modo: --missing                                              │   │
│  │                                                               │   │
│  │  1. Cargar estado desde Prefect Cloud                        │   │
│  │     state_manager = get_pipeline_state("pipeline-state")     │   │
│  │     last_processed = await state_manager.get_last_processed_hour()
│  │                                                               │   │
│  │  2. Calcular horas faltantes                                 │   │
│  │     missing = find_missing_hours(last_processed, now)        │   │
│  │                                                               │   │
│  │  3. Ejecutar backfill                                        │   │
│  │     await run_backfill(missing[0], current_hour)             │   │
│  │                                                               │   │
│  │  4. Actualizar estado                                        │   │
│  │     await state_manager.set_last_processed_hour(...)         │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Modo: --start / --end                                        │   │
│  │                                                               │   │
│  │  1. Parsear fechas                                           │   │
│  │     start = parse_datetime(args.start)                       │   │
│  │     end = parse_datetime(args.end)                           │   │
│  │                                                               │   │
│  │  2. Ejecutar backfill                                        │   │
│  │     await run_backfill(start, end, pollutants)               │   │
│  │                                                               │   │
│  │  (No actualiza estado automáticamente)                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       run_backfill()                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  async with influxdb_client() as client:                            │
│                                                                      │
│    for pollutant in pollutants:                                     │
│      ┌─────────────────────────────────────────────────────────┐   │
│      │ 1. Obtener datos de cada hora                           │   │
│      │    backfilled = await backfill_hours(                   │   │
│      │        client, pollutant, start_hour, end_hour          │   │
│      │    )                                                     │   │
│      │    → Retorna: [(hour1, df1), (hour2, df2), ...]         │   │
│      └─────────────────────────────────────────────────────────┘   │
│                                                                      │
│      ┌─────────────────────────────────────────────────────────┐   │
│      │ 2. Transformar cada hora                                │   │
│      │    for hour, df in backfilled:                          │   │
│      │      transformed = aggregate_to_long_format(df, ...)    │   │
│      │      all_data.append(transformed)                       │   │
│      └─────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ 3. Combinar y exportar                                      │   │
│  │    combined = pl.concat(all_data)                           │   │
│  │    export_to_excel(combined, output_path)                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Casos de Uso Comunes

### Caso 1: Pipeline caído por 5 horas

```bash
# Detecta automáticamente las 5 horas faltantes y las procesa
python backfill.py --missing
```

### Caso 2: Reprocesar un día completo

```bash
# Reprocesa todas las horas del 15 de enero
python backfill.py --start 2024-01-15T00:00:00 --end 2024-01-16T00:00:00
```

### Caso 3: Reprocesar solo CO de una semana

```bash
# Solo CO del 10 al 17 de enero
python backfill.py \
  --start 2024-01-10T00:00:00 \
  --end 2024-01-17T00:00:00 \
  --pollutants co
```

### Caso 4: Verificar horas faltantes sin procesar

```bash
# Ver qué horas faltan (sin procesar ni exportar)
python backfill.py --missing --no-export
# Luego cancela con Ctrl+C después de ver la lista
```

---

## Argumentos de Línea de Comandos

| Argumento | Descripción | Ejemplo |
|-----------|-------------|---------|
| `--start` | Hora de inicio (ISO format) | `--start 2024-01-10T08:00:00` |
| `--end` | Hora de fin (ISO format) | `--end 2024-01-10T18:00:00` |
| `--pollutants` | Contaminantes a procesar | `--pollutants co nox pm10` |
| `--missing` | Detectar horas faltantes automáticamente | `--missing` |
| `--state-block` | Nombre del block de estado | `--state-block my-state` |
| `--no-export` | No exportar a Excel | `--no-export` |

---

## Archivos de Salida

Los archivos se guardan en `output/` con el formato:

```
output/backfill_YYYYMMDDHHMMSS.xlsx
```

Ejemplo: `output/backfill_20240115160045.xlsx`

---

## Diferencias con el Pipeline Principal

| Aspecto | Pipeline (`pipeline_horario.py`) | Backfill (`backfill.py`) |
|---------|----------------------------------|--------------------------|
| **Ejecución** | Automática (scheduler) | Manual (CLI) |
| **Rango** | Última hora no procesada | Rango especificado o faltantes |
| **Destinos** | Excel, BigQuery, MotherDuck | Solo Excel |
| **Estado** | Siempre actualiza | Solo con `--missing` |
| **Uso** | Procesamiento continuo | Recuperación de datos |

---

## Notas Importantes

1. **El modo `--missing` actualiza el estado** en Prefect Cloud después de un backfill exitoso.

2. **El modo manual (`--start`/`--end`) NO actualiza el estado** automáticamente. Esto es intencional para evitar conflictos.

3. **Los datos se exportan solo a Excel**, no a BigQuery ni MotherDuck. Si necesitas cargar a otros destinos, usa el pipeline principal o modifica el script.

4. **El script es async** y usa las mismas utilidades que el pipeline principal, garantizando consistencia en el procesamiento.
