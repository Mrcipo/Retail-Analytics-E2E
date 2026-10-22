# retail-analytics-e2e

Pipeline E2E de analítica retail: ingesta → DuckDB → dbt → Airflow.

## Stack

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.12 |
| Base de datos | DuckDB 1.1.0 (`data/retail_analytics.duckdb`) |
| Transformaciones | dbt-core 1.8.7 + dbt-duckdb 1.8.3 |
| API de ingesta | FastAPI 0.115.0 + Uvicorn |
| Procesamiento | Polars 1.7.1, Pandas 2.2.2, PyArrow 17.0.0 |
| Orquestación | Apache Airflow (planificado, `airflow/dags/` aún no existe) |
| Entorno virtual | `venv/` — activar con `venv\Scripts\activate` |

## Estructura del proyecto

```
extraction/
  afip_ingest.py          # Padrón AFIP (CSV latin-1, carga completa)
  openfoodfacts_ingest.py # Open Food Facts (Parquet Hugging Face, incremental)
  utils/
    logger.py             # get_logger(name) → logging a consola + data/control/logs/<name>.log
    db.py                 # get_connection(), ensure_control_schema()
api/
  main.py                 # FastAPI app
  routers/webhooks.py     # POST /webhooks/tiendanube (bronze layer JSON)
dbt/retail_analytics/
  profiles.yml            # target: dev → DuckDB en $DUCKDB_PATH (default ../../data/retail_analytics.duckdb)
  dbt_project.yml
  models/                 # stg_ / int_ / fact_ / dim_ / mart_
  snapshots/              # SCD Type 2
scripts/
  create_sample_db.py     # raw.* sintético para probar dbt sin descargas
tests/                    # pytest (API de webhooks, landing de órdenes)
airflow/dags/             # DAGs de orquestación (pendiente)
data/
  retail_analytics.duckdb
  raw/                    # datos crudos (no commitear)
    afip/
    tiendanube/           # particionado por topic/fecha en JSON
    off/                  # Parquet de Open Food Facts (descarga local)
  control/logs/           # logs por módulo
```

## Reglas de código

- **Logging**: siempre `logging` vía `get_logger(name)`. Nunca `print()`.
- **Conexión DuckDB**: usar siempre `get_connection()` (context manager) y `ensure_control_schema()`.
- **Rutas**: `pathlib.Path` relativo a la raíz del repo. Nunca rutas absolutas hardcodeadas.
- **Errores**: `try/except` explícito con `logger.error()`. El `except` genérico (`Exception`) solo al nivel más alto del script.
- **Schema de fuentes externas**: antes de asumir tipos, inspeccionarlos con `pl.scan_parquet(...).collect_schema()`.

## Esquemas DuckDB

| Esquema | Contenido |
|---|---|
| `raw.*` | Datos crudos tal como llegan de la fuente |
| `control.ingestion_watermarks` | Watermarks y metadata de cada corrida de ingesta |
| `staging.*` | Modelos dbt `stg_` |
| `marts.*` | Modelos dbt `fact_`, `dim_`, `mart_` |

## Fuentes de datos

### AFIP (`extraction/afip_ingest.py`)
- CSV latin-1, separador `;`, carga completa (no incremental).
- Archivo manual en `data/raw/afip/padron_afip.csv`.
- Destino: `raw.afip_padron` (reemplaza en cada corrida).

### Tiendanube (`api/routers/webhooks.py`)
- Webhook POST en `/webhooks/tiendanube`.
- Topics soportados: `order/created`, `order/updated`, `product/created`, `product/updated`, `customer/created`.
- Persiste JSON en `data/raw/tiendanube/<topic>/<fecha>/`.

### Open Food Facts (`extraction/openfoodfacts_ingest.py`)
- Parquet descargado desde Hugging Face a `data/raw/off/` y escaneado local (Polars lazy).
- Ingesta incremental por `last_modified_t` (Int64).
- **Schema actual del Parquet** (verificado):
  - `product_name` → `List(Struct{lang, text})`. Extraer con `.list.first().struct.field("text")`.
  - `countries` **no existe** → usar `countries_tags` (`List(String)`), aplanar con `.list.join(", ")`.
  - `last_modified_t` → `Int64` (watermark).
- Destino: `raw.off_products` (upsert por `code`).

## dbt

```bash
# Siempre desde la raíz del repo
cd dbt/retail_analytics
dbt run --select <modelo>
dbt test --select <modelo>
```

O con `DBT_PROFILES_DIR` explícito si se corre desde otro directorio:
```bash
DBT_PROFILES_DIR=dbt/retail_analytics dbt run --project-dir dbt/retail_analytics
```

### Convenciones de modelos

- `stg_<fuente>__<entidad>`: staging, 1:1 con la fuente, solo renombres y casts. Selecciona desde `source()`.
- `int_<descripcion>`: lógica de negocio, joins, agregaciones intermedias.
- `fact_<entidad>`, `dim_<entidad>`, `mart_<nombre>`: capa de consumo.
- Estructura obligatoria: CTEs con nombres descriptivos + un único `SELECT` final. Sin subqueries anidadas.
- Tests mínimos por modelo: `unique` + `not_null` en PK, `relationships` en FK.
- Toda columna documentada en español en el `schema.yml` correspondiente.
- SCD Type 2 con snapshots dbt (`strategy: timestamp` o `check`).

## Comandos frecuentes

```bash
# Activar entorno virtual (Windows)
venv\Scripts\activate

# Levantar API de ingesta
uvicorn api.main:app --reload

# Correr ingesta AFIP
python extraction/afip_ingest.py

# Correr ingesta Open Food Facts
python extraction/openfoodfacts_ingest.py

# Tests unitarios
pytest

# Base de ejemplo + dbt build (lo mismo que corre el CI)
DUCKDB_PATH=$PWD/data/sample.duckdb python -m scripts.create_sample_db
cd dbt/retail_analytics && DUCKDB_PATH=../../data/sample.duckdb DBT_PROFILES_DIR=. dbt build

# Validar DAG de Airflow (sin import errors)
python airflow/dags/<dag>.py
```

## Variables de entorno

Crear `.env` en la raíz (no commitear):
```
DUCKDB_PATH=./data/retail_analytics.duckdb
```

## Estado del proyecto

- Fase 1 (setup): completada.
- Fase 2 (ingesta): en curso — AFIP y Tiendanube funcionando; Open Food Facts activo.
- Fase 3 (dbt): staging hecho (4 modelos + tests); marts y snapshots pendientes.
- Fase 4 (Airflow): pendiente.

## Agentes disponibles

- `.claude/agents/data-extractor.md`: scripts de ingesta Python.
- `.claude/agents/dbt-modeler.md`: modelos, snapshots, tests y docs de dbt.
- `.claude/agents/airflow-orchestrator.md`: DAGs de Airflow.
