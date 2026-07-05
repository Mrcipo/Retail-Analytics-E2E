"""
Ingesta incremental de Open Food Facts.

Se escanea el Parquet en modo lazy (Polars) y se filtra por last_modified_t
mayor al último watermark guardado en control.ingestion_watermarks, para no
reprocesar millones de filas que no cambiaron.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import requests
import polars as pl

sys.path.append(str(Path(__file__).resolve().parents[1]))
from extraction.utils.logger import get_logger
from extraction.utils.db import get_connection, ensure_control_schema

logger = get_logger("openfoodfacts_ingest")

SOURCE_NAME = "openfoodfacts_products"
PARQUET_URL = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true"
LOCAL_PARQUET = Path(__file__).resolve().parents[1] / "data" / "raw" / "off" / "food.parquet"
RAW_COLUMNS = ["code", "product_name", "categories", "brands", "countries_tags", "last_modified_t"]

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def get_remote_size(url: str) -> int:
    """Obtiene el Content-Length del servidor siguiendo redirects."""
    try:
        r = requests.head(url, timeout=30, allow_redirects=True)
        r.raise_for_status()
        return int(r.headers.get("Content-Length", 0))
    except Exception as e:
        logger.warning(f"No se pudo obtener el tamaño remoto: {e}")
        return 0


def download_parquet(url: str, destination: Path, remote_size: int) -> None:
    """Descarga el Parquet con streaming por chunks y soporte de resume (Range header)."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    local_size = destination.stat().st_size if destination.exists() else 0
    headers = {}
    mode = "wb"
    if local_size > 0:
        headers["Range"] = f"bytes={local_size}-"
        mode = "ab"
        logger.info(f"Resumiendo descarga desde {local_size / 1024**2:.0f} MB")
    else:
        logger.info(f"Iniciando descarga desde {url}")
    try:
        with requests.get(url, stream=True, timeout=60, allow_redirects=True, headers=headers) as r:
            if r.status_code == 416:
                logger.info("El servidor indica que el archivo ya está completo (416 Range Not Satisfiable).")
                return
            r.raise_for_status()
            downloaded = local_size
            with open(destination, mode) as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        print(f"\r  Descargado: {downloaded / 1024**2:.0f} MB / {remote_size / 1024**2:.0f} MB", end="", flush=True)
            print()
        logger.info(f"Parquet en {destination} ({destination.stat().st_size / 1024**3:.2f} GB)")
    except Exception as e:
        logger.error(f"Error al descargar el Parquet: {e}")
        raise


def ensure_parquet_local(url: str, destination: Path) -> None:
    """Descarga o resume el Parquet hasta que el tamaño local coincida con el remoto."""
    remote_size = get_remote_size(url)
    local_size = destination.stat().st_size if destination.exists() else 0
    if remote_size > 0 and local_size >= remote_size:
        logger.info(f"Parquet local completo: {destination} ({local_size / 1024**3:.2f} GB)")
        return
    if local_size > 0:
        logger.info(f"Parquet incompleto ({local_size / 1024**2:.0f} MB / {remote_size / 1024**2:.0f} MB), resumiendo...")
    else:
        logger.info(f"Parquet no encontrado, descargando ({remote_size / 1024**3:.2f} GB)...")
    download_parquet(url, destination, remote_size)


def get_last_watermark(conn) -> int:
    try:
        result = conn.execute(
            "SELECT last_watermark FROM control.ingestion_watermarks WHERE source_name = ?",
            [SOURCE_NAME],
        ).fetchone()
        watermark = result[0] if result and result[0] is not None else 0
        logger.info(f"Watermark actual para '{SOURCE_NAME}': {watermark}")
        return watermark
    except Exception as e:
        logger.error(f"Error al leer el watermark: {e}")
        raise


def extract_incremental(watermark: int, parquet_path: Path) -> pl.LazyFrame:
    """
    Extrae productos nuevos/modificados desde el último watermark.

    Transformaciones aplicadas sobre el schema crudo de Hugging Face:
    - product_name: List(Struct({lang, text})) -> se toma el texto del primer
      idioma disponible en la lista (suele ser el idioma principal del producto).
    - countries_tags: List(String) -> se aplana a string separado por comas.
    """
    try:
        logger.info(f"Escaneando Parquet local: {parquet_path}")
        return (
            pl.scan_parquet(parquet_path)
            .select(RAW_COLUMNS)
            .filter(pl.col("last_modified_t") > watermark)
            .with_columns([
                pl.col("product_name").list.first().struct.field("text").alias("product_name"),
                pl.col("countries_tags").list.join(", ").alias("countries"),
            ])
            .drop("countries_tags")
            .head(1000)  # Limitar a 1k filas para validación inicial
        )
    except Exception as e:
        logger.error(f"Error al escanear el Parquet de Open Food Facts: {e}")
        raise


def persist_incremental(df: pl.DataFrame, conn) -> None:
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.off_products (
                code             VARCHAR PRIMARY KEY,
                product_name     VARCHAR,
                categories       VARCHAR,
                brands           VARCHAR,
                countries        VARCHAR,
                last_modified_t  BIGINT,
                _ingested_at     TIMESTAMP
            );
        """)
        conn.register("incoming_products", df.to_arrow())
        conn.execute("""
            INSERT INTO raw.off_products
            SELECT code, product_name, categories, brands, countries, last_modified_t, now()
            FROM incoming_products
            ON CONFLICT (code) DO UPDATE SET
                product_name = excluded.product_name,
                categories = excluded.categories,
                brands = excluded.brands,
                countries = excluded.countries,
                last_modified_t = excluded.last_modified_t,
                _ingested_at = excluded._ingested_at;
        """)
        logger.info(f"Upsert de {len(df)} productos en raw.off_products completado.")
    except Exception as e:
        logger.error(f"Error al persistir productos en DuckDB: {e}")
        raise


def update_watermark(conn, new_watermark: int, rows_ingested: int) -> None:
    try:
        conn.execute("""
            INSERT INTO control.ingestion_watermarks
                (source_name, last_watermark, last_run_at, rows_ingested)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source_name) DO UPDATE SET
                last_watermark = excluded.last_watermark,
                last_run_at = excluded.last_run_at,
                rows_ingested = excluded.rows_ingested;
        """, [SOURCE_NAME, new_watermark, datetime.now(timezone.utc), rows_ingested])
        logger.info(f"Watermark actualizado a {new_watermark}")
    except Exception as e:
        logger.error(f"Error al actualizar el watermark: {e}")
        raise


def run() -> None:
    try:
        ensure_parquet_local(PARQUET_URL, LOCAL_PARQUET)

        with get_connection() as conn:
            ensure_control_schema(conn)
            watermark = get_last_watermark(conn)

            df = extract_incremental(watermark, LOCAL_PARQUET).collect()

            if df.is_empty():
                logger.info("No hay productos nuevos o modificados desde el último watermark.")
                return

            persist_incremental(df, conn)
            new_watermark = int(df["last_modified_t"].max())
            update_watermark(conn, new_watermark, len(df))

        logger.info("Ingesta incremental de Open Food Facts finalizada con éxito.")
    except Exception as e:
        logger.critical(f"Ingesta de Open Food Facts falló: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()