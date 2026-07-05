"""
Ingesta del Padrón de Contribuyentes AFIP.

Nota: el padrón se descarga manualmente desde el sitio de AFIP (requiere
sesión) y se coloca en data/raw/afip/padron_afip.csv. El script no es
incremental: cada corrida reemplaza el snapshot completo en raw.afip_padron.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))
from extraction.utils.logger import get_logger
from extraction.utils.db import get_connection, ensure_control_schema

logger = get_logger("afip_ingest")

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "afip"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_afip_csv(url: str, destination: Path) -> Path:
    """Descarga el CSV desde una URL pública, si aplica en tu caso."""
    try:
        logger.info(f"Descargando padrón AFIP desde {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        destination.write_bytes(response.content)
        logger.info(f"Archivo guardado en {destination} ({len(response.content)} bytes)")
        return destination
    except requests.exceptions.RequestException as e:
        logger.error(f"Fallo al descargar el CSV de AFIP: {e}")
        raise


def load_afip_csv(file_path: Path) -> pd.DataFrame:
    """Carga el CSV manejando encoding Latin-1 y separador ';'."""
    if not file_path.exists():
        raise FileNotFoundError(
            f"No se encontró {file_path}. Descargá el padrón y colocalo ahí."
        )
    try:
        logger.info(f"Leyendo CSV AFIP: {file_path}")
        df = pd.read_csv(
            file_path,
            encoding="latin-1",
            sep=";",
            dtype=str,               # todo string en la capa raw; se tipa en dbt staging
            on_bad_lines="warn",     # no frena la ingesta por una fila corrupta
            engine="python",
        )
        logger.info(f"CSV leído: {len(df)} filas, {len(df.columns)} columnas")
        return df
    except UnicodeDecodeError as e:
        logger.error(f"Error de encoding: {e}")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Error de parseo (separador/estructura): {e}")
        raise


def persist_raw_afip(df: pd.DataFrame) -> None:
    try:
        with get_connection() as conn:
            ensure_control_schema(conn)
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            conn.register("df_afip", df)
            conn.execute(
                "CREATE OR REPLACE TABLE raw.afip_padron AS "
                "SELECT *, now() AS _ingested_at FROM df_afip;"
            )
            conn.execute("""
                INSERT INTO control.ingestion_watermarks
                    (source_name, last_watermark, last_run_at, rows_ingested)
                VALUES ('afip_padron', NULL, ?, ?)
                ON CONFLICT (source_name) DO UPDATE SET
                    last_run_at = excluded.last_run_at,
                    rows_ingested = excluded.rows_ingested;
            """, [datetime.now(timezone.utc), len(df)])
        logger.info(f"Persistidas {len(df)} filas en raw.afip_padron")
    except Exception as e:
        logger.error(f"Error al persistir datos AFIP: {e}")
        raise


def run(local_file: str | None = None) -> None:
    try:
        file_path = Path(local_file) if local_file else RAW_DIR / "padron_afip.csv"
        df = load_afip_csv(file_path)
        persist_raw_afip(df)
        logger.info("Ingesta AFIP finalizada con éxito.")
    except Exception as e:
        logger.critical(f"Ingesta AFIP falló: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()