import os
from contextlib import contextmanager

import duckdb
from dotenv import load_dotenv

load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/retail_analytics.duckdb")


@contextmanager
def get_connection():
    """Context manager que garantiza el cierre de la conexión aunque falle algo."""
    conn = None
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        yield conn
    finally:
        if conn is not None:
            conn.close()


def ensure_control_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Crea el esquema de control y la tabla de watermarks si no existen."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS control;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS control.ingestion_watermarks (
            source_name     VARCHAR PRIMARY KEY,
            last_watermark  BIGINT,
            last_run_at     TIMESTAMP,
            rows_ingested   BIGINT
        );
    """)