"""
Land Tiendanube Orders: consolida webhooks JSON de disco hacia tablas raw.* en DuckDB.

Uso:
    python -m extraction.land_tiendanube_orders
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from extraction.utils.db import ensure_control_schema, get_connection
from extraction.utils.logger import get_logger

logger = get_logger("land_tiendanube_orders")

RAW_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "tiendanube"
    / "order_created"
)


def load_json_files(raw_dir: Path) -> list[dict]:
    """Lee todos los archivos JSON del directorio de webhooks y devuelve los registros crudos."""
    files = list(raw_dir.glob("**/*.json"))
    logger.info(f"Archivos JSON encontrados: {len(files)}")

    records = []
    for path in files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            records.append(data)
        except Exception as exc:
            logger.error(f"Error leyendo {path}: {exc}")

    logger.info(f"Registros cargados correctamente: {len(records)}")
    return records


def parse_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Desanida los registros en listas de órdenes y líneas de orden."""
    orders = []
    order_lines = []

    for record in records:
        try:
            payload = record["payload"]
            order = {
                "order_id": payload["id"],
                "store_id": payload["store_id"],
                "customer_id": payload["customer"]["id"],
                "customer_name": payload["customer"]["name"],
                "customer_email": payload["customer"]["email"],
                "created_at": payload["created_at"],
                "total": float(payload["total"]),
                "_received_at": record["_received_at"],
            }
            orders.append(order)

            for product in payload.get("products", []):
                line = {
                    "order_id": payload["id"],
                    "product_code": product["product_id"],
                    "product_name": product["name"],
                    "quantity": int(product["quantity"]),
                    "unit_price": float(product["price"]),
                }
                order_lines.append(line)

        except Exception as exc:
            logger.error(
                f"Error parseando registro con order_id={record.get('payload', {}).get('id', 'UNKNOWN')}: {exc}"
            )

    logger.info(f"Órdenes parseadas: {len(orders)} | Líneas parseadas: {len(order_lines)}")
    return orders, order_lines


def persist(df_orders: pl.DataFrame, df_lines: pl.DataFrame) -> None:
    """Persiste los DataFrames en DuckDB y actualiza el watermark de control."""
    try:
        with get_connection() as conn:
            ensure_control_schema(conn)
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            conn.register("df_orders", df_orders.to_arrow())
            conn.execute("""
                CREATE OR REPLACE TABLE raw.tiendanube_orders AS
                SELECT DISTINCT ON (order_id) * FROM df_orders
                ORDER BY order_id, _received_at DESC
            """)
            logger.info(
                f"Tabla raw.tiendanube_orders creada con "
                f"{conn.execute('SELECT COUNT(*) FROM raw.tiendanube_orders').fetchone()[0]} filas"
            )

            conn.register("df_lines", df_lines.to_arrow())
            conn.execute("""
                CREATE OR REPLACE TABLE raw.tiendanube_order_lines AS
                SELECT * FROM df_lines
            """)
            logger.info(
                f"Tabla raw.tiendanube_order_lines creada con "
                f"{conn.execute('SELECT COUNT(*) FROM raw.tiendanube_order_lines').fetchone()[0]} filas"
            )

            conn.execute(
                """
                INSERT INTO control.ingestion_watermarks
                    (source_name, last_watermark, last_run_at, rows_ingested)
                VALUES ('tiendanube_orders', NULL, ?, ?)
                ON CONFLICT (source_name) DO UPDATE SET
                    last_run_at = excluded.last_run_at,
                    rows_ingested = excluded.rows_ingested
                """,
                [datetime.now(timezone.utc), len(df_orders)],
            )
            logger.info("Watermark actualizado en control.ingestion_watermarks")

    except Exception as exc:
        logger.error(f"Error al persistir en DuckDB: {exc}")
        raise


def run() -> None:
    logger.info("=== Iniciando land_tiendanube_orders ===")

    try:
        records = load_json_files(RAW_DIR)
        if not records:
            logger.critical("No se encontraron registros JSON. Abortando.")
            sys.exit(1)

        orders, order_lines = parse_records(records)
        if not orders:
            logger.critical("No se pudieron parsear órdenes. Abortando.")
            sys.exit(1)

        df_orders = pl.DataFrame(orders)
        df_lines = pl.DataFrame(order_lines)

        persist(df_orders, df_lines)

        logger.info("=== land_tiendanube_orders finalizado con éxito ===")

    except SystemExit:
        raise
    except Exception as exc:
        logger.critical(f"Error fatal en run(): {exc}")
        sys.exit(1)


if __name__ == "__main__":
    run()
