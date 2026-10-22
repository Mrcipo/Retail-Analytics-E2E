"""
Crea una base DuckDB chica con datos sintéticos en raw.* para probar dbt sin
descargar el Parquet de Open Food Facts ni el padrón de AFIP.

Uso (desde la raíz del repo):
    DUCKDB_PATH=./data/sample.duckdb python -m scripts.create_sample_db

Reutiliza las mismas funciones de persistencia que las ingestas reales, así que
las tablas raw quedan con el mismo esquema.
"""

import random

import pandas as pd
import polars as pl

from extraction.afip_ingest import persist_raw_afip
from extraction.land_tiendanube_orders import parse_records, persist
from extraction.openfoodfacts_ingest import persist_incremental
from extraction.seed_tiendanube_webhooks import build_order
from extraction.utils.db import ensure_control_schema, get_connection
from extraction.utils.logger import get_logger

logger = get_logger("create_sample_db")

NUM_PRODUCTS = 20
NUM_ORDERS = 30
SEED = 42


def sample_products() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "code": [f"7790000{i:06d}" for i in range(NUM_PRODUCTS)],
            "product_name": [f" Producto {i} " for i in range(NUM_PRODUCTS)],
            "categories": ["Bebidas" if i % 2 else "Snacks" for i in range(NUM_PRODUCTS)],
            "brands": [f"Marca {i % 5}" for i in range(NUM_PRODUCTS)],
            "countries": ["en:argentina"] * NUM_PRODUCTS,
            "last_modified_t": [1_700_000_000 + i for i in range(NUM_PRODUCTS)],
        }
    )


def sample_afip() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cuit": ["30-71234567-1", "20-12345678-9", "27-87654321-0"],
            "razon_social": ["Distribuidora Sur SA", "Juan Pérez", "Almacén Norte SRL"],
            "condicion_iva": ["responsable inscripto", "monotributo", "responsable inscripto"],
            "domicilio_fiscal": ["Av. Siempreviva 742", "Calle Falsa 123", "Mitre 500"],
            "actividad_principal": ["463100", "471110", "472190"],
            "fecha_inscripcion": ["2015-03-01", "2019-07-15", "no-informada"],
        }
    )


def main() -> None:
    random.seed(SEED)

    products = sample_products()
    with get_connection() as conn:
        ensure_control_schema(conn)
        persist_incremental(products, conn)

    persist_raw_afip(sample_afip())

    catalog = [
        {"code": code, "product_name": name.strip()}
        for code, name in zip(products["code"], products["product_name"])
    ]
    records = [
        {"_received_at": f"2026-01-01T00:00:{i:02d}+00:00", "_topic": "order/created",
         "payload": build_order(1000 + i, catalog)}
        for i in range(NUM_ORDERS)
    ]
    orders, lines = parse_records(records)
    persist(pl.DataFrame(orders), pl.DataFrame(lines))

    logger.info(f"Base de ejemplo lista: {NUM_PRODUCTS} productos, {len(orders)} órdenes, {len(lines)} líneas")


if __name__ == "__main__":
    main()
