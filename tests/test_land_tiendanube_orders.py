import json

import duckdb
import polars as pl
import pytest

from extraction import land_tiendanube_orders as land
from extraction.seed_tiendanube_webhooks import build_order
from extraction.utils import db

CATALOG = [{"code": "779000000001", "product_name": "Yerba"}, {"code": "779000000002", "product_name": "Café"}]


def _record(order_id, received_at="2026-01-01T00:00:00+00:00"):
    return {"_received_at": received_at, "_topic": "order/created", "payload": build_order(order_id, CATALOG)}


@pytest.fixture
def duckdb_path(tmp_path, monkeypatch):
    path = tmp_path / "test.duckdb"
    monkeypatch.setattr(db, "DUCKDB_PATH", str(path))
    return path


def test_parse_records_flattens_orders_and_lines():
    record = _record(1)
    orders, lines = land.parse_records([record])

    assert len(orders) == 1
    assert orders[0]["order_id"] == 1
    assert orders[0]["customer_email"] == record["payload"]["customer"]["email"]
    assert len(lines) == len(record["payload"]["products"])
    assert {line["product_code"] for line in lines} <= {p["code"] for p in CATALOG}


def test_parse_records_skips_malformed_records():
    orders, lines = land.parse_records([{"payload": {"id": 99}}, _record(2)])

    assert [o["order_id"] for o in orders] == [2]


def test_load_json_files_reads_nested_partitions(tmp_path):
    day = tmp_path / "2026-01-01"
    day.mkdir()
    (day / "a.json").write_text(json.dumps(_record(1)), encoding="utf-8")
    (day / "broken.json").write_text("{", encoding="utf-8")

    records = land.load_json_files(tmp_path)

    assert len(records) == 1


def test_persist_keeps_latest_version_of_each_order(duckdb_path):
    old = _record(1, received_at="2026-01-01T00:00:00+00:00")
    new = _record(1, received_at="2026-01-02T00:00:00+00:00")
    new["payload"]["total"] = "999.00"
    orders, lines = land.parse_records([old, new, _record(2)])

    land.persist(pl.DataFrame(orders), pl.DataFrame(lines))

    conn = duckdb.connect(str(duckdb_path))
    rows = conn.execute("SELECT order_id, total FROM raw.tiendanube_orders ORDER BY order_id").fetchall()
    watermark = conn.execute(
        "SELECT rows_ingested FROM control.ingestion_watermarks WHERE source_name = 'tiendanube_orders'"
    ).fetchone()
    conn.close()

    assert rows[0] == (1, 999.0)
    assert len(rows) == 2
    assert watermark == (3,)
