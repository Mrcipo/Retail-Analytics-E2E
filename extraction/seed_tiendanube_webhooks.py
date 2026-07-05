"""
Seed script: genera ~80 órdenes realistas y las envía como webhooks
al endpoint POST /webhooks/tiendanube de la API FastAPI local.

Uso:
    python -m extraction.seed_tiendanube_webhooks
"""

import random
from datetime import datetime, timedelta, timezone

import requests

from extraction.utils.db import get_connection
from extraction.utils.logger import get_logger

logger = get_logger("seed_tiendanube_webhooks")

API_URL = "http://localhost:8000/webhooks/tiendanube"
STORE_ID = 12345
NUM_ORDERS = 80
DAYS_BACK = 60

FIRST_NAMES = [
    "Valentina", "Sofía", "Camila", "Lucía", "Martina",
    "Agustina", "Florencia", "Carolina", "Daniela", "Natalia",
    "Santiago", "Matías", "Nicolás", "Sebastián", "Tomás",
    "Joaquín", "Facundo", "Ignacio", "Lucas", "Diego",
]
LAST_NAMES = [
    "García", "Rodríguez", "Martínez", "López", "González",
    "Pérez", "Sánchez", "Romero", "Torres", "Ramírez",
    "Flores", "Díaz", "Morales", "Herrera", "Castro",
]
EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com"]


def _random_date(days_back: int = DAYS_BACK) -> str:
    """Devuelve una fecha ISO 8601 aleatoria dentro de los últimos `days_back` días."""
    delta = random.randint(0, days_back * 24 * 60 * 60)
    dt = datetime.now(timezone.utc) - timedelta(seconds=delta)
    return dt.isoformat()


def _random_customer(order_id: int) -> dict:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    domain = random.choice(EMAIL_DOMAINS)
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{domain}"
    return {
        "id": order_id * 10 + random.randint(1, 9),
        "name": f"{first} {last}",
        "email": email,
    }


def fetch_products(limit: int = 30) -> list[dict]:
    """Lee codes y nombres reales de raw.off_products desde DuckDB."""
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT code, product_name FROM raw.off_products LIMIT ?", [limit]
            ).fetchall()
        products = [{"code": row[0], "product_name": row[1]} for row in rows]
        logger.info(f"Productos cargados desde DuckDB: {len(products)}")
        return products
    except Exception as exc:
        logger.error(f"Error al leer productos de DuckDB: {exc}")
        raise


def build_order(order_id: int, products: list[dict]) -> dict:
    """Construye el payload de una orden con 1-4 líneas de producto."""
    num_lines = random.randint(1, 4)
    selected = random.sample(products, k=min(num_lines, len(products)))

    order_lines = []
    total = 0.0
    for prod in selected:
        price = round(random.uniform(50.0, 5000.0), 2)
        qty = random.randint(1, 5)
        total += price * qty
        order_lines.append(
            {
                "product_id": prod["code"],
                "name": prod["product_name"],
                "quantity": qty,
                "price": f"{price:.2f}",
                "ean": prod["code"],
            }
        )

    return {
        "event": "order/created",
        "store_id": STORE_ID,
        "id": order_id,
        "created_at": _random_date(),
        "total": f"{total:.2f}",
        "customer": _random_customer(order_id),
        "products": order_lines,
    }


def send_webhooks(orders: list[dict]) -> tuple[int, int]:
    """Envía cada orden al endpoint. Devuelve (ok_count, fail_count)."""
    ok_count = 0
    fail_count = 0

    for i, payload in enumerate(orders, start=1):
        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            if response.status_code == 200:
                ok_count += 1
                logger.info(f"[{i}/{len(orders)}] OK — order_id={payload['id']}")
            else:
                fail_count += 1
                logger.warning(
                    f"[{i}/{len(orders)}] HTTP {response.status_code} — "
                    f"order_id={payload['id']} | body={response.text[:120]}"
                )
        except requests.RequestException as exc:
            fail_count += 1
            logger.error(
                f"[{i}/{len(orders)}] Error de red — order_id={payload['id']}: {exc}"
            )

    return ok_count, fail_count


def main() -> None:
    logger.info(f"Iniciando seed: {NUM_ORDERS} órdenes → {API_URL}")

    try:
        products = fetch_products(limit=30)
    except Exception:
        logger.error("No se pudieron cargar productos. Abortando.")
        return

    base_id = int(datetime.now(timezone.utc).timestamp())
    orders = [build_order(base_id + i, products) for i in range(NUM_ORDERS)]
    logger.info(f"Órdenes generadas: {len(orders)}")

    ok, fail = send_webhooks(orders)
    logger.info(
        f"Seed finalizado — OK: {ok} | Fallidos: {fail} | Total: {ok + fail}"
    )


if __name__ == "__main__":
    main()
