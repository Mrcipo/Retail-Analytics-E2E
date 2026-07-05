import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from extraction.utils.logger import get_logger

logger = get_logger("webhooks_api")
router = APIRouter(prefix="/webhooks", tags=["webhooks"])

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "tiendanube"
ALLOWED_TOPICS = {
    "order/created", "order/updated",
    "product/created", "product/updated",
    "customer/created",
}


@router.post("/tiendanube")
async def receive_tiendanube_webhook(request: Request):
    """Recibe webhooks y los persiste tal cual (bronze layer), particionados por topic/fecha."""
    try:
        payload = await request.json()
    except json.JSONDecodeError as e:
        logger.error(f"Payload inválido (no es JSON): {e}")
        raise HTTPException(status_code=400, detail="Payload JSON inválido")

    topic = payload.get("event") or payload.get("topic")
    if topic not in ALLOWED_TOPICS:
        logger.warning(f"Topic no reconocido: {topic}")
        raise HTTPException(status_code=422, detail=f"Topic '{topic}' no soportado")

    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target_dir = RAW_DIR / topic.replace("/", "_") / today
        target_dir.mkdir(parents=True, exist_ok=True)

        record = {
            "_received_at": datetime.now(timezone.utc).isoformat(),
            "_topic": topic,
            "payload": payload,
        }
        file_path = target_dir / f"{datetime.now(timezone.utc).timestamp()}.json"
        file_path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")

        logger.info(f"Webhook '{topic}' persistido en {file_path}")
        return {"status": "ok", "topic": topic}
    except OSError as e:
        logger.error(f"Error de filesystem al guardar el webhook: {e}")
        raise HTTPException(status_code=500, detail="Error interno al persistir el webhook")