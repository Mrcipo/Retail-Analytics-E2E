import sys
from pathlib import Path

from fastapi import FastAPI

sys.path.append(str(Path(__file__).resolve().parents[1]))
from api.routers import webhooks
from extraction.utils.logger import get_logger

logger = get_logger("api_main")

app = FastAPI(title="Retail Analytics - Ingestion API", version="0.1.0")
app.include_router(webhooks.router)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.on_event("startup")
def on_startup():
    logger.info("API de ingesta iniciada.")