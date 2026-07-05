import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "data" / "control" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Garantiza UTF-8 en stdout para que caracteres no-ASCII no rompan en Windows (cp1252).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def get_logger(name: str) -> logging.Logger:
    """Logger estándar del proyecto: consola + archivo por módulo."""
    logger = logging.getLogger(name)
    if logger.handlers:  # evita duplicar handlers si se importa más de una vez
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger