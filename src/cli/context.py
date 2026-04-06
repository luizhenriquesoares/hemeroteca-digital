"""Contexto compartilhado do CLI."""

from __future__ import annotations

import logging
from datetime import datetime

from rich.console import Console
from rich.logging import RichHandler

from src.config import LOGS_DIR

console = Console()


def configure_logging() -> logging.Logger:
    log_file = LOGS_DIR / f"hemeroteca_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RichHandler(console=console, show_path=False, markup=False),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    for noisy in ("httpx", "httpcore", "huggingface_hub", "sentence_transformers", "chromadb"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger("hemeroteca.cli")


logger = configure_logging()
