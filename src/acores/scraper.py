"""Scraper para acervos paroquiais da Biblioteca Digital dos Açores."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

ACORES_BASE_URL = "https://culturacores.azores.gov.pt/biblioteca_digital/"
DEFAULT_DELAY = 1.0  # segundos entre requests


def discover_collection_pages(collection_id: str) -> list[str]:
    """Descobre as páginas de uma coleção testando URLs sequenciais."""
    # O index.html é gerado por JS, não tem links estáticos.
    # Descobrir total testando URLs sequenciais até 404.
    logger.info("Descobrindo páginas de %s...", collection_id)

    pages = []
    for i in range(1, 500):
        page_num = f"{i:04d}"
        url = build_image_url(collection_id, page_num)
        try:
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                pages.append(page_num)
            else:
                # 3 falhas seguidas = fim da coleção
                if len(pages) > 0:
                    next1 = requests.head(build_image_url(collection_id, f"{i+1:04d}"), timeout=10)
                    if next1.status_code != 200:
                        break
        except Exception:
            if len(pages) > 0:
                break

    logger.info("Coleção %s: %d páginas encontradas", collection_id, len(pages))
    return pages


def build_image_url(collection_id: str, page_num: str) -> str:
    """Monta a URL da imagem JPG de uma página."""
    return (
        f"{ACORES_BASE_URL}{collection_id}/"
        f"{collection_id}_master/{collection_id}_JPG/"
        f"{collection_id}_{page_num}.jpg"
    )


def download_collection(
    collection_id: str,
    output_dir: Path,
    *,
    delay: float = DEFAULT_DELAY,
    force: bool = False,
) -> dict:
    """Baixa todas as imagens de uma coleção paroquial.

    Args:
        collection_id: ex. "SMG-PD-SAOPEDRO-B-1798-1813"
        output_dir: diretório base para salvar
        delay: delay entre downloads em segundos
        force: re-baixar mesmo se já existe

    Returns:
        dict com estatísticas
    """
    pages = discover_collection_pages(collection_id)
    if not pages:
        return {"collection": collection_id, "pages": 0, "downloaded": 0, "skipped": 0, "errors": 0}

    col_dir = output_dir / collection_id
    col_dir.mkdir(parents=True, exist_ok=True)

    # Salvar metadata
    meta = _parse_collection_metadata(collection_id)
    meta["total_pages"] = len(pages)
    (col_dir / "metadata.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    downloaded = 0
    skipped = 0
    errors = 0

    for page_num in pages:
        img_path = col_dir / f"{collection_id}_{page_num}.jpg"

        if img_path.exists() and not force:
            skipped += 1
            continue

        url = build_image_url(collection_id, page_num)
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            img_path.write_bytes(resp.content)
            downloaded += 1
            logger.debug("Baixada: %s (%d bytes)", img_path.name, len(resp.content))
        except Exception as exc:
            errors += 1
            logger.warning("Erro ao baixar %s: %s", url, exc)

        if delay > 0:
            time.sleep(delay)

    logger.info(
        "Coleção %s: %d baixadas, %d existentes, %d erros",
        collection_id, downloaded, skipped, errors,
    )
    return {
        "collection": collection_id,
        "pages": len(pages),
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
    }


def _parse_collection_metadata(collection_id: str) -> dict:
    """Extrai metadados do ID da coleção.

    Ex: SMG-PD-SAOPEDRO-B-1798-1813
        ilha=SMG (São Miguel), tipo=PD (Ponta Delgada),
        parish=SAOPEDRO, record_type=B (batismos),
        years=1798-1813
    """
    parts = collection_id.split("-")
    record_types = {"B": "baptism", "C": "marriage", "O": "death"}

    island = parts[0] if parts else ""
    city = parts[1] if len(parts) > 1 else ""
    parish = parts[2] if len(parts) > 2 else ""
    rec_type = parts[3] if len(parts) > 3 else ""
    year_start = parts[4] if len(parts) > 4 else ""
    year_end = parts[5] if len(parts) > 5 else ""

    island_names = {
        "SMG": "São Miguel", "TER": "Terceira", "FAI": "Faial",
        "PIC": "Pico", "SJG": "São Jorge", "GRA": "Graciosa",
        "FLO": "Flores", "COR": "Corvo", "SMA": "Santa Maria",
    }

    return {
        "collection_id": collection_id,
        "island_code": island,
        "island": island_names.get(island, island),
        "city": city,
        "parish": parish,
        "record_type": record_types.get(rec_type, rec_type),
        "record_type_code": rec_type,
        "year_start": year_start,
        "year_end": year_end,
    }
