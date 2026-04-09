"""Pipeline completo: download → OCR → banco para acervos paroquiais dos Açores."""

from __future__ import annotations

import logging
from pathlib import Path

from src.config import DATA_DIR

logger = logging.getLogger(__name__)

ACORES_DATA_DIR = DATA_DIR / "acores"
ACORES_IMAGES_DIR = ACORES_DATA_DIR / "images"
ACORES_OCR_DIR = ACORES_DATA_DIR / "ocr"


def run_pipeline(
    collection_id: str,
    *,
    skip_download: bool = False,
    skip_ocr: bool = False,
    model: str = "gpt-4o",
    force: bool = False,
    max_pages: int | None = None,
) -> dict:
    """Executa o pipeline completo para uma coleção paroquial.

    Args:
        collection_id: ex. "SMG-PD-SAOPEDRO-B-1798-1813"
        skip_download: pular download de imagens
        skip_ocr: pular OCR (usar resultados existentes)
        model: modelo para OCR (gpt-4o, gpt-4o-mini)
        force: re-processar mesmo se já existe
        max_pages: limitar número de páginas (para teste)
    """
    import json

    from src.acores.scraper import download_collection, _parse_collection_metadata
    from src.acores.ocr import transcribe_collection
    from src.acores.repository import ParishRepository

    metadata = _parse_collection_metadata(collection_id)
    images_dir = ACORES_IMAGES_DIR / collection_id
    ocr_dir = ACORES_OCR_DIR / collection_id

    result = {
        "collection": collection_id,
        "metadata": metadata,
        "download": None,
        "ocr": None,
        "import": None,
    }

    # 1. Download
    if not skip_download:
        logger.info("Baixando imagens de %s...", collection_id)
        dl = download_collection(collection_id, ACORES_IMAGES_DIR, force=force)
        result["download"] = dl
        logger.info("Download: %d páginas", dl["downloaded"] + dl["skipped"])
    else:
        logger.info("Download pulado")

    # Limitar páginas para teste
    if max_pages:
        images = sorted(images_dir.glob("*.jpg"))[:max_pages]
        # Mover extras para um subdir temporário
        temp_dir = images_dir / "_overflow"
        temp_dir.mkdir(exist_ok=True)
        all_images = sorted(images_dir.glob("*.jpg"))
        for img in all_images[max_pages:]:
            img.rename(temp_dir / img.name)

    # 2. OCR
    if not skip_ocr:
        logger.info("Transcrevendo com %s...", model)
        ocr = transcribe_collection(
            images_dir, ocr_dir,
            metadata=metadata, model=model, force=force,
        )
        result["ocr"] = ocr
        logger.info("OCR: %d páginas, %d registros, $%.3f", ocr["pages_processed"], ocr["records_extracted"], ocr["total_cost"])
    else:
        logger.info("OCR pulado")

    # Restaurar imagens se limitamos
    if max_pages:
        temp_dir = images_dir / "_overflow"
        if temp_dir.exists():
            for img in temp_dir.glob("*.jpg"):
                img.rename(images_dir / img.name)
            temp_dir.rmdir()

    # 3. Importar para banco
    logger.info("Importando registros para banco...")
    repo = ParishRepository()
    imp = repo.import_collection_results(ocr_dir, collection_id)
    result["import"] = imp
    logger.info("Importados: %d registros", imp["imported"])

    # Stats finais
    stats = repo.get_stats()
    result["stats"] = stats

    return result
