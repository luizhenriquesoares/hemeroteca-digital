"""Persistência e execução em lote para OCR."""

from __future__ import annotations

import json
import logging
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

from src.config import IMAGES_DIR, TEXT_DIR
from src.processing.ocr_quality import OCRComparison

logger = logging.getLogger(__name__)


def comparison_metadata(comparison: OCRComparison, *, bib: str, image_name: str, text_name: str, texto: str) -> dict:
    selected_result = comparison.new_result if comparison.selected_source == "new" else comparison.existing_result
    return {
        "bib": bib,
        "imagem": image_name,
        "arquivo_texto": text_name,
        "caracteres": len(texto),
        "palavras": len(texto.split()),
        "ocr_quality_score": selected_result.score if selected_result else 0.0,
        "ocr_variant": comparison.selected_variant,
        "ocr_metrics": selected_result.metrics if selected_result else {},
        "ocr_source_selected": comparison.selected_source,
        "ocr_decision_reason": comparison.reason,
        "ocr_candidate_variant": comparison.new_result.variant,
        "ocr_candidate_score": comparison.new_result.score,
        "ocr_candidate_metrics": comparison.new_result.metrics,
    }


def persist_ocr_outputs(txt_path: Path, meta_path: Path, metadata: dict, texto: str, comparison: OCRComparison) -> bool:
    if not texto:
        return False
    if comparison.selected_source == "new" or not txt_path.exists():
        txt_path.write_text(texto, encoding="utf-8")
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def build_ocr_task_list(*, force: bool = False) -> list[tuple[str, str, str, str]]:
    tasks = []
    if not IMAGES_DIR.exists():
        return tasks

    for acervo_dir in sorted(IMAGES_DIR.iterdir()):
        if not acervo_dir.is_dir():
            continue
        bib = acervo_dir.name
        txt_dir = TEXT_DIR / bib
        txt_dir.mkdir(parents=True, exist_ok=True)

        imagens = sorted(acervo_dir.glob("*.jpg")) + sorted(acervo_dir.glob("*.png"))
        for img_path in imagens:
            txt_path = txt_dir / f"{img_path.stem}.txt"
            meta_path = txt_dir / f"{img_path.stem}.json"
            if txt_path.exists() and not force:
                continue
            tasks.append((str(img_path), str(txt_path), str(meta_path), bib))
    return tasks


def run_parallel_ocr(tasks: list[tuple[str, str, str, str]], worker_fn, workers: int = 0) -> dict:
    if not tasks:
        logger.info("Nenhuma imagem para processar")
        return {}

    if workers <= 0:
        workers = max(2, cpu_count() - 2)

    logger.info("OCR paralelo: %s imagens com %s workers", len(tasks), workers)
    stats = {}
    done = 0
    t0 = time.time()

    with Pool(processes=workers) as pool:
        for img_name, success, _chars in pool.imap_unordered(worker_fn, tasks, chunksize=4):
            done += 1
            if success:
                bib = img_name.rsplit("_", 1)[0] if "_" in img_name else "unknown"
                stats[bib] = stats.get(bib, 0) + 1

            if done % 100 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed
                eta = (len(tasks) - done) / rate if rate > 0 else 0
                logger.info(
                    "OCR: %s/%s (%s%%) | %.1f img/s | ETA %.0fmin",
                    done,
                    len(tasks),
                    done * 100 // len(tasks),
                    rate,
                    eta / 60,
                )

    elapsed = time.time() - t0
    total_ok = sum(stats.values())
    logger.info("OCR concluído: %s/%s em %.1fmin (%.1f img/s)", total_ok, len(tasks), elapsed / 60, total_ok / elapsed)
    return stats
