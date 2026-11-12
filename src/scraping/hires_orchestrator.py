"""Orquestração paralela do pipeline hi-res."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from threading import Lock, Thread

from src.config import CACHE_DIR, TEXT_DIR

logger = logging.getLogger(__name__)
_PAGE_SUFFIX_RE = re.compile(r"_(\d{5})\.json$")


def get_total_pages(bib: str) -> int:
    cache_file = CACHE_DIR / "acervos_pe.json"
    if not cache_file.exists():
        return 0
    try:
        with open(cache_file, encoding="utf-8") as fh:
            acervos = json.load(fh)
        for acervo in acervos:
            if acervo["bib"] == bib:
                return acervo.get("paginas", 0)
    except Exception:
        pass
    return 0


def get_acervo_nome(bib: str) -> str:
    """Título do periódico a partir do cache de acervos.

    Sem isso o metadado da página fica com `jornal: "Acervo 029033_01"` e o
    título real se perde — o que quebra a ficha historiográfica e a busca por
    periódico.
    """
    cache_file = CACHE_DIR / "acervos_pe.json"
    if cache_file.exists():
        try:
            with open(cache_file, encoding="utf-8") as fh:
                for acervo in json.load(fh):
                    if acervo.get("bib") == bib and acervo.get("nome"):
                        return acervo["nome"]
        except Exception:
            pass
    return f"Acervo {bib}"


def _build_full_ranges(total_pages: int, workers: int, max_pages: int = 0) -> list[tuple[int, int]]:
    target_total = min(total_pages, max_pages) if max_pages else total_pages
    if target_total <= 0:
        return []

    worker_count = max(1, min(workers, target_total))
    chunk_size = target_total // worker_count
    remainder = target_total % worker_count

    ranges: list[tuple[int, int]] = []
    start = 1
    for index in range(worker_count):
        extra = 1 if index < remainder else 0
        end = start + chunk_size + extra - 1
        ranges.append((start, end))
        start = end + 1
    return ranges


def _load_saved_hires_pages(txt_dir: Path) -> set[int]:
    saved_pages: set[int] = set()
    for meta_path in txt_dir.glob("*.json"):
        match = _PAGE_SUFFIX_RE.search(meta_path.name)
        if not match:
            continue
        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if metadata.get("hires"):
            saved_pages.add(int(match.group(1)))
    return saved_pages


def _merge_pages_to_ranges(pages: list[int]) -> list[tuple[int, int]]:
    if not pages:
        return []

    ranges: list[tuple[int, int]] = []
    start = prev = pages[0]
    for page in pages[1:]:
        if page == prev + 1:
            prev = page
            continue
        ranges.append((start, prev))
        start = prev = page
    ranges.append((start, prev))
    return ranges


def _build_resume_ranges(
    *,
    txt_dir: Path,
    total_pages: int,
    failed_pages: list[int],
    max_pages: int = 0,
) -> tuple[list[tuple[int, int]], dict]:
    saved_pages = _load_saved_hires_pages(txt_dir)
    highest_saved = max(saved_pages) if saved_pages else 0

    implicit_missing = [
        page
        for page in range(1, highest_saved + 1)
        if page not in saved_pages and page not in failed_pages
    ]

    target_pages = set(failed_pages)
    if highest_saved < total_pages:
        target_pages.update(range(highest_saved + 1, total_pages + 1))
    target_pages.update(implicit_missing)

    sorted_targets = sorted(target_pages)
    if max_pages:
        sorted_targets = sorted_targets[:max_pages]

    stats = {
        "highest_saved": highest_saved,
        "saved_pages": len(saved_pages),
        "failed_pages": len(failed_pages),
        "implicit_missing": len(implicit_missing),
        "pending_tail": max(0, total_pages - highest_saved),
        "target_pages": len(sorted_targets),
    }
    return _merge_pages_to_ranges(sorted_targets), stats


def processar_acervo_paralelo(
    acervo,
    *,
    workers,
    headless,
    force,
    max_pages,
    keep_images,
    processar_com_driver_fn,
    load_progress_fn,
    mark_done_fn,
    set_bib_stats_fn=None,
    create_driver_fn,
):
    bib_code = acervo["bib"]
    nome = acervo["nome"]
    total_pages = get_total_pages(bib_code) or acervo.get("paginas", 0)

    if not total_pages:
        logger.error("Total de páginas desconhecido para %s. Use --workers 1.", bib_code)
        return {}

    txt_dir = TEXT_DIR / bib_code
    txt_dir.mkdir(parents=True, exist_ok=True)

    progress = load_progress_fn()
    failed_pages = sorted(set(progress.get("failed_pages", {}).get(bib_code, [])))
    if force:
        ranges = _build_full_ranges(total_pages, workers, max_pages=max_pages)
        logger.info(
            "Hi-res paralelo: %s (%s páginas) | %s workers | ranges: %s",
            bib_code,
            total_pages,
            workers,
            ranges,
        )
    else:
        ranges, resume_stats = _build_resume_ranges(
            txt_dir=txt_dir,
            total_pages=total_pages,
            failed_pages=failed_pages,
            max_pages=max_pages,
        )
        if not ranges:
            logger.info("Hi-res resume: %s sem páginas pendentes após checagem local.", bib_code)
            mark_done_fn(bib_code)
            return {bib_code: 0}
        logger.info(
            "Hi-res resume: %s | %s ranges | maior página salva=%s | falhas=%s | cauda pendente=%s",
            bib_code,
            len(ranges),
            resume_stats["highest_saved"],
            resume_stats["failed_pages"],
            resume_stats["pending_tail"],
        )
        logger.info("Hi-res resume ranges (%s): %s", bib_code, ranges[:12] + ([("...", "...")] if len(ranges) > 12 else []))

    results = []
    queue = list(ranges)
    queue_lock = Lock()

    def safe_pop():
        with queue_lock:
            if queue:
                return queue.pop(0)
            return None

    def worker_fn(worker_id):
        driver = create_driver_fn(headless=headless)
        try:
            while True:
                task = safe_pop()
                if task is None:
                    break

                start_page, end_page = task
                logger.info("Worker %s: %s páginas %s-%s", worker_id, bib_code, start_page, end_page)
                result = processar_com_driver_fn(
                    driver,
                    bib_code,
                    nome,
                    txt_dir,
                    force=force,
                    max_pages=0,
                    keep_images=keep_images,
                    start_page=start_page,
                    end_page=end_page,
                )
                results.append(result)
                logger.info(
                    "Worker %s: ✓ %s p%s-%s — %s processadas, %s skipped",
                    worker_id,
                    bib_code,
                    start_page,
                    end_page,
                    result["processed"],
                    result.get("skipped", 0),
                )
        except Exception as exc:
            logger.error("Worker %s: ERRO em %s: %s", worker_id, bib_code, exc)
            results.append({"processed": 0, "failed_pages": [f"error:{exc}"], "complete": False})
        finally:
            try:
                driver.quit()
            except Exception:
                pass
            logger.info("Worker %s: finalizado", worker_id)

    threads = []
    for index in range(min(workers, len(ranges))):
        thread = Thread(target=worker_fn, args=(index + 1,), daemon=True)
        thread.start()
        threads.append(thread)
        time.sleep(3)

    for thread in threads:
        thread.join()

    total_processed = sum(result["processed"] for result in results if result)
    total_skipped = sum(result.get("skipped", 0) for result in results if result)
    progress = load_progress_fn()
    all_failed_pages = sorted(set(progress["failed_pages"].get(bib_code, [])))
    complete = len(all_failed_pages) == 0 and total_processed > 0

    if set_bib_stats_fn:
        set_bib_stats_fn(
            bib_code,
            {
                "processed": total_processed,
                "skipped": total_skipped,
                "failed_pages": all_failed_pages,
                "complete": complete,
                "workers": workers,
                "running": False,
                "total_expected": total_pages,
            },
        )

    if complete:
        mark_done_fn(bib_code)

    logger.info("Hi-res paralelo concluído: %s — %s processadas, %s falhas", bib_code, total_processed, len(all_failed_pages))
    return {bib_code: total_processed} if complete else {}


def processar_todos_hires(
    *,
    headless: bool,
    force: bool,
    bib: str | None,
    workers: int,
    max_pages: int,
    keep_images: bool,
    load_progress_fn,
    processar_com_driver_fn,
    processar_acervo_paralelo_fn,
    mark_done_fn,
    set_bib_stats_fn=None,
    unmark_done_fn,
    create_driver_fn,
):
    if bib:
        acervos = [{"bib": bib, "nome": get_acervo_nome(bib)}]
    else:
        cache_file = CACHE_DIR / "acervos_pe.json"
        if not cache_file.exists():
            logger.error("Cache de acervos não encontrado. Execute 'listar' primeiro.")
            return {}
        with open(cache_file, encoding="utf-8") as fh:
            acervos = json.load(fh)

    progress = load_progress_fn()
    done = set(progress.get("done", []))
    failed_pages = progress.get("failed_pages", {})

    pendentes = []
    for acervo in acervos:
        bib_code = acervo["bib"]
        has_failed_pages = bool(failed_pages.get(bib_code))
        if force or bib_code not in done or has_failed_pages:
            pendentes.append(acervo)

    logger.info(
        "Hi-res pipeline: %s acervos | %s concluídos | %s pendentes | %s workers",
        len(acervos),
        len(done),
        len(pendentes),
        workers,
    )

    if not pendentes:
        logger.info("Nenhum acervo pendente.")
        return {}

    if len(pendentes) == 1 and workers > 1:
        return processar_acervo_paralelo_fn(
            pendentes[0],
            workers=workers,
            headless=headless,
            force=force,
            max_pages=max_pages,
            keep_images=keep_images,
            processar_com_driver_fn=processar_com_driver_fn,
            load_progress_fn=load_progress_fn,
            mark_done_fn=mark_done_fn,
            set_bib_stats_fn=set_bib_stats_fn,
            create_driver_fn=create_driver_fn,
        )

    queue = list(pendentes)
    queue_lock = Lock()

    def safe_pop():
        with queue_lock:
            if queue:
                return queue.pop(0)
            return None

    def worker_fn(worker_id):
        driver = create_driver_fn(headless=headless)
        try:
            while True:
                acervo = safe_pop()
                if acervo is None:
                    break

                bib_code = acervo["bib"]
                nome = acervo["nome"]
                logger.info("Worker %s: processando %s (%s)", worker_id, nome, bib_code)

                txt_dir = TEXT_DIR / bib_code
                txt_dir.mkdir(parents=True, exist_ok=True)

                try:
                    result = processar_com_driver_fn(
                        driver,
                        bib_code,
                        nome,
                        txt_dir,
                        force=force,
                        max_pages=max_pages,
                        keep_images=keep_images,
                    )
                    if result["complete"]:
                        mark_done_fn(bib_code)
                        logger.info("Worker %s: ✓ %s — %s páginas (completo)", worker_id, nome, result["processed"])
                    else:
                        unmark_done_fn(bib_code)
                        logger.error(
                            "Worker %s: ✗ %s incompleto — %s processadas, %s falhas pendentes",
                            worker_id,
                            nome,
                            result["processed"],
                            len(result["failed_pages"]),
                        )
                except Exception as exc:
                    logger.error("Worker %s: ERRO em %s: %s", worker_id, bib_code, exc)
                    unmark_done_fn(bib_code)
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = create_driver_fn(headless=headless)
        finally:
            try:
                driver.quit()
            except Exception:
                pass
            logger.info("Worker %s: finalizado", worker_id)

    threads = []
    for index in range(min(workers, len(pendentes))):
        thread = Thread(target=worker_fn, args=(index + 1,), daemon=True)
        thread.start()
        threads.append(thread)
        time.sleep(3)

    for thread in threads:
        thread.join()

    final_progress = load_progress_fn()
    final_done = set(final_progress.get("done", []))
    new_done = final_done - done

    logger.info("Hi-res pipeline concluído: %s acervos processados", len(new_done))
    return {bib: 1 for bib in new_done}
