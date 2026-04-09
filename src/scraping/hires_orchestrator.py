"""Orquestração paralela do pipeline hi-res."""

from __future__ import annotations

import json
import logging
import time
from threading import Lock, Thread

from src.config import CACHE_DIR, TEXT_DIR

logger = logging.getLogger(__name__)


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

    chunk_size = total_pages // workers
    ranges = []
    for index in range(workers):
        start = 1 + index * chunk_size
        end = start + chunk_size - 1 if index < workers - 1 else total_pages
        ranges.append((start, end))

    logger.info("Hi-res paralelo: %s (%s páginas) | %s workers | ranges: %s", bib_code, total_pages, workers, ranges)

    results = [None] * workers

    def worker_fn(worker_id, start_page, end_page):
        driver = create_driver_fn(headless=headless)
        try:
            logger.info("Worker %s: %s páginas %s-%s", worker_id, bib_code, start_page, end_page)
            result = processar_com_driver_fn(
                driver,
                bib_code,
                nome,
                txt_dir,
                force=force,
                max_pages=max_pages,
                keep_images=keep_images,
                start_page=start_page,
                end_page=end_page,
            )
            results[worker_id - 1] = result
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
            logger.error("Worker %s: ERRO em %s p%s-%s: %s", worker_id, bib_code, start_page, end_page, exc)
            results[worker_id - 1] = {"processed": 0, "failed_pages": [f"error:{exc}"], "complete": False}
        finally:
            try:
                driver.quit()
            except Exception:
                pass
            logger.info("Worker %s: finalizado", worker_id)

    threads = []
    for index, (start, end) in enumerate(ranges):
        thread = Thread(target=worker_fn, args=(index + 1, start, end), daemon=True)
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
        acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
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
