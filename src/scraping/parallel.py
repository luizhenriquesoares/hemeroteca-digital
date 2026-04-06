"""Pipeline paralelo: múltiplos Chrome drivers processando acervos simultaneamente."""

import json
import logging
import multiprocessing as mp
import signal
import time
from pathlib import Path

from src.config import CACHE_DIR

logger = logging.getLogger(__name__)

PROGRESS_FILE = CACHE_DIR / "pipeline_progress.json"


def _load_progress():
    """Carrega progresso do pipeline."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"done": [], "failed": []}


def _save_progress(progress):
    """Salva progresso do pipeline (escrita atômica via rename)."""
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(progress, f, indent=2)
    tmp.replace(PROGRESS_FILE)


def _worker_process(worker_id: int, task_queue: mp.Queue, result_queue: mp.Queue,
                    max_pages: int, headless: bool, capture_only: bool = False):
    """
    Processo worker: cria seu próprio Chrome e processa acervos da fila.

    Se capture_only=True, faz apenas download de imagens (OCR/chunking depois em batch).
    """
    # Ignorar SIGINT nos workers (processo principal trata)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Imports dentro do worker para evitar problemas com multiprocessing spawn
    from src.scraping.driver import create_driver
    from src.scraping.scraper import scrape_jornal

    # Configurar logging do worker
    logging.basicConfig(
        level=logging.INFO,
        format=f"[W{worker_id}] %(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(
                CACHE_DIR.parent.parent / "logs" / f"worker_{worker_id}.log",
                encoding="utf-8",
            ),
        ],
    )
    wlog = logging.getLogger(f"worker-{worker_id}")

    driver = None
    try:
        driver = create_driver(headless=headless)
        wlog.info("Driver Chrome criado")
    except Exception as e:
        result_queue.put({
            "worker": worker_id, "bib": "", "nome": "",
            "status": "fatal", "error": f"Falha ao criar driver: {e}",
        })
        return

    try:
        while True:
            try:
                acervo = task_queue.get(timeout=5)
            except Exception:
                break

            if acervo is None:  # Poison pill
                break

            bib = acervo["bib"]
            nome = acervo["nome"]

            try:
                # 1. Captura (download de imagens)
                wlog.info(f"Capturando {nome} ({bib})...")
                pages = scrape_jornal(driver, bib, nome, max_pages=max_pages)
                page_count = len(pages)
                wlog.info(f"  {page_count} páginas capturadas")

                ocr_count = 0
                chunk_count = 0

                if not capture_only:
                    from src.processing.ocr import processar_acervo
                    from src.processing.chunker import criar_chunks_acervo
                    wlog.info(f"  OCR...")
                    ocr_count = processar_acervo(bib)
                    wlog.info(f"  Chunking...")
                    chunk_count = criar_chunks_acervo(bib)

                result_queue.put({
                    "worker": worker_id,
                    "bib": bib,
                    "nome": nome,
                    "status": "done",
                    "pages": page_count,
                    "ocr": ocr_count,
                    "chunks": chunk_count,
                })

            except Exception as e:
                wlog.error(f"Erro em {bib}: {e}", exc_info=True)
                result_queue.put({
                    "worker": worker_id,
                    "bib": bib,
                    "nome": nome,
                    "status": "failed",
                    "error": str(e),
                })
                # Recriar driver em caso de erro
                try:
                    driver.quit()
                except Exception:
                    pass
                try:
                    driver = create_driver(headless=headless)
                    wlog.info("Driver recriado após erro")
                except Exception as e2:
                    wlog.error(f"Falha ao recriar driver: {e2}")
                    result_queue.put({
                        "worker": worker_id, "bib": "", "nome": "",
                        "status": "fatal", "error": f"Driver irrecuperável: {e2}",
                    })
                    return
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        wlog.info("Worker finalizado")


def run_parallel_pipeline(acervos: list[dict], num_workers: int = 4,
                          max_pages: int = 0, headless: bool = True,
                          skip_indexing: bool = False,
                          capture_only: bool = False) -> dict:
    """
    Executa pipeline em paralelo com N Chrome drivers.

    Etapas paralelas (por worker): Captura -> OCR -> Chunking
    Etapa final (sequencial):      Indexação no ChromaDB

    Args:
        acervos: lista de {"bib": ..., "nome": ...}
        num_workers: número de workers paralelos
        max_pages: limite de páginas por acervo (0 = sem limite)
        headless: modo headless do Chrome
        skip_indexing: pular indexação final

    Returns:
        dict com progresso {"done": [...], "failed": [...]}
    """
    progress = _load_progress()
    done_set = set(progress["done"])

    pendentes = [a for a in acervos if a["bib"] not in done_set]

    if not pendentes:
        print(f"Todos os {len(acervos)} acervos já foram processados!")
        return progress

    actual_workers = min(num_workers, len(pendentes))

    print(f"\n{'='*60}")
    print(f"  PIPELINE PARALELO - {actual_workers} workers")
    print(f"  Total: {len(acervos)} | Concluídos: {len(done_set)} | Pendentes: {len(pendentes)}")
    print(f"{'='*60}\n")

    task_queue = mp.Queue()
    result_queue = mp.Queue()

    # Preencher fila de tarefas
    for acervo in pendentes:
        task_queue.put(acervo)

    # Poison pills
    for _ in range(actual_workers):
        task_queue.put(None)

    # Iniciar workers
    workers = []
    for i in range(actual_workers):
        p = mp.Process(
            target=_worker_process,
            args=(i, task_queue, result_queue, max_pages, headless, capture_only),
            name=f"worker-{i}",
        )
        p.start()
        workers.append(p)
        print(f"  Worker {i} iniciado (PID {p.pid})")

    print()

    # Coletar resultados
    completed = 0
    failed_count = 0
    start_time = time.time()
    active_workers = set(range(actual_workers))

    try:
        while active_workers or not result_queue.empty():
            # Verificar workers ainda vivos
            for i, w in enumerate(workers):
                if i in active_workers and not w.is_alive():
                    active_workers.discard(i)

            if not active_workers and result_queue.empty():
                break

            try:
                result = result_queue.get(timeout=5)
            except Exception:
                continue

            if result["status"] == "done":
                progress["done"].append(result["bib"])
                done_set.add(result["bib"])
                completed += 1
                _save_progress(progress)

                elapsed = time.time() - start_time
                rate = completed / (elapsed / 60) if elapsed > 0 else 0
                remaining = len(pendentes) - completed - failed_count
                eta_min = remaining / rate if rate > 0 else 0

                print(
                    f"  [W{result['worker']}] OK {result['nome'][:35]:35s} "
                    f"({result['bib']}) "
                    f"{result['pages']:4d} pgs {result['chunks']:4d} chunks "
                    f"[{completed + failed_count}/{len(pendentes)}] "
                    f"~{eta_min:.0f}min"
                )

            elif result["status"] == "failed":
                if result["bib"] and result["bib"] not in progress["failed"]:
                    progress["failed"].append(result["bib"])
                failed_count += 1
                _save_progress(progress)
                print(
                    f"  [W{result['worker']}] ERRO {result.get('nome', '?')[:35]:35s} "
                    f"({result.get('bib', '?')}) "
                    f"{result.get('error', '?')[:60]}"
                )

            elif result["status"] == "fatal":
                w_id = result["worker"]
                active_workers.discard(w_id)
                print(f"  [W{w_id}] FATAL: {result.get('error', '?')}")

    except KeyboardInterrupt:
        print("\n\nInterrompido! Salvando progresso...")
        _save_progress(progress)
        for w in workers:
            w.terminate()

    # Aguardar workers
    for w in workers:
        w.join(timeout=30)
        if w.is_alive():
            w.terminate()

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"  Captura + OCR + Chunking concluídos em {elapsed/60:.1f} minutos")
    print(f"  {completed} OK | {failed_count} erros | {len(pendentes) - completed - failed_count} restantes")
    print(f"{'='*60}")

    # Indexação final (sequencial, evita contention no SQLite do ChromaDB)
    if not skip_indexing and completed > 0:
        print("\nIndexando no ChromaDB...")
        from src.processing.indexer import indexar_todos
        idx_stats = indexar_todos()
        total_indexed = sum(idx_stats.values())
        print(f"  {total_indexed} chunks indexados no total")

    return progress
