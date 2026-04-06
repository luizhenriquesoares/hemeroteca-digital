"""Helpers utilitários do CLI."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json

from src.cli.context import console


def run_parallel_file_jobs(arquivos, worker_fn, workers: int) -> tuple[int, int]:
    processados = 0
    falhas = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker_fn, arquivo): arquivo for arquivo in arquivos}
        for fut in as_completed(futures):
            arquivo = futures[fut]
            try:
                ok = fut.result()
                if ok:
                    processados += 1
                else:
                    falhas += 1
                total_done = processados + falhas
                if total_done % 10 == 0 or total_done == len(arquivos):
                    console.print(f"  [{total_done}/{len(arquivos)}] ok={processados} falhas={falhas}")
            except Exception as exc:
                falhas += 1
                console.print(f"  [red]ERRO {arquivo.name}: {exc}[/red]")

    return processados, falhas


def infer_saved_paths_from_image(image_path: Path) -> tuple[Path | None, Path | None]:
    saved = Path("data/text") / image_path.parent.name / f"{image_path.stem}.txt"
    corrected = Path("data/text") / image_path.parent.name / f"{image_path.stem}_corrigido.txt"
    return (saved if saved.exists() else None, corrected if corrected.exists() else None)


def load_json(path: Path, default):
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return default


def save_json(path: Path, payload) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
