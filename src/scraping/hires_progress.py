"""Persistência de progresso do pipeline hi-res."""

from __future__ import annotations

import json
from pathlib import Path


def default_progress() -> dict:
    return {
        "done": [],
        "failed_pages": {},
        "stats": {},
    }


def load_progress(progress_file: Path) -> dict:
    if not progress_file.exists():
        return default_progress()

    try:
        with open(progress_file, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return default_progress()

    if not isinstance(data, dict):
        return default_progress()

    data.setdefault("done", [])
    data.setdefault("failed_pages", {})
    data.setdefault("stats", {})
    return data


def save_progress(progress_file: Path, data: dict) -> None:
    tmp = progress_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(progress_file)


def update_failed_page(progress_file: Path, lock, bib: str, page_num: int, *, failed: bool) -> None:
    with lock:
        data = load_progress(progress_file)
        pages = set(data["failed_pages"].get(bib, []))
        if failed:
            pages.add(page_num)
        elif page_num in pages:
            pages.remove(page_num)

        if pages:
            data["failed_pages"][bib] = sorted(pages)
        else:
            data["failed_pages"].pop(bib, None)

        save_progress(progress_file, data)


def set_bib_stats(progress_file: Path, lock, bib: str, stats: dict) -> None:
    with lock:
        data = load_progress(progress_file)
        data["stats"][bib] = stats
        save_progress(progress_file, data)


def mark_done(progress_file: Path, lock, bib: str, *, done: bool) -> None:
    with lock:
        data = load_progress(progress_file)
        if done:
            if bib not in data["done"]:
                data["done"].append(bib)
                data["done"] = sorted(set(data["done"]))
            data["failed_pages"].pop(bib, None)
        else:
            data["done"] = [item for item in data["done"] if item != bib]
        save_progress(progress_file, data)
