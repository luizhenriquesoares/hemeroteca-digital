"""Helpers de página, fonte e estatísticas da API web."""

from __future__ import annotations

import json
from pathlib import Path

def busca_textual(query: str, n: int, bib: str = None) -> dict:
    from src.processing.search import buscar_textual_historica

    resultados = buscar_textual_historica(query, n_results=n, filtro_bib=bib)
    for resultado in resultados:
        resultado["modo"] = "textual"
    return {"query": query, "total": len(resultados), "resultados": resultados}


def count_page_texts(text_dir: Path) -> int:
    if not text_dir.exists():
        return 0
    return sum(1 for path in text_dir.rglob("*.txt") if not path.name.endswith("_corrigido.txt"))


def load_progress_status(cache_dir: Path) -> tuple[int, int]:
    """Combina progresso do pipeline legado e do fluxo hi-res."""
    done = set()
    failed = set()

    pipeline_progress = cache_dir / "pipeline_progress.json"
    if pipeline_progress.exists():
        with open(pipeline_progress, encoding="utf-8") as fh:
            progress = json.load(fh)
        done.update(progress.get("done", []))
        failed.update(progress.get("failed", []))

    hires_progress = cache_dir / "hires_progress.json"
    if hires_progress.exists():
        with open(hires_progress, encoding="utf-8") as fh:
            progress = json.load(fh)
        done.update(progress.get("done", []))
        failed.update(progress.get("failed_pages", {}).keys())

    return len(done), len(failed - done)


def resolve_image_url(images_dir: Path, bib: str, pagina: str) -> str | None:
    image_path = images_dir / bib / f"{pagina}.jpg"
    if image_path.exists():
        return f"/images/{bib}/{pagina}.jpg"
    return None


def read_optional_text(path_str: str | None) -> str | None:
    if not path_str:
        return None
    path = Path(path_str)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def serialize_page_record(page: dict, images_dir: Path) -> dict:
    image_url = resolve_image_url(images_dir, page["bib"], page["pagina"])
    return {
        "bib": page["bib"],
        "pagina": page["pagina"],
        "jornal": page.get("jornal", "?"),
        "ano": page.get("ano", "?"),
        "edicao": page.get("edicao", "?"),
        "image_url": image_url,
        "text_path": page.get("text_path"),
        "image_path": page.get("image_path"),
        "ocr_text": read_optional_text(page.get("text_path")),
    }
