"""Helpers de página, fonte e estatísticas da API web."""

from __future__ import annotations

import json
from pathlib import Path

_ACERVO_CACHE: dict[str, dict] | None = None


def _load_acervo_cache(cache_dir: Path) -> dict[str, dict]:
    global _ACERVO_CACHE
    if _ACERVO_CACHE is not None:
        return _ACERVO_CACHE
    cache_file = cache_dir / "acervos_pe.json"
    if not cache_file.exists():
        _ACERVO_CACHE = {}
        return _ACERVO_CACHE
    try:
        acervos = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        _ACERVO_CACHE = {}
        return _ACERVO_CACHE
    _ACERVO_CACHE = {
        item["bib"]: item
        for item in acervos
        if isinstance(item, dict) and item.get("bib")
    }
    return _ACERVO_CACHE


def _is_generic_journal_name(value: str | None, bib: str) -> bool:
    if not value:
        return True
    normalized = value.strip().lower()
    return normalized in {"?", "", f"acervo {bib.lower()}"}


def _clean_editorial_label(value: str | None, prefix: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "?"
    lowered = text.lower()
    if lowered.startswith(prefix.lower() + " "):
        return text[len(prefix) + 1 :].strip() or text
    return text


def _resolve_editorial_name(cache_dir: Path, bib: str, jornal: str | None, periodico: str | None) -> str:
    if not _is_generic_journal_name(jornal, bib):
        return str(jornal).strip()
    if not _is_generic_journal_name(periodico, bib):
        return str(periodico).strip()
    acervo = _load_acervo_cache(cache_dir).get(bib, {})
    return acervo.get("nome") or jornal or periodico or "?"


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
    from src.config import CACHE_DIR, TEXT_DIR

    bib = page["bib"]
    pagina = page["pagina"]
    image_url = resolve_image_url(images_dir, bib, pagina)

    # Resolver texto: preferir corrigido > text_path > buscar no filesystem
    ocr_text = read_optional_text(page.get("text_path"))
    if not ocr_text:
        # Tentar buscar pelo formato padrão do filesystem
        page_num = f"{bib}_{int(pagina):05d}" if pagina.isdigit() else pagina
        corrigido = TEXT_DIR / bib / f"{page_num}_corrigido.txt"
        original = TEXT_DIR / bib / f"{page_num}.txt"
        if corrigido.exists():
            ocr_text = corrigido.read_text(encoding="utf-8")
        elif original.exists():
            ocr_text = original.read_text(encoding="utf-8")

    return {
        "bib": bib,
        "pagina": pagina,
        "jornal": _resolve_editorial_name(CACHE_DIR, bib, page.get("jornal"), page.get("periodico")),
        "ano": _clean_editorial_label(page.get("ano", "?"), "Ano"),
        "edicao": _clean_editorial_label(page.get("edicao", "?"), "Edição"),
        "image_url": image_url,
        "text_path": page.get("text_path"),
        "image_path": page.get("image_path"),
        "ocr_text": ocr_text,
    }
