"""Enriquecimento seguro de metadados sem refazer OCR."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from src.config import TEXT_DIR

logger = logging.getLogger(__name__)


def _load_acervo_name_from_cache(bib: str) -> str | None:
    cache_file = Path("data/cache/acervos_pe.json")
    if not cache_file.exists():
        return None
    try:
        acervos = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    for acervo in acervos:
        if isinstance(acervo, dict) and acervo.get("bib") == bib and acervo.get("nome"):
            return acervo["nome"]
    return None


def _merge_metadata(existing: dict, fresh: dict) -> dict:
    merged = dict(existing or {})
    merged.update({k: v for k, v in fresh.items() if v not in (None, "")})
    return merged


def enrich_bib_metadata(bib: str, nome: str, driver) -> dict:
    """Atualiza apenas os .json das páginas que já têm .txt."""
    from src.scraping.hires_pipeline import _get_page_metadata, _proxima_pagina, _setup_acervo

    txt_dir = TEXT_DIR / bib
    if not txt_dir.exists():
        return {"updated": 0, "skipped": 0, "total": 0}

    page_txts = sorted(
        p for p in txt_dir.glob("*.txt")
        if not p.name.endswith("_corrigido.txt")
    )
    if not page_txts:
        return {"updated": 0, "skipped": 0, "total": 0}

    low_res_src, first_src = _setup_acervo(driver, bib, nome, start_page=1)
    if not low_res_src or not first_src:
        raise RuntimeError(f"Não foi possível abrir o acervo {bib} para enriquecer metadados")

    updated = 0
    skipped = 0
    expected_page = 1

    for txt_path in page_txts:
        try:
            current_page = int(txt_path.stem.rsplit("_", 1)[-1])
        except ValueError:
            skipped += 1
            continue

        while expected_page < current_page:
            _proxima_pagina(driver)
            expected_page += 1
            time.sleep(0.15)

        meta_path = txt_dir / f"{txt_path.stem}.json"
        existing = {}
        if meta_path.exists():
            try:
                existing = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                existing = {}

        fresh = _get_page_metadata(driver, bib, nome, current_page)
        merged = _merge_metadata(existing, fresh)
        meta_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        updated += 1

        _proxima_pagina(driver)
        expected_page = current_page + 1
        time.sleep(0.15)

    return {"updated": updated, "skipped": skipped, "total": len(page_txts)}


def enrich_metadata(headless: bool = True, bib: str | None = None) -> dict[str, dict]:
    from src.scraping.driver import create_driver

    if bib:
        acervos = [{"bib": bib, "nome": _load_acervo_name_from_cache(bib) or f"Acervo {bib}"}]
    else:
        cache_file = Path("data/cache/acervos_pe.json")
        if not cache_file.exists():
            raise RuntimeError("Cache de acervos não encontrado. Execute 'listar' primeiro.")
        acervos = json.loads(cache_file.read_text(encoding="utf-8"))

    results: dict[str, dict] = {}
    driver = create_driver(headless=headless)
    try:
        for acervo in acervos:
            bib_code = acervo["bib"]
            nome = acervo["nome"]
            logger.info(f"Enriquecendo metadados: {nome} ({bib_code})")
            results[bib_code] = enrich_bib_metadata(bib_code, nome, driver)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return results
