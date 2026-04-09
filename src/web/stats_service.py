"""Serviços de estatísticas e acervos da camada web."""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def build_stats(*, images_dir, cache_dir, count_page_texts_fn, load_progress_status_fn):
    img_count = sum(1 for _ in images_dir.rglob("*.jpg")) if images_dir.exists() else 0
    txt_count = count_page_texts_fn()

    chunks_indexados = 0
    try:
        try:
            from src.indexer import stats as idx_stats
        except Exception:
            from src.processing.indexer import stats as idx_stats
        chunks_indexados = idx_stats()["total_chunks"]
    except Exception as exc:
        logger.debug("Indexer stats não disponível: %s", exc)
    if chunks_indexados == 0:
        # Fallback: contar linhas nos arquivos chunks.jsonl
        chunks_dir = cache_dir.parent / "chunks"
        if chunks_dir.exists():
            for f in chunks_dir.rglob("chunks.jsonl"):
                try:
                    with open(f) as fh:
                        chunks_indexados += sum(1 for _ in fh)
                except Exception as exc:
                    logger.warning("Erro ao contar chunks em %s: %s", f, exc)

    done, failed = 0, 0
    try:
        done, failed = load_progress_status_fn()
    except Exception as exc:
        logger.warning("Erro ao carregar status de progresso: %s", exc)

    acervos_file = cache_dir / "acervos_pe.json"
    total_acervos = 0
    total_paginas = 0
    if acervos_file.exists():
        try:
            with open(acervos_file, encoding="utf-8") as fh:
                acervos = json.load(fh)
            total_acervos = len(acervos)
            total_paginas = sum(acervo.get("paginas", 0) for acervo in acervos)
        except Exception as exc:
            logger.warning("Erro ao carregar acervos_pe.json: %s", exc)

    paginas_processadas = max(img_count, txt_count)
    return {
        "acervos_total": total_acervos,
        "acervos_concluidos": done,
        "acervos_falhas": failed,
        "paginas_total": total_paginas,
        "imagens": img_count,
        "paginas_processadas": paginas_processadas,
        "textos_ocr": txt_count,
        "chunks_indexados": chunks_indexados,
        "progresso_pct": round(paginas_processadas * 100 / total_paginas, 1) if total_paginas > 0 else 0,
    }


def load_acervos(cache_dir) -> list[dict]:
    acervos_file = cache_dir / "acervos_pe.json"
    if not acervos_file.exists():
        return []

    with open(acervos_file, encoding="utf-8") as fh:
        acervos = json.load(fh)

    return [
        {"bib": acervo["bib"], "nome": acervo["nome"], "paginas": acervo.get("paginas", 0)}
        for acervo in sorted(acervos, key=lambda item: item["nome"])
    ]
