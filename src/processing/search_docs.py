"""Carregamento e enriquecimento de documentos para busca."""

from __future__ import annotations

import json

from src import config

_ACERVO_CACHE: dict[str, dict] | None = None


def load_chunk_docs(filtro_bib: str | None = None, enrich_metadata_fn=None) -> list[dict]:
    from src.processing.chunker import carregar_chunks

    enrich = enrich_metadata_fn or (lambda metadata: metadata)
    chunks = carregar_chunks(filtro_bib)
    return [
        {
            "id": chunk["id"],
            "texto": chunk["text"],
            "metadata": enrich(chunk.get("metadata", {})),
        }
        for chunk in chunks
    ]


def load_page_docs(filtro_bib: str | None = None, enrich_metadata_fn=None) -> list[dict]:
    enrich = enrich_metadata_fn or (lambda metadata: metadata)
    docs = []
    text_dir = config.TEXT_DIR
    if filtro_bib:
        search_dirs = [text_dir / filtro_bib]
    else:
        search_dirs = [directory for directory in text_dir.iterdir() if directory.is_dir()] if text_dir.exists() else []

    for acervo_dir in search_dirs:
        if not acervo_dir.exists():
            continue
        for txt_file in sorted(path for path in acervo_dir.glob("*.txt") if not path.name.endswith("_corrigido.txt")):
            corrigido = txt_file.parent / txt_file.name.replace(".txt", "_corrigido.txt")
            fonte = corrigido if corrigido.exists() else txt_file
            try:
                texto = fonte.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            docs.append(
                {
                    "id": f"{acervo_dir.name}_{txt_file.stem}",
                    "texto": texto,
                    "metadata": enrich(
                        {
                            "bib": acervo_dir.name,
                            "pagina": txt_file.stem,
                            "corrigido_llm": corrigido.exists(),
                        }
                    ),
                }
            )
    return docs


def load_acervo_cache() -> dict[str, dict]:
    global _ACERVO_CACHE
    if _ACERVO_CACHE is not None:
        return _ACERVO_CACHE

    acervos_file = config.CACHE_DIR / "acervos_pe.json"
    if not acervos_file.exists():
        _ACERVO_CACHE = {}
        return _ACERVO_CACHE

    try:
        with open(acervos_file, encoding="utf-8") as fh:
            acervos = json.load(fh)
    except Exception:
        _ACERVO_CACHE = {}
        return _ACERVO_CACHE

    _ACERVO_CACHE = {
        item["bib"]: item
        for item in acervos
        if isinstance(item, dict) and item.get("bib")
    }
    return _ACERVO_CACHE


def enrich_metadata(metadata: dict) -> dict:
    enriched = dict(metadata or {})
    bib = enriched.get("bib")
    acervo = load_acervo_cache().get(bib, {}) if bib else {}
    if acervo:
        current_jornal = str(enriched.get("jornal") or "").strip()
        current_periodico = str(enriched.get("periodico") or "").strip()
        generic = {"", "?", f"Acervo {bib}"}
        if current_jornal in generic:
            enriched["jornal"] = acervo.get("nome")
        else:
            enriched.setdefault("jornal", acervo.get("nome"))
        if current_periodico in generic:
            enriched["periodico"] = acervo.get("nome")
        else:
            enriched.setdefault("periodico", acervo.get("nome"))
    return enriched


def load_search_docs(filtro_bib: str | None = None, *, load_chunk_docs_fn=None, load_page_docs_fn=None) -> list[dict]:
    chunk_loader = load_chunk_docs_fn or load_chunk_docs
    page_loader = load_page_docs_fn or load_page_docs
    try:
        docs = chunk_loader(filtro_bib)
        if docs:
            return docs
    except Exception:
        pass
    return page_loader(filtro_bib)
