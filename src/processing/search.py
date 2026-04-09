"""Busca híbrida com normalização histórica e reranqueamento."""

from __future__ import annotations

from src.processing.search_docs import (
    load_acervo_cache as _load_acervo_cache_impl,
    load_chunk_docs as _load_chunk_docs_impl,
    load_page_docs as _load_page_docs_impl,
    load_search_docs as _load_search_docs_impl,
)
from src.processing.search_profile import (
    QueryProfile,
    build_query_profile,
    expand_query_variants,
    focus_query,
    normalize_text,
    strip_accents,
    tokenize_significant,
)
from src.processing.search_scoring import (
    compactness_bonus as _compactness_bonus,
    best_token_match as _best_token_match,
    extract_evidence_snippet,
    ordered_ratio as _ordered_ratio,
    score_text as _score_text,
    similarity as _similarity,
)


def _load_acervo_cache() -> dict[str, dict]:
    return _load_acervo_cache_impl()


def _enrich_metadata(metadata: dict) -> dict:
    enriched = dict(metadata or {})
    bib = enriched.get("bib")
    acervo = _load_acervo_cache().get(bib, {}) if bib else {}
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


def _load_chunk_docs(filtro_bib: str | None = None) -> list[dict]:
    return _load_chunk_docs_impl(filtro_bib, enrich_metadata_fn=_enrich_metadata)


def _load_page_docs(filtro_bib: str | None = None) -> list[dict]:
    return _load_page_docs_impl(filtro_bib, enrich_metadata_fn=_enrich_metadata)


def _load_search_docs(filtro_bib: str | None = None) -> list[dict]:
    return _load_search_docs_impl(
        filtro_bib,
        load_chunk_docs_fn=_load_chunk_docs,
        load_page_docs_fn=_load_page_docs,
    )


def buscar_textual_historica(query: str, n_results: int = 10, filtro_bib: str | None = None) -> list[dict]:
    profile = build_query_profile(query)
    docs = _load_search_docs(filtro_bib)
    results = []

    for doc in docs:
        score, matched_tokens = _score_text(profile, doc["texto"])
        if score <= 0:
            continue
        results.append(
            {
                "id": doc["id"],
                "texto": doc["texto"],
                "metadata": doc.get("metadata", {}),
                "score": round(score, 3),
                "modo": "textual_historico",
                "matched_tokens": matched_tokens,
            }
        )

    results.sort(
        key=lambda item: (
            item["score"],
            len(item.get("matched_tokens", [])),
            len(item["texto"]),
        ),
        reverse=True,
    )
    return results[:n_results]


def buscar_semantica(query: str, n_results: int = 10, filtro_bib: str | None = None) -> list[dict]:
    from src.processing.indexer import buscar as buscar_vetorial

    raw_results = buscar_vetorial(query, n_results=n_results, filtro_bib=filtro_bib)
    return [
        {
            "id": item["id"],
            "texto": item["texto"],
            "metadata": item["metadata"],
            "score": round(max(0.0, 1 - item["distancia"]), 3),
            "modo": "semantica",
        }
        for item in raw_results
    ]


def buscar_hibrida(query: str, n_results: int = 10, filtro_bib: str | None = None) -> list[dict]:
    text_results = buscar_textual_historica(query, n_results=max(20, n_results * 3), filtro_bib=filtro_bib)

    try:
        semantic_results = buscar_semantica(query, n_results=max(20, n_results * 3), filtro_bib=filtro_bib)
    except Exception:
        semantic_results = []

    merged = {}
    for item in text_results:
        merged[item["id"]] = {
            **item,
            "text_score": item["score"],
            "semantic_score": 0.0,
        }

    for item in semantic_results:
        if item["id"] in merged:
            merged[item["id"]]["semantic_score"] = item["score"]
            merged[item["id"]]["score"] = min(
                1.0,
                max(merged[item["id"]]["text_score"], item["score"])
                + 0.12 * min(merged[item["id"]]["text_score"], item["score"])
                + 0.05,
            )
            merged[item["id"]]["modo"] = "hibrida"
        else:
            merged[item["id"]] = {
                **item,
                "text_score": 0.0,
                "semantic_score": item["score"],
            }

    output = sorted(
        merged.values(),
        key=lambda item: (
            item["score"],
            item.get("text_score", 0.0),
            item.get("semantic_score", 0.0),
        ),
        reverse=True,
    )
    return output[:n_results]
