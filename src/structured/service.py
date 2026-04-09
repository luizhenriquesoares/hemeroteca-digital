"""Serviço de orquestração da camada estruturada antes do grafo."""

from __future__ import annotations

from pathlib import Path

from src.structured.entities import extract_entities, normalize_name
from src.structured.identity import resolve_entity_identity, resolve_relation_entity_id
from src.structured.repository import StructuredRepository
from src.structured.relations import extract_relations
from src.structured.models import ExtractionBundle, PageReference
from src.config import IMAGES_DIR, TEXT_DIR


def _resolve_page_reference(metadata: dict) -> PageReference:
    bib = metadata.get("bib", "?")
    pagina = str(metadata.get("pagina", "?"))
    text_path = TEXT_DIR / bib / f"{pagina}.txt"
    image_path = IMAGES_DIR / bib / f"{pagina}.jpg"
    return PageReference(
        bib=bib,
        pagina=pagina,
        jornal=metadata.get("jornal") or metadata.get("periodico") or "?",
        ano=str(metadata.get("ano", "?")),
        edicao=str(metadata.get("edicao", "?")),
        text_path=str(text_path) if text_path.exists() else None,
        image_path=str(image_path) if image_path.exists() else None,
    )


def _extract_snippet(text: str, needle: str, radius: int = 110) -> str:
    if not text:
        return ""
    lower_text = text.lower()
    lower_needle = (needle or "").lower()
    pos = lower_text.find(lower_needle) if lower_needle else -1
    if pos == -1:
        compact = " ".join(text.split())
        return compact[: radius * 2].strip()
    start = max(0, pos - radius)
    end = min(len(text), pos + len(needle) + radius)
    snippet = " ".join(text[start:end].split())
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    return snippet


def _is_legible_chunk(text: str) -> bool:
    """Verifica se o chunk tem texto suficientemente legível para extração.

    Filtra apenas chunks longos com alta proporção de lixo OCR.
    Chunks curtos passam direto — a filtragem fina é feita na extração.
    """
    if not text or len(text.strip()) < 10:
        return False
    words = text.split()
    # Chunks com menos de 30 palavras passam direto
    if len(words) < 30:
        return True
    # Para chunks longos, exigir proporção mínima de palavras reais
    real_words = sum(
        1 for w in words
        if len(w) >= 3 and any(c in w.lower() for c in "aeiouáàâãéêíóôõú")
    )
    return (real_words / len(words)) >= 0.35


def extract_from_chunk(chunk: dict) -> ExtractionBundle:
    page = _resolve_page_reference(chunk.get("metadata", {}))
    text = chunk.get("text", "")
    if not _is_legible_chunk(text):
        return ExtractionBundle(page=page, entities=[], relations=[], source_text=text)
    entities = extract_entities(text)
    relations = extract_relations(text, entities)
    return ExtractionBundle(page=page, entities=entities, relations=relations, source_text=text)


def process_chunk(chunk: dict, repository: StructuredRepository) -> dict:
    bundle = extract_from_chunk(chunk)
    page_id = repository.upsert_page(bundle.page)
    entity_candidates: dict[str, list[dict]] = {}
    mentions = 0

    for entity in bundle.entities:
        identity = resolve_entity_identity(entity, bundle.page, bundle.entities)
        attributes = {
            **(entity.attributes or {}),
            "base_normalized_name": entity.normalized_name,
            "identity_key": identity.identity_key,
            "identity_status": identity.status,
            "identity_hints": list(identity.hints),
            "identity_confidence": identity.confidence,
        }
        entity_id = repository.upsert_entity(
            entity_type=entity.entity_type,
            canonical_name=entity.canonical_name,
            normalized_name=identity.identity_key,
            base_normalized_name=entity.normalized_name,
            aliases=entity.aliases,
            attributes=attributes,
            year=bundle.page.ano if bundle.page.ano not in {"", "?"} else "",
        )
        entity_candidates.setdefault(entity.normalized_name, []).append(
            {
                "entity_id": entity_id,
                "base_name": entity.normalized_name,
                "canonical_name": entity.canonical_name,
                "status": identity.status,
                "confidence": identity.confidence,
            }
        )
        repository.add_mention(
            entity_id=entity_id,
            page_id=page_id,
            chunk_id=chunk["id"],
            surface_form=entity.surface_form,
            snippet=_extract_snippet(bundle.source_text, entity.surface_form or entity.canonical_name),
            confidence=entity.confidence,
            source_text=bundle.source_text,
        )
        mentions += 1

    relation_count = 0
    for relation in bundle.relations:
        subject_entity_id = resolve_relation_entity_id(
            relation.subject_name,
            entity_candidates.get(normalize_name(relation.subject_name), []),
        )
        if not subject_entity_id:
            continue
        object_entity_id = None
        if relation.object_name:
            object_entity_id = resolve_relation_entity_id(
                relation.object_name,
                entity_candidates.get(normalize_name(relation.object_name), []),
            )
        if object_entity_id is None and relation.object_literal:
            object_entity_id = resolve_relation_entity_id(
                relation.object_literal,
                entity_candidates.get(normalize_name(relation.object_literal), []),
            )
        relation_id = repository.upsert_relation(
            subject_entity_id=subject_entity_id,
            predicate=relation.predicate,
            object_entity_id=object_entity_id,
            object_literal=relation.object_literal,
            confidence=relation.confidence,
            status=relation.status,
            extraction_method=relation.extraction_method,
        )
        repository.add_relation_evidence(
            relation_id=relation_id,
            page_id=page_id,
            chunk_id=chunk["id"],
            quote=relation.evidence_quote or relation.subject_name,
            confidence=relation.confidence,
        )
        relation_count += 1

    return {
        "page_id": page_id,
        "entities": len(bundle.entities),
        "mentions": mentions,
        "relations": relation_count,
    }


def process_bib(bib: str, repository: StructuredRepository | None = None) -> dict:
    from src.processing.chunker import carregar_chunks

    repo = repository or StructuredRepository()
    chunks = carregar_chunks(bib)
    summary = {"chunks": 0, "entities": 0, "mentions": 0, "relations": 0}
    for chunk in chunks:
        result = process_chunk(chunk, repo)
        summary["chunks"] += 1
        summary["entities"] += result["entities"]
        summary["mentions"] += result["mentions"]
        summary["relations"] += result["relations"]
    return summary


def process_all(repository: StructuredRepository | None = None) -> dict[str, dict]:
    repo = repository or StructuredRepository()
    stats: dict[str, dict] = {}
    for bib_dir in sorted(Path(TEXT_DIR).iterdir()):
        if not bib_dir.is_dir():
            continue
        stats[bib_dir.name] = process_bib(bib_dir.name, repository=repo)
    # Rebuild cache de stats para queries rápidas
    repo.rebuild_entity_stats_cache()
    return stats
