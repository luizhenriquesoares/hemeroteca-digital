"""Modelos de domínio para extração estruturada baseada em evidências."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PageReference:
    bib: str
    pagina: str
    jornal: str = "?"
    ano: str = "?"
    edicao: str = "?"
    text_path: str | None = None
    image_path: str | None = None


@dataclass(frozen=True)
class ExtractedEntity:
    entity_type: str
    canonical_name: str
    normalized_name: str
    surface_form: str
    aliases: tuple[str, ...] = ()
    confidence: float = 0.5
    role: str = ""
    attributes: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ExtractedRelation:
    subject_name: str
    predicate: str
    object_name: str = ""
    object_literal: str = ""
    confidence: float = 0.5
    status: str = "hypothesis"
    evidence_quote: str = ""
    extraction_method: str = "heuristic"


@dataclass(frozen=True)
class ExtractionBundle:
    page: PageReference
    entities: list[ExtractedEntity]
    relations: list[ExtractedRelation]
    source_text: str
