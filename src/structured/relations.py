"""Extração heurística de relações históricas com evidência textual."""

from __future__ import annotations

import re

from src.structured.entities import canonicalize_person_name, normalize_name
from src.structured.models import ExtractedEntity, ExtractedRelation

INSTITUTION_MARKERS = {
    "associacao", "associação", "companhia", "sociedade", "instituto",
    "camara", "câmara", "igreja", "alfandega", "alfândega", "commercio",
    "commercial", "partido", "junta", "tribunal",
}

NAME_PATTERN = (
    r"(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)"
    r"(?:\s+(?:(?:de|da|do|das|dos|e|d['’])\s+)?(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)){1,6}"
)

RELATION_PATTERNS = [
    (
        "spouse_of",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:e sua esposa|E sua esposa|casado com|Casado com|consorte de|Consorte de)\s+(?P<object>{NAME_PATTERN})",
        ),
        0.88,
        "probable",
    ),
    (
        "child_of",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:filho|filha|Filho|Filha)\s+(?:leg[ií]tim[oa]\s+)?de\s+(?P<object>{NAME_PATTERN})",
        ),
        0.92,
        "probable",
    ),
    (
        "parent_of",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:pai|m[aã]e|Pai|M[aã]e)\s+de\s+(?P<object>{NAME_PATTERN})",
        ),
        0.90,
        "probable",
    ),
    (
        "widow_of",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:vi[uú]va)\s+de\s+(?P<object>{NAME_PATTERN})",
        ),
        0.83,
        "hypothesis",
    ),
    (
        "member_of",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:s[óo]cio|membro|integrante|presidente|secret[aá]rio)\s+(?:do|da)\s+(?P<object>[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][^,;\n]{{4,80}})",
        ),
        0.82,
        "probable",
    ),
]


def _find_entity_name(raw_name: str, entities: list[ExtractedEntity]) -> str:
    target = normalize_name(raw_name)
    for entity in entities:
        if entity.normalized_name == target:
            return entity.canonical_name
    canonical_name, _ = canonicalize_person_name(raw_name)
    return canonical_name


def extract_relations(text: str, entities: list[ExtractedEntity]) -> list[ExtractedRelation]:
    """Extrai relações explícitas e relações de cargo a partir das entidades."""
    relations: list[ExtractedRelation] = []
    seen = set()

    for entity in entities:
        if entity.role:
            key = (entity.canonical_name, "holds_role", "", entity.role)
            if key not in seen:
                seen.add(key)
                relations.append(
                    ExtractedRelation(
                        subject_name=entity.canonical_name,
                        predicate="holds_role",
                        object_literal=entity.role,
                        confidence=min(entity.confidence, 0.94),
                        status="probable",
                        evidence_quote=entity.surface_form,
                        extraction_method="title_heuristic",
                    )
                )

    for predicate, pattern, confidence, status in RELATION_PATTERNS:
        for match in pattern.finditer(text or ""):
            subject_name = _find_entity_name(match.group("subject"), entities)
            object_entity_name = ""
            object_literal = ""
            raw_object = match.group("object")
            if predicate in {"member_of", "resident_of"}:
                object_literal = raw_object.strip(" ,.;:")
            else:
                object_entity_name = _find_entity_name(raw_object, entities)

            key = (subject_name, predicate, object_entity_name, object_literal)
            if key in seen or subject_name == object_entity_name:
                continue
            seen.add(key)
            relations.append(
                ExtractedRelation(
                    subject_name=subject_name,
                    predicate=predicate,
                    object_name=object_entity_name,
                    object_literal=object_literal,
                    confidence=confidence,
                    status=status,
                    evidence_quote=match.group(0).strip(),
                    extraction_method="pattern_heuristic",
                )
            )

    _append_residence_relations(text, entities, relations, seen)
    _append_cooccurrence_relations(entities, relations, seen)
    return relations


def _append_residence_relations(
    text: str,
    entities: list[ExtractedEntity],
    relations: list[ExtractedRelation],
    seen: set,
) -> None:
    place_pattern = r"(?P<place>[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][^,;\n]{2,60})"
    for entity in entities:
        if entity.entity_type != "person" or _looks_like_institution(entity.canonical_name):
            continue
        aliases = [entity.surface_form, entity.canonical_name, *entity.aliases]
        for alias in aliases:
            alias = alias.strip()
            if not alias:
                continue
            pattern = re.compile(
                rf"{re.escape(alias)}(?:[^.;\n]{{0,120}}?)\s*,\s*(?:morador|residente|domiciliado)\s+(?:na|no|em)\s+{place_pattern}"
            )
            match = pattern.search(text or "")
            if not match:
                continue
            object_literal = match.group("place").strip(" ,.;:")
            key = (entity.canonical_name, "resident_of", "", object_literal)
            if key in seen:
                continue
            seen.add(key)
            relations.append(
                ExtractedRelation(
                    subject_name=entity.canonical_name,
                    predicate="resident_of",
                    object_literal=object_literal,
                    confidence=0.78,
                    status="hypothesis",
                    evidence_quote=match.group(0).strip(),
                    extraction_method="pattern_heuristic",
                )
            )
            break


def _looks_like_institution(name: str) -> bool:
    normalized = normalize_name(name)
    return any(marker in normalized.split() for marker in INSTITUTION_MARKERS)


def _append_cooccurrence_relations(
    entities: list[ExtractedEntity],
    relations: list[ExtractedRelation],
    seen: set,
) -> None:
    people = [entity for entity in entities if entity.entity_type == "person"]
    for idx, entity in enumerate(people):
        for other in people[idx + 1:]:
            key = (entity.canonical_name, "mentioned_with", other.canonical_name, "")
            reverse = (other.canonical_name, "mentioned_with", entity.canonical_name, "")
            if key in seen or reverse in seen:
                continue
            seen.add(key)
            relations.append(
                ExtractedRelation(
                    subject_name=entity.canonical_name,
                    predicate="mentioned_with",
                    object_name=other.canonical_name,
                    confidence=0.58,
                    status="hypothesis",
                    evidence_quote=f"{entity.surface_form} / {other.surface_form}",
                    extraction_method="cooccurrence_heuristic",
                )
            )
