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
    # --- Novos padrões ---
    (
        "appointed_to",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:nomeado|nomeada|nomeio|eleito|eleita)\s+(?:para\s+)?(?:o\s+cargo\s+de\s+|ao\s+cargo\s+de\s+|para\s+)?(?P<object>[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][^,;.\n]{{3,60}})",
        ),
        0.85,
        "probable",
    ),
    (
        "deceased",
        re.compile(
            rf"(?:faleceu|falleceo|falleceu)\s+(?:o|a|os|as)?\s*(?P<subject>{NAME_PATTERN})",
        ),
        0.80,
        "hypothesis",
    ),
    (
        "deceased_after",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:faleceu|falleceo|falleceu|falecido|falecida)",
        ),
        0.80,
        "hypothesis",
    ),
    (
        "traveled_to",
        re.compile(
            rf"(?P<subject>{NAME_PATTERN})\s*,?\s*(?:embarcou|partiu|chegou|desembarcou)\s+(?:no\s+porto\s+de\s+|para\s+|de\s+|em\s+)(?P<object>[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+(?:\s+(?:de|do|da)\s+[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+){{0,2}})",
        ),
        0.75,
        "hypothesis",
    ),
    (
        "signed_by",
        re.compile(
            rf"(?:assinado|assignado|firmado)\s+(?:por|pelo|pela)\s+(?P<subject>{NAME_PATTERN})",
        ),
        0.78,
        "hypothesis",
    ),
]

# Padrões que não capturam "object" (deceased, signed_by usam object_literal)
_NO_OBJECT_PREDICATES = {"deceased", "deceased_after", "signed_by"}


_TRUNCATED_RE = re.compile(r"[A-Z][a-z]{0,2}$|[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ]{1,3}$")


def _looks_truncated(name: str) -> bool:
    """Detecta nomes que parecem cortados pelo OCR (terminam em fragmento curto)."""
    parts = name.strip().split()
    if not parts:
        return True
    last = parts[-1]
    # Último token com 1-3 chars e começa com maiúscula = provavelmente truncado
    if len(last) <= 3 and last[0].isupper() and len(parts) >= 2:
        return True
    # Contém sequências sem sentido (consoantes sem vogais, > 3 chars)
    if len(last) >= 3 and not re.search(r"[aeiouáàâãéêíóôõú]", last, re.IGNORECASE):
        return True
    return False


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
            if _looks_truncated(entity.canonical_name):
                continue
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
            raw_subject = match.group("subject")

            if _looks_truncated(raw_subject):
                continue
            # Rejeitar subjects que começam com preposição (lugar, não pessoa)
            first_word = raw_subject.strip().split()[0].lower() if raw_subject.strip() else ""
            if first_word in {"na", "no", "em", "da", "do", "ao", "pela", "pelo"}:
                continue

            # Padrões sem grupo "object" (deceased, signed_by)
            has_object = predicate not in _NO_OBJECT_PREDICATES
            raw_object = ""
            if has_object:
                try:
                    raw_object = match.group("object")
                except IndexError:
                    continue
                if _looks_truncated(raw_object):
                    continue

            subject_name = _find_entity_name(raw_subject, entities)
            object_entity_name = ""
            object_literal = ""

            # Normalizar deceased_after → deceased
            effective_predicate = "deceased" if predicate == "deceased_after" else predicate

            if not has_object:
                # deceased/signed_by: sem objeto, apenas registrar o fato
                object_literal = effective_predicate.replace("_", " ")
            elif effective_predicate in {"member_of", "resident_of", "appointed_to", "traveled_to"}:
                object_literal = raw_object.strip(" ,.;:")
            else:
                object_entity_name = _find_entity_name(raw_object, entities)

            key = (subject_name, effective_predicate, object_entity_name, object_literal)
            if key in seen or (object_entity_name and subject_name == object_entity_name):
                continue
            seen.add(key)
            relations.append(
                ExtractedRelation(
                    subject_name=subject_name,
                    predicate=effective_predicate,
                    object_name=object_entity_name,
                    object_literal=object_literal,
                    confidence=confidence,
                    status=status,
                    evidence_quote=match.group(0).strip(),
                    extraction_method="pattern_heuristic",
                )
            )

    _append_residence_relations(text, entities, relations, seen)
    _append_ocr_tolerant_relations(text, entities, relations, seen)
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


def _append_ocr_tolerant_relations(
    text: str,
    entities: list[ExtractedEntity],
    relations: list[ExtractedRelation],
    seen: set,
) -> None:
    """Extrai relações biográficas com padrões tolerantes a OCR degradado.

    Em vez de exigir NAME_PATTERN (maiúscula-minúscula perfeitas), usa as
    entidades já extraídas como âncora e busca padrões biográficos ao redor.
    """
    if not text or not entities:
        return

    people = [e for e in entities if e.entity_type == "person"]
    if not people:
        return

    # Padrões OCR-tolerantes que buscam relação DEPOIS de uma entidade conhecida
    _ocr_patterns = [
        # filho/filha de NOME
        ("child_of", re.compile(
            r",?\s*filh[oa]\s+(?:leg[ií]tim[oa]\s+)?d[eao]\s+([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\s',\-]{3,50})",
            re.IGNORECASE,
        ), 0.88),
        # viuva de NOME
        ("widow_of", re.compile(
            r",?\s*vi[uú]va\s+d[eoa]\s+([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\s',\-]{3,50})",
            re.IGNORECASE,
        ), 0.80),
        # casado/casada com NOME
        ("spouse_of", re.compile(
            r",?\s*casad[oa]\s+com\s+([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\s',\-]{3,50})",
            re.IGNORECASE,
        ), 0.85),
        # nomeado para CARGO/FUNCAO
        ("appointed_to", re.compile(
            r",?\s*nomead[oa]\s+(?:para\s+)?(?:o\s+cargo\s+d[eao]\s+)?([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\s]{3,50})",
            re.IGNORECASE,
        ), 0.82),
    ]

    for entity in people:
        if _looks_truncated(entity.canonical_name):
            continue
        # Buscar o contexto ao redor de cada menção da entidade
        for alias in [entity.surface_form, entity.canonical_name]:
            alias = alias.strip()
            if not alias or len(alias) < 5:
                continue
            # Encontrar posições da entidade no texto
            search_start = 0
            while True:
                pos = text.lower().find(alias.lower(), search_start)
                if pos == -1:
                    break
                search_start = pos + 1
                # Contexto depois da menção (200 chars)
                after = text[pos + len(alias):pos + len(alias) + 200]

                for predicate, pattern, confidence in _ocr_patterns:
                    match = pattern.match(after)
                    if not match:
                        continue

                    raw_object = match.group(1).strip(" ,.;:-\n|")
                    # Limpar: pegar apenas até o primeiro delimitador forte
                    raw_object = re.split(r"[,;.|\n]", raw_object)[0].strip()
                    if len(raw_object) < 3 or _looks_truncated(raw_object):
                        continue

                    if predicate in {"child_of", "widow_of", "spouse_of"}:
                        object_name = _find_entity_name(raw_object, entities)
                        object_literal = ""
                    else:
                        object_name = ""
                        object_literal = raw_object

                    key = (entity.canonical_name, predicate, object_name, object_literal)
                    if key in seen:
                        continue
                    if object_name and entity.canonical_name == object_name:
                        continue
                    seen.add(key)

                    evidence = text[pos:pos + len(alias) + match.end()].strip()
                    relations.append(
                        ExtractedRelation(
                            subject_name=entity.canonical_name,
                            predicate=predicate,
                            object_name=object_name,
                            object_literal=object_literal,
                            confidence=confidence,
                            status="hypothesis",
                            evidence_quote=evidence[:200],
                            extraction_method="ocr_tolerant_heuristic",
                        )
                    )
                    break  # Só uma relação por padrão por posição

            # Só usar a primeira alias que produzir resultado
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
