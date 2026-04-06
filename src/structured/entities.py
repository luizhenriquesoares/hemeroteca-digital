"""Extração heurística de entidades com normalização histórica leve."""

from __future__ import annotations

import re
import unicodedata

from src.structured.models import ExtractedEntity

PERSON_TITLES = {
    "dr": "Doutor",
    "doutor": "Doutor",
    "doutora": "Doutora",
    "sr": "Senhor",
    "sra": "Senhora",
    "senhor": "Senhor",
    "senhora": "Senhora",
    "capitao": "Capitão",
    "capitão": "Capitão",
    "coronel": "Coronel",
    "major": "Major",
    "tenente": "Tenente",
    "padre": "Padre",
    "vigario": "Vigário",
    "vigário": "Vigário",
    "conselheiro": "Conselheiro",
    "desembargador": "Desembargador",
    "comendador": "Comendador",
    "barao": "Barão",
    "barão": "Barão",
    "visconde": "Visconde",
    "d": "D.",
    "d.": "D.",
}

INSTITUTION_MARKERS = {
    "associacao", "associação", "companhia", "comp", "sociedade", "instituto",
    "camara", "câmara", "alfandega", "alfândega", "mesa", "consulado",
    "secretaria", "thezouro", "thesouro", "igreja", "tribunal", "junta",
    "prefeitura", "governo", "presidencia", "presidência", "typographia",
}

PLACE_PREPOSITIONS = {"em", "na", "no", "para", "de"}
PLACE_STOPWORDS = {"rua", "villa", "vila", "cidade", "provincia", "província", "bairro"}

ROLE_TITLES = {
    "capitao", "major", "tenente", "coronel", "padre", "vigario", "doutor",
    "doutora", "conselheiro", "desembargador", "comendador", "barao", "visconde",
}

STOP_WORDS = {
    "de", "da", "do", "das", "dos", "e", "em", "para", "por", "com", "sem",
}

PERSON_PATTERN = re.compile(
    r"\b(?:(?P<title>D\.|Dr\.?|Doutor|Doutora|Sr\.?|Sra\.?|Senhor|Senhora|Capit[aã]o|Coronel|Major|Tenente|Padre|Vig[aá]rio|Conselheiro|Desembargador|Comendador|Bar[aã]o|Visconde)\s+)?"
    r"(?P<name>(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)"
    r"(?:\s+(?:(?:de|da|do|das|dos|e|d['’])\s+)?(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)){1,6})\b"
)

INSTITUTION_PATTERN = re.compile(
    r"\b(?:Associaç[aã]o|Association|Sociedade|Companhia|Comp\.?|Instituto|C[aâ]mara|Alf[aâ]ndega|Mesa|Consulado|Secretaria|Thesouro|Typographia|Igreja|Tribunal|Junta|Governo|Presid[eê]ncia)"
    r"(?:\s+(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|de|do|da|dos|das|e)){0,10}\b"
)

PLACE_PATTERN = re.compile(
    r"\b(?:em|na|no|para)\s+(?P<place>[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+(?:\s+(?:de|do|da|dos|das)\s+[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+){0,3})\b"
)


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("’", "'")
    text = re.sub(r"\bd[' ]", "de ", text)
    text = text.replace("benedicto", "benedito")
    text = text.replace("affonso", "afonso")
    text = text.replace("d'araújo", "de araujo").replace("d'araujo", "de araujo")
    text = re.sub(r"[^a-z0-9'\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonicalize_person_name(surface_form: str, title: str = "") -> tuple[str, str]:
    raw_name = re.sub(r"\s+", " ", (surface_form or "").strip(" ,.;:-"))
    normalized_title = normalize_name(title).replace(".", "")
    title_label = PERSON_TITLES.get(normalized_title, "")
    parts = [part for part in raw_name.split() if normalize_name(part) not in STOP_WORDS or len(raw_name.split()) <= 2]
    canonical = " ".join(parts) if parts else raw_name
    canonical = re.sub(r"\s+", " ", canonical).strip()
    return canonical, title_label


def build_person_aliases(canonical_name: str, surface_name: str) -> tuple[str, ...]:
    aliases = {surface_name.strip(), canonical_name.strip()}
    normalized = normalize_name(canonical_name)

    if "affonso" in canonical_name.lower():
        aliases.add(re.sub(r"Affonso", "Afonso", canonical_name, flags=re.IGNORECASE))
    if "benedicto" in canonical_name.lower():
        aliases.add(re.sub(r"Benedicto", "Benedito", canonical_name, flags=re.IGNORECASE))
    if "d'" in canonical_name.lower():
        aliases.add(re.sub(r"\bd['’]", "de ", canonical_name, flags=re.IGNORECASE))
    if " de " in canonical_name.lower():
        aliases.add(re.sub(r"\bde\s+([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ])", r"d'\1", canonical_name))

    # Variante normalizada como fallback de busca, mas não exibir se igual visualmente.
    aliases.add(" ".join(part.capitalize() for part in normalized.split()))
    return tuple(sorted(alias for alias in aliases if alias and alias.strip()))


def _confidence_for_person(name: str, title: str) -> float:
    token_count = len(name.split())
    score = 0.55 + min(token_count, 5) * 0.07
    if title:
        score += 0.08
    return min(score, 0.96)


def _looks_like_institution_name(name: str) -> bool:
    normalized_tokens = set(normalize_name(name).split())
    return bool(normalized_tokens & INSTITUTION_MARKERS)


def _extract_people(text: str) -> dict[tuple[str, str], ExtractedEntity]:
    seen: dict[tuple[str, str], ExtractedEntity] = {}

    for match in PERSON_PATTERN.finditer(text or ""):
        title = match.group("title") or ""
        surface_name = match.group("name") or ""
        canonical_name, normalized_title = canonicalize_person_name(surface_name, title)
        normalized_name = normalize_name(canonical_name)
        if len(normalized_name.split()) < 2:
            continue
        if _looks_like_institution_name(canonical_name):
            continue
        key = ("person", normalized_name)
        aliases = build_person_aliases(canonical_name, surface_name)
        role = normalized_title if normalize_name(normalized_title) in ROLE_TITLES else ""
        entity = ExtractedEntity(
            entity_type="person",
            canonical_name=canonical_name,
            normalized_name=normalized_name,
            surface_form=(title + " " + surface_name).strip(),
            aliases=aliases,
            confidence=_confidence_for_person(canonical_name, title),
            role=role,
            attributes={"title": normalized_title} if normalized_title else {},
        )
        previous = seen.get(key)
        if not previous or entity.confidence > previous.confidence:
            seen[key] = entity

    return seen


def _extract_institutions(text: str) -> dict[tuple[str, str], ExtractedEntity]:
    seen: dict[tuple[str, str], ExtractedEntity] = {}
    for match in INSTITUTION_PATTERN.finditer(text or ""):
        surface = re.sub(r"\s+", " ", match.group(0).strip(" ,.;:-"))
        normalized = normalize_name(surface)
        if len(normalized.split()) < 1:
            continue
        key = ("institution", normalized)
        entity = ExtractedEntity(
            entity_type="institution",
            canonical_name=surface,
            normalized_name=normalized,
            surface_form=surface,
            aliases=(surface,),
            confidence=0.82,
            attributes={},
        )
        seen[key] = entity
    return seen


def _extract_places(text: str) -> dict[tuple[str, str], ExtractedEntity]:
    seen: dict[tuple[str, str], ExtractedEntity] = {}
    for match in PLACE_PATTERN.finditer(text or ""):
        surface = re.sub(r"\s+", " ", match.group("place").strip(" ,.;:-"))
        normalized = normalize_name(surface)
        tokens = normalized.split()
        if not tokens:
            continue
        if tokens[0] in PLACE_STOPWORDS:
            continue
        key = ("place", normalized)
        entity = ExtractedEntity(
            entity_type="place",
            canonical_name=surface,
            normalized_name=normalized,
            surface_form=surface,
            aliases=(surface,),
            confidence=0.72,
            attributes={},
        )
        seen[key] = entity
    return seen


def extract_entities(text: str) -> list[ExtractedEntity]:
    """Extrai pessoas, instituições e lugares a partir do texto."""
    seen: dict[tuple[str, str], ExtractedEntity] = {}
    seen.update(_extract_people(text))
    seen.update(_extract_institutions(text))
    seen.update(_extract_places(text))
    return sorted(seen.values(), key=lambda item: (-item.confidence, item.entity_type, item.canonical_name))
