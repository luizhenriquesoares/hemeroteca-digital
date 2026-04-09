"""Resolução leve de identidade para entidades históricas."""

from __future__ import annotations

from dataclasses import dataclass

from src.structured.entities import normalize_name
from src.structured.models import ExtractedEntity, PageReference


@dataclass(frozen=True)
class IdentityResolution:
    identity_key: str
    base_name: str
    status: str
    confidence: float
    hints: tuple[str, ...]


def resolve_entity_identity(
    entity: ExtractedEntity,
    page: PageReference,
    related_entities: list[ExtractedEntity],
) -> IdentityResolution:
    base_name = entity.normalized_name
    if entity.entity_type != "person":
        return IdentityResolution(
            identity_key=base_name,
            base_name=base_name,
            status="resolved",
            confidence=entity.confidence,
            hints=(),
        )

    hints = []
    title = normalize_name(entity.attributes.get("title", "")) if entity.attributes else ""
    role = normalize_name(entity.role)
    if title:
        hints.append(f"title:{title}")
    elif role:
        hints.append(f"role:{role}")

    context_entities = [
        related for related in related_entities
        if related.entity_type in {"institution", "place"} and related.canonical_name != entity.canonical_name
    ]
    for related in context_entities[:2]:
        prefix = "inst" if related.entity_type == "institution" else "place"
        hints.append(f"{prefix}:{normalize_name(related.canonical_name)}")

    related_people = [
        related for related in related_entities
        if related.entity_type == "person" and related.canonical_name != entity.canonical_name
    ]
    surnames = base_name.split()
    primary_surname = surnames[-1] if surnames else ""
    family_matches = []
    for related in related_people:
        related_tokens = related.normalized_name.split()
        if primary_surname and primary_surname in related_tokens:
            family_matches.append(related.normalized_name)

    for related_name in family_matches[:2]:
        hints.append(f"family:{related_name}")

    year = page.ano if page.ano not in {"", "?"} else ""
    token_count = len(base_name.split())

    if token_count >= 3 and not hints:
        return IdentityResolution(
            identity_key=base_name,
            base_name=base_name,
            status="resolved",
            confidence=min(0.95, entity.confidence),
            hints=(),
        )

    if token_count <= 2 and year:
        hints.append(f"year:{year}")

    has_role_context = any(hint.startswith(("title:", "role:")) for hint in hints)
    has_family_context = any(hint.startswith("family:") for hint in hints)

    if hints:
        identity_key = f"{base_name}::{'+'.join(hints)}"
        status = "contextual" if (has_role_context or has_family_context or token_count >= 3) else "ambiguous"
        confidence = min(0.9, entity.confidence)
    else:
        identity_key = base_name
        status = "resolved"
        confidence = entity.confidence

    return IdentityResolution(
        identity_key=identity_key,
        base_name=base_name,
        status=status,
        confidence=confidence,
        hints=tuple(hints),
    )


def resolve_relation_entity_id(
    name: str,
    candidates: list[dict],
) -> int | None:
    target = normalize_name(name)
    if not candidates:
        return None

    exact = [candidate for candidate in candidates if candidate["base_name"] == target]
    if len(exact) == 1:
        return exact[0]["entity_id"]

    preferred = [
        candidate for candidate in exact
        if candidate["status"] in {"resolved", "contextual"}
    ]
    if len(preferred) == 1:
        return preferred[0]["entity_id"]

    ranked = sorted(
        exact or candidates,
        key=lambda item: (
            item["status"] == "resolved",
            item["status"] == "contextual",
            item["confidence"],
            len(item["canonical_name"]),
        ),
        reverse=True,
    )
    return ranked[0]["entity_id"] if ranked else None
