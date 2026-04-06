"""Normalização e perfil de consulta para busca histórica."""

from __future__ import annotations

import itertools
import re
import unicodedata
from dataclasses import dataclass

STOPWORDS = {
    "a", "o", "os", "as", "e", "de", "da", "do", "das", "dos", "d", "em", "na", "no",
    "sobre", "busque", "buscar", "informacao", "informacoes", "quem", "quais", "qual",
    "dados", "me", "traga", "trazer",
}

TOKEN_CANONICAL_MAP = {
    "benedicto": "benedito",
    "benedicta": "benedita",
    "capitam": "capitao",
    "dr": "doutor",
}

TOKEN_VARIANTS = {
    "antonio": {"antonio", "antônio"},
    "benedito": {"benedito", "benedicto"},
    "benedita": {"benedita", "benedicta"},
    "araujo": {"araujo", "araújo", "d'araujo", "d araujo", "de araujo"},
    "capitao": {"capitao", "capitão", "capitam"},
    "de": {"de", "d"},
    "doutor": {"dr", "doutor"},
}


@dataclass
class QueryProfile:
    raw: str
    normalized: str
    significant_tokens: list[str]
    variants: list[str]


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(text: str) -> str:
    text = strip_accents(text.lower())
    text = text.replace("’", "'").replace("`", "'").replace("´", "'")
    text = re.sub(r"\bd'(?=[a-z])", "de ", text)
    text = re.sub(r"[^a-z0-9']+", " ", text)
    tokens = []
    for token in text.split():
        token = TOKEN_CANONICAL_MAP.get(token, token)
        if token == "d":
            token = "de"
        tokens.append(token)
    return " ".join(tokens)


def tokenize_significant(text: str) -> list[str]:
    tokens = [token for token in normalize_text(text).split() if len(token) >= 2]
    return [token for token in tokens if token not in STOPWORDS]


def _expand_token_forms(token: str) -> set[str]:
    forms = {token}
    if token.endswith("s") and len(token) >= 5:
        forms.add(token[:-1])
    if token.endswith("es") and len(token) >= 6:
        forms.add(token[:-2])
    return {normalize_text(form) for form in forms if normalize_text(form)}


def expand_query_variants(tokens: list[str], max_variants: int = 32) -> list[str]:
    if not tokens:
        return []

    variant_lists = []
    for token in tokens:
        raw_variants = set(TOKEN_VARIANTS.get(token, {token}))
        raw_variants.update(_expand_token_forms(token))
        variants = sorted({normalize_text(v) for v in raw_variants if normalize_text(v)})
        variant_lists.append(variants[:4])

    results = []
    seen = set()
    for combo in itertools.product(*variant_lists):
        phrase = " ".join(combo).strip()
        if phrase and phrase not in seen:
            seen.add(phrase)
            results.append(phrase)
        if len(results) >= max_variants:
            break

    base = " ".join(tokens)
    if base and base not in seen:
        results.insert(0, base)

    return results[:max_variants]


def build_query_profile(query: str) -> QueryProfile:
    tokens = tokenize_significant(query)
    return QueryProfile(
        raw=query,
        normalized=normalize_text(query),
        significant_tokens=tokens,
        variants=expand_query_variants(tokens) or [normalize_text(query)],
    )


def focus_query(query: str) -> str:
    profile = build_query_profile(query)
    if profile.significant_tokens:
        return " ".join(profile.significant_tokens)
    return normalize_text(query)
