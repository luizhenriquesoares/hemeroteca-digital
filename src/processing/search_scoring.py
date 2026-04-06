"""Scoring e recorte de evidência para busca histórica."""

from __future__ import annotations

import math
from difflib import SequenceMatcher

from src.processing.search_profile import QueryProfile, build_query_profile, normalize_text


def extract_evidence_snippet(query: str, text: str, max_chars: int = 420) -> str:
    if not text:
        return ""

    normalized_text = normalize_text(text)
    profile = build_query_profile(query)

    best_start = 0
    best_score = -1.0
    token_positions = []

    for variant in profile.variants:
        pos = normalized_text.find(variant)
        if pos >= 0:
            token_positions.append(pos)

    for token in profile.significant_tokens:
        pos = normalized_text.find(token)
        if pos >= 0:
            token_positions.append(pos)

    if token_positions:
        best_start = min(token_positions)
        best_score = 1.0

    if best_score < 0:
        raw_tokens = [token for token in profile.significant_tokens if len(token) >= 4]
        for token in raw_tokens:
            rough = SequenceMatcher(None, normalized_text, token)
            match = rough.find_longest_match(0, len(normalized_text), 0, len(token))
            if match.size > 0:
                best_start = match.a
                best_score = match.size
                break

    if best_score < 0:
        snippet = text[:max_chars]
        return snippet.strip() + ("..." if len(text) > max_chars else "")

    left = max(0, best_start - max_chars // 3)
    right = min(len(text), left + max_chars)
    snippet = text[left:right].strip()

    if left > 0:
        snippet = "..." + snippet
    if right < len(text):
        snippet = snippet + "..."
    return snippet


def similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if abs(len(a) - len(b)) > 2:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def best_token_match(token: str, doc_tokens: list[str]) -> tuple[float, int]:
    best_score = 0.0
    best_pos = -1
    for idx, doc_token in enumerate(doc_tokens):
        score = similarity(token, doc_token)
        if score > best_score:
            best_score = score
            best_pos = idx
        if score >= 0.999:
            break
    return best_score, best_pos


def ordered_ratio(positions: list[int]) -> float:
    valid = [pos for pos in positions if pos >= 0]
    if len(valid) < 2:
        return 0.0
    return 1.0 if valid == sorted(valid) else 0.35


def compactness_bonus(positions: list[int]) -> float:
    valid = [pos for pos in positions if pos >= 0]
    if len(valid) < 2:
        return 0.0
    span = max(valid) - min(valid) + 1
    return max(0.0, 1.0 - min(span, 40) / 40)


def score_text(profile: QueryProfile, text: str) -> tuple[float, list[str]]:
    normalized = normalize_text(text)
    doc_tokens = normalized.split()
    if not doc_tokens or not profile.significant_tokens:
        return 0.0, []

    token_scores = []
    positions = []
    matched_tokens = []

    for token in profile.significant_tokens:
        score, pos = best_token_match(token, doc_tokens)
        token_scores.append(score)
        positions.append(pos)
        if score >= 0.84:
            matched_tokens.append(token)

    min_required = max(1, math.ceil(len(profile.significant_tokens) * 0.6))
    if len(matched_tokens) < min_required:
        return 0.0, []

    coverage = sum(token_scores) / len(profile.significant_tokens)
    phrase_bonus = 1.0 if any(variant in normalized for variant in profile.variants) else 0.0
    order_bonus = ordered_ratio(positions)
    compactness = compactness_bonus(positions)

    score = (
        coverage * 0.60
        + phrase_bonus * 0.20
        + order_bonus * 0.10
        + compactness * 0.10
    )
    return min(1.0, round(score, 4)), matched_tokens
