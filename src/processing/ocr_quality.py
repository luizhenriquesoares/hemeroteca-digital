"""Métricas, comparação e seleção de resultados de OCR."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class OCRResult:
    text: str
    score: float
    variant: str
    metrics: dict


@dataclass(frozen=True)
class OCRComparison:
    selected_text: str
    selected_source: str
    selected_variant: str
    new_result: OCRResult
    existing_result: OCRResult | None
    reason: str


def limpar_texto(texto: str) -> str:
    lines = texto.split("\n")
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if len(re.findall(r"[a-zA-ZÀ-ú0-9]", stripped)) >= 2:
            cleaned_lines.append(stripped)

    texto = "\n".join(cleaned_lines)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    texto = re.sub(r" {3,}", " ", texto)
    return texto.strip()


def score_ocr_text(texto: str) -> dict:
    cleaned = limpar_texto(texto)
    words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'-]{1,}", cleaned)
    long_words = [word for word in words if len(word) >= 3]
    valid_words = [word for word in long_words if re.search(r"[aeiouáéíóúãõâêôà]", word.lower())]
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    alpha_count = len(re.findall(r"[A-Za-zÀ-ÿ]", cleaned))
    odd_count = len(re.findall(r"[^A-Za-zÀ-ÿ0-9\s,.;:!?()'\"-]", cleaned))
    short_line_ratio = (
        sum(1 for line in lines if len(re.findall(r"[A-Za-zÀ-ÿ]", line)) < 8) / len(lines)
        if lines else 1.0
    )
    uppercase_ratio = (
        len(re.findall(r"[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ]", cleaned)) / alpha_count if alpha_count else 0
    )
    valid_word_ratio = len(valid_words) / max(len(long_words), 1)
    density = len(cleaned) / max(len(lines), 1)
    odd_char_ratio = odd_count / max(len(cleaned), 1)
    score = (
        valid_word_ratio * 0.52
        + min(len(valid_words) / 120, 1.0) * 0.18
        + min(density / 50, 1.0) * 0.14
        + max(0, 1 - short_line_ratio) * 0.10
        + max(0, 1 - odd_char_ratio * 8) * 0.06
    )

    if len(cleaned) < 120:
        score *= 0.55
    if uppercase_ratio > 0.72 and valid_word_ratio < 0.55:
        score *= 0.8

    return {
        "score": round(max(0.0, min(score, 1.0)), 4),
        "chars": len(cleaned),
        "lines": len(lines),
        "valid_word_ratio": round(valid_word_ratio, 4),
        "short_line_ratio": round(short_line_ratio, 4),
        "odd_char_ratio": round(odd_char_ratio, 4),
        "uppercase_ratio": round(uppercase_ratio, 4),
    }


def select_best_ocr_result(candidates: list[OCRResult]) -> OCRResult:
    valid_candidates = [candidate for candidate in candidates if candidate.text.strip()]
    if not valid_candidates:
        return OCRResult(text="", score=0.0, variant="empty", metrics=score_ocr_text(""))
    return max(
        valid_candidates,
        key=lambda item: (item.score, item.metrics.get("valid_word_ratio", 0), item.metrics.get("chars", 0)),
    )


def result_from_text(text: str, variant: str = "saved") -> OCRResult:
    metrics = score_ocr_text(text)
    return OCRResult(text=text, score=metrics["score"], variant=variant, metrics=metrics)


def compare_with_existing(existing_text: str, new_result: OCRResult) -> OCRComparison:
    existing = result_from_text(existing_text, variant="saved")

    score_gain = new_result.score - existing.score
    valid_gain = new_result.metrics["valid_word_ratio"] - existing.metrics["valid_word_ratio"]
    odd_gain = existing.metrics["odd_char_ratio"] - new_result.metrics["odd_char_ratio"]
    short_gain = existing.metrics["short_line_ratio"] - new_result.metrics["short_line_ratio"]
    char_gain = new_result.metrics["chars"] - existing.metrics["chars"]

    strong_improvement = score_gain >= 0.035 and valid_gain >= 0.01 and odd_gain >= 0.004
    moderate_improvement = (
        score_gain >= 0.02
        and valid_gain >= 0.005
        and odd_gain >= 0.002
        and short_gain >= 0.02
        and char_gain >= -200
    )
    rescue_bad_existing = (
        (existing.metrics["odd_char_ratio"] >= 0.025 or existing.score <= 0.55)
        and new_result.score >= 0.8
        and new_result.metrics["valid_word_ratio"] >= existing.metrics["valid_word_ratio"]
        and new_result.metrics["chars"] >= max(80, existing.metrics["chars"] * 0.6)
    )

    if strong_improvement or moderate_improvement or rescue_bad_existing:
        return OCRComparison(
            selected_text=new_result.text,
            selected_source="new",
            selected_variant=new_result.variant,
            new_result=new_result,
            existing_result=existing,
            reason="new_ocr_won",
        )

    return OCRComparison(
        selected_text=existing.text,
        selected_source="existing",
        selected_variant=existing.variant,
        new_result=new_result,
        existing_result=existing,
        reason="existing_ocr_kept",
    )
