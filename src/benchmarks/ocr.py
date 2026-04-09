"""Benchmark para comparar OCR salvo, OCR adaptativo e texto corrigido."""

from __future__ import annotations

import difflib
import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from src.config import BASE_DIR
from src.ocr import _score_ocr_text, extrair_texto_com_qualidade

BENCHMARK_DIR = BASE_DIR / "data" / "benchmarks"
BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class OCRBenchmarkResult:
    label: str
    ok: bool
    elapsed_sec: float
    chars: int
    words: int
    score: float
    valid_word_ratio: float
    odd_char_ratio: float
    short_line_ratio: float
    historiographic_score: float
    named_entity_ratio: float
    heading_ratio: float
    date_signal: bool
    similarity_to_corrected: float | None
    operational_bad_page_score: float
    recommendation: str
    output_path: str
    variant: str = ""
    error: str = ""


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return round(difflib.SequenceMatcher(a=a, b=b).ratio(), 4)


def _count_named_entity_signals(text: str) -> int:
    pattern = re.compile(
        r"\b(?:D\.|Dr\.?|Sr\.?|Sra\.|[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)"
        r"(?:\s+(?:(?:de|da|do|dos|das|d['’]|e)\s+)?[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+){1,3}\b"
    )
    return len(pattern.findall(text or ""))


def _heading_ratio(text: str) -> float:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return 0.0
    heading_like = 0
    for line in lines[:12]:
        alpha = re.sub(r"[^A-Za-zÀ-ÿ]", "", line)
        if alpha and alpha.upper() == alpha and len(line.split()) <= 8:
            heading_like += 1
    return round(min(1.0, heading_like / max(1, min(len(lines), 12))), 4)


def _has_date_signal(text: str) -> bool:
    patterns = [
        r"\b\d{1,2}\s+de\s+[A-Za-zÀ-ÿ]+\s+de\s+\d{4}\b",
        r"\banno\s+de\s+\d{4}\b",
        r"\bhoje\s+[A-Za-zÀ-ÿ]+\b",
    ]
    normalized = text.lower()
    return any(re.search(pattern, normalized) for pattern in patterns)


def score_historiographic_quality(text: str, corrected_text: str = "") -> dict:
    metrics = _score_ocr_text(text)
    words = max(1, len((text or "").split()))
    named_entity_ratio = round(min(1.0, _count_named_entity_signals(text) / max(2, words / 18)), 4)
    heading_ratio = _heading_ratio(text)
    date_signal = _has_date_signal(text)
    similarity = _similarity(text, corrected_text) if corrected_text else None

    score = (
        metrics["valid_word_ratio"] * 0.35
        + (1 - metrics["odd_char_ratio"]) * 0.20
        + (1 - metrics["short_line_ratio"]) * 0.10
        + named_entity_ratio * 0.15
        + heading_ratio * 0.10
        + (0.10 if date_signal else 0.0)
    )
    if similarity is not None:
        score = score * 0.8 + similarity * 0.2
    score = round(max(0.0, min(score, 1.0)), 4)

    bad_page_score = (
        (1 - metrics["valid_word_ratio"]) * 0.35
        + metrics["odd_char_ratio"] * 0.30
        + metrics["short_line_ratio"] * 0.20
        + (0.08 if not date_signal else 0.0)
        + (0.07 if named_entity_ratio < 0.15 else 0.0)
    )
    if similarity is not None:
        bad_page_score += (1 - similarity) * 0.15
    bad_page_score = round(max(0.0, min(bad_page_score, 1.0)), 4)

    if bad_page_score >= 0.7:
        recommendation = "reprocessar"
    elif bad_page_score >= 0.4:
        recommendation = "revisar"
    else:
        recommendation = "manter"

    return {
        **metrics,
        "historiographic_score": score,
        "named_entity_ratio": named_entity_ratio,
        "heading_ratio": heading_ratio,
        "date_signal": date_signal,
        "similarity_to_corrected": similarity,
        "operational_bad_page_score": bad_page_score,
        "recommendation": recommendation,
    }


def _build_result(
    *,
    label: str,
    text: str,
    output_path: Path,
    elapsed_sec: float,
    corrected_text: str,
    variant: str = "",
) -> OCRBenchmarkResult:
    metrics = score_historiographic_quality(text, corrected_text=corrected_text)
    output_path.write_text(text, encoding="utf-8")
    return OCRBenchmarkResult(
        label=label,
        ok=bool(text.strip()),
        elapsed_sec=round(elapsed_sec, 2),
        chars=len(text),
        words=len(text.split()),
        score=metrics["score"],
        valid_word_ratio=metrics["valid_word_ratio"],
        odd_char_ratio=metrics["odd_char_ratio"],
        short_line_ratio=metrics["short_line_ratio"],
        historiographic_score=metrics["historiographic_score"],
        named_entity_ratio=metrics["named_entity_ratio"],
        heading_ratio=metrics["heading_ratio"],
        date_signal=metrics["date_signal"],
        similarity_to_corrected=metrics["similarity_to_corrected"],
        operational_bad_page_score=metrics["operational_bad_page_score"],
        recommendation=metrics["recommendation"],
        output_path=str(output_path),
        variant=variant,
    )


def run_benchmark(
    image_path: Path,
    *,
    saved_ocr_path: Path | None = None,
    corrected_path: Path | None = None,
) -> dict:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = BENCHMARK_DIR / f"{image_path.stem}_ocr_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    saved_text = saved_ocr_path.read_text(encoding="utf-8") if saved_ocr_path and saved_ocr_path.exists() else ""
    corrected_text = corrected_path.read_text(encoding="utf-8") if corrected_path and corrected_path.exists() else ""

    results: list[OCRBenchmarkResult] = []

    if saved_text:
        results.append(
            _build_result(
                label="ocr-salvo",
                text=saved_text,
                output_path=out_dir / "ocr-salvo.txt",
                elapsed_sec=0.0,
                corrected_text=corrected_text,
                variant="saved",
            )
        )

    if corrected_text:
        results.append(
            _build_result(
                label="corrigido-llm",
                text=corrected_text,
                output_path=out_dir / "corrigido-llm.txt",
                elapsed_sec=0.0,
                corrected_text=corrected_text,
                variant="reference",
            )
        )

    t0 = time.time()
    adaptive = extrair_texto_com_qualidade(image_path)
    results.append(
        _build_result(
            label="ocr-adaptativo",
            text=adaptive.text,
            output_path=out_dir / "ocr-adaptativo.txt",
            elapsed_sec=time.time() - t0,
            corrected_text=corrected_text,
            variant=adaptive.variant,
        )
    )

    summary = {
        "image_path": str(image_path),
        "saved_ocr_path": str(saved_ocr_path) if saved_ocr_path else "",
        "corrected_path": str(corrected_path) if corrected_path else "",
        "out_dir": str(out_dir),
        "results": [asdict(item) for item in results],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
