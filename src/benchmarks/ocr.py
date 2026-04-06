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
    similarity_to_corrected: float | None
    output_path: str
    variant: str = ""
    error: str = ""


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return round(difflib.SequenceMatcher(a=a, b=b).ratio(), 4)


def _build_result(
    *,
    label: str,
    text: str,
    output_path: Path,
    elapsed_sec: float,
    corrected_text: str,
    variant: str = "",
) -> OCRBenchmarkResult:
    metrics = _score_ocr_text(text)
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
        similarity_to_corrected=_similarity(text, corrected_text) if corrected_text else None,
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
