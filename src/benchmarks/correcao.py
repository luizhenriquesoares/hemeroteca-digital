"""Benchmark simples para comparar correção OpenAI vs Claude."""

from __future__ import annotations

import difflib
import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from src.config import BASE_DIR
from src.correcao_provider import (
    CLAUDE_DEFAULT_MODEL,
    OPENAI_DEFAULT_MODEL,
    OPENAI_MAX_MODEL,
    corrigir_texto,
)

BENCHMARK_DIR = BASE_DIR / "data" / "benchmarks"
BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class BenchmarkResult:
    label: str
    provider: str
    model: str
    ok: bool
    elapsed_sec: float
    input_chars: int
    output_chars: int
    output_words: int
    changed_ratio: float
    real_word_ratio: float
    output_path: str
    error: str = ""


def _estimate_changed_ratio(original: str, corrected: str) -> float:
    if not original:
        return 0.0
    matcher = difflib.SequenceMatcher(a=original, b=corrected)
    return round(1.0 - matcher.ratio(), 4)


def _real_word_ratio(text: str) -> float:
    words = text.split()
    if not words:
        return 0.0
    valid = [w for w in words if len(re.findall(r"[A-Za-zÀ-ÿ]", w)) >= 2]
    return round(len(valid) / len(words), 4)


def _slug(label: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "-", label.lower()).strip("-")


def default_runs() -> list[tuple[str, str, str]]:
    return [
        ("openai-mini", "openai", OPENAI_DEFAULT_MODEL),
        ("openai-max", "openai", OPENAI_MAX_MODEL),
        ("claude", "claude", CLAUDE_DEFAULT_MODEL),
    ]


def run_benchmark(
    txt_path: Path,
    runs: list[tuple[str, str, str]] | None = None,
    sample_chars: int = 0,
) -> dict:
    original = txt_path.read_text(encoding="utf-8")
    sample = original[:sample_chars] if sample_chars and sample_chars < len(original) else original

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = BENCHMARK_DIR / f"{txt_path.stem}_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "original.txt").write_text(sample, encoding="utf-8")

    results = []
    for label, provider, model in (runs or default_runs()):
        t0 = time.time()
        output_path = out_dir / f"{_slug(label)}.txt"
        try:
            corrected = corrigir_texto(sample, provider=provider, model=model)
            elapsed = round(time.time() - t0, 2)
            if not corrected:
                result = BenchmarkResult(
                    label=label,
                    provider=provider,
                    model=model,
                    ok=False,
                    elapsed_sec=elapsed,
                    input_chars=len(sample),
                    output_chars=0,
                    output_words=0,
                    changed_ratio=0.0,
                    real_word_ratio=0.0,
                    output_path=str(output_path),
                    error="saida_vazia",
                )
            else:
                output_path.write_text(corrected, encoding="utf-8")
                result = BenchmarkResult(
                    label=label,
                    provider=provider,
                    model=model,
                    ok=True,
                    elapsed_sec=elapsed,
                    input_chars=len(sample),
                    output_chars=len(corrected),
                    output_words=len(corrected.split()),
                    changed_ratio=_estimate_changed_ratio(sample, corrected),
                    real_word_ratio=_real_word_ratio(corrected),
                    output_path=str(output_path),
                )
        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            result = BenchmarkResult(
                label=label,
                provider=provider,
                model=model,
                ok=False,
                elapsed_sec=elapsed,
                input_chars=len(sample),
                output_chars=0,
                output_words=0,
                changed_ratio=0.0,
                real_word_ratio=0.0,
                output_path=str(output_path),
                error=str(e),
            )
        results.append(result)

    summary = {
        "source_file": str(txt_path),
        "sample_chars": len(sample),
        "out_dir": str(out_dir),
        "results": [asdict(r) for r in results],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
