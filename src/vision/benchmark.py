"""Benchmark da leitura visão-primeiro: configurações x páginas.

Para cada par registra tokens reais, custo (avulso e Batch), tempo, e métricas
de qualidade sem gabarito humano: proporção de palavras válidas (mesma métrica
do benchmark de OCR) e cobertura do vocabulário do texto de referência local
(``_corrigido.txt`` ou ``.txt`` do Tesseract) quando existir. Extrapola o custo
para o recorte alvo (padrão: 150 mil páginas do século XIX).
"""

from __future__ import annotations

import json
import re
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from src.config import DATA_DIR, IMAGES_DIR, TEXT_DIR
from src.processing.ocr_quality import score_ocr_text
from src.vision.reader import (
    CONFIGS,
    ReadConfig,
    ReadResult,
    build_request,
    count_input_tokens,
    estimate_cost,
    read_page,
)
from src.vision.tiles import make_tiles

BENCHMARK_DIR = DATA_DIR / "benchmarks" / "visao"
GOLDEN_FILE = Path(__file__).with_name("golden.json")
DEFAULT_EXTRAPOLATION_PAGES = 150_000

_WORD_RE = re.compile(r"[a-zà-ÿ]{4,}")


def load_golden() -> list[str]:
    payload = json.loads(GOLDEN_FILE.read_text(encoding="utf-8"))
    return [entry["id"] if isinstance(entry, dict) else str(entry) for entry in payload.get("pages", [])]


def resolve_page(page: str) -> dict[str, Any]:
    """Aceita caminho de imagem ou id (``029033_01_00044``) e localiza imagem/referência."""
    candidate = Path(page)
    if candidate.suffix.lower() in {".jpg", ".jpeg", ".png"} and candidate.exists():
        page_id = candidate.stem
        bib = candidate.parent.name
        image = candidate
    else:
        page_id = candidate.stem if candidate.suffix else page
        bib = page_id.rsplit("_", 1)[0]
        image_candidate = IMAGES_DIR / bib / f"{page_id}.jpg"
        image = image_candidate if image_candidate.exists() else None

    reference = None
    for name in (f"{page_id}_corrigido.txt", f"{page_id}.txt"):
        path = TEXT_DIR / bib / name
        if path.exists():
            reference = path
            break
    return {"id": page_id, "bib": bib, "image": image, "reference": reference}


def _normalized_words(text: str) -> set[str]:
    return set(_WORD_RE.findall(text.lower()))


def quality_metrics(extraction: dict[str, Any] | None, reference_text: str | None) -> dict[str, Any]:
    segments = (extraction or {}).get("segments", []) or []
    text = "\n\n".join(segment.get("text", "") for segment in segments)
    people = [person.get("name", "") for segment in segments for person in segment.get("people", [])]
    relations = sum(len(segment.get("relations", [])) for segment in segments)
    types = Counter(segment.get("type", "outro") for segment in segments)

    if text.strip():
        quality = score_ocr_text(text)
        valid_word_ratio = quality["valid_word_ratio"]
    else:
        valid_word_ratio = 0.0

    metrics: dict[str, Any] = {
        "chars": len(text),
        "segments": len(segments),
        "people": len(people),
        "distinct_people": len({name for name in people if name}),
        "relations": relations,
        "valid_word_ratio": round(valid_word_ratio, 4),
        "segment_types": dict(types),
        "ref_word_overlap": None,
        "ref_words": 0,
    }
    if reference_text:
        found = _normalized_words(text)
        expected = _normalized_words(reference_text)
        metrics["ref_words"] = len(expected)
        metrics["ref_word_overlap"] = round(len(found & expected) / len(expected), 4) if expected else None
    return metrics


def select_configs(
    names: Iterable[str] | None,
    grid: tuple[int, int] | None = None,
    backend: str | None = None,
) -> list[ReadConfig]:
    chosen = list(names) if names else list(CONFIGS)
    configs = []
    for name in chosen:
        if name not in CONFIGS:
            raise ValueError(f"Configuração desconhecida: {name!r}. Opções: {', '.join(CONFIGS)}")
        config = CONFIGS[name]
        if grid:
            config = config.with_grid(grid)
        if backend:
            config = config.with_backend(backend)
        configs.append(config)
    return configs


def _mean(values: list[float]) -> float | None:
    values = [value for value in values if value is not None]
    return round(statistics.fmean(values), 4) if values else None


def aggregate(results: list[dict[str, Any]], extrapolate_pages: int) -> dict[str, dict[str, Any]]:
    by_config: dict[str, list[dict[str, Any]]] = {}
    for item in results:
        by_config.setdefault(item["config"], []).append(item)

    summary: dict[str, dict[str, Any]] = {}
    for name, items in by_config.items():
        ok = [item for item in items if not item.get("error")]
        metrics = [item.get("quality", {}) for item in ok]
        cost = _mean([item["cost_usd"] for item in ok])
        cost_batch = _mean([item["cost_usd_batch"] for item in ok])
        summary[name] = {
            "model": items[0]["model"],
            "mode": items[0]["mode"],
            "pages": len(items),
            "pages_ok": len(ok),
            "errors": [item["error"] for item in items if item.get("error")],
            "input_tokens": _mean([item["usage"].get("input_tokens", 0) for item in ok]),
            "output_tokens": _mean([item["usage"].get("output_tokens", 0) for item in ok]),
            "tiles_tokens_estimate": _mean([item["tiles_tokens_estimate"] for item in items]),
            "cost_usd_page": cost,
            "cost_usd_page_batch": cost_batch,
            "cost_usd_page_reported": _mean([item.get("cost_usd_reported") for item in ok]),
            "cost_usd_total": round(cost * extrapolate_pages, 2) if cost is not None else None,
            "cost_usd_total_batch": round(cost_batch * extrapolate_pages, 2) if cost_batch is not None else None,
            "elapsed_s": _mean([item["elapsed_s"] for item in ok]),
            "valid_word_ratio": _mean([m.get("valid_word_ratio") for m in metrics]),
            "ref_word_overlap": _mean([m.get("ref_word_overlap") for m in metrics]),
            "chars": _mean([m.get("chars") for m in metrics]),
            "segments": _mean([m.get("segments") for m in metrics]),
            "people": _mean([m.get("people") for m in metrics]),
            "relations": _mean([m.get("relations") for m in metrics]),
        }
    return summary


def render_summary_table(summary: dict[str, dict[str, Any]], extrapolate_pages: int) -> str:
    def fmt(value: Any, spec: str = "") -> str:
        if value is None:
            return "-"
        return format(value, spec) if spec else str(value)

    header = (
        f"| config | págs | tok in | tok in (est.) | tok out | US$/pág | US$/pág batch | US$ {extrapolate_pages:,} págs (batch) "
        "| palavras válidas | cobertura ref | segs | pessoas | relações | s/pág |"
    )
    lines = [header, "|" + "---|" * 14]
    for name, row in summary.items():
        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    f"{row['pages_ok']}/{row['pages']}",
                    fmt(row["input_tokens"], ",.0f"),
                    fmt(row["tiles_tokens_estimate"], ",.0f"),
                    fmt(row["output_tokens"], ",.0f"),
                    fmt(row["cost_usd_page"], ".4f"),
                    fmt(row["cost_usd_page_batch"], ".4f"),
                    fmt(row["cost_usd_total_batch"], ",.0f"),
                    fmt(row["valid_word_ratio"], ".2%"),
                    fmt(row["ref_word_overlap"], ".2%"),
                    fmt(row["segments"], ".1f"),
                    fmt(row["people"], ".1f"),
                    fmt(row["relations"], ".1f"),
                    fmt(row["elapsed_s"], ".0f"),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def run_benchmark(
    pages: list[str],
    config_names: Iterable[str] | None = None,
    *,
    grid: tuple[int, int] | None = None,
    backend: str | None = None,
    out_dir: Path | None = None,
    dry_run: bool = False,
    extrapolate_pages: int = DEFAULT_EXTRAPOLATION_PAGES,
    client=None,
    read_fn: Callable[..., ReadResult] = read_page,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Executa (ou só estima, em dry-run) todas as combinações e grava os artefatos."""
    configs = select_configs(config_names, grid, backend)
    out_dir = out_dir or (BENCHMARK_DIR / datetime.now().strftime("%Y%m%d_%H%M%S"))
    out_dir.mkdir(parents=True, exist_ok=True)
    say = progress or (lambda _msg: None)

    resolved = [resolve_page(page) for page in pages]
    missing = [item["id"] for item in resolved if item["image"] is None]
    available = [item for item in resolved if item["image"] is not None]

    results: list[dict[str, Any]] = []
    for page in available:
        reference_text = page["reference"].read_text(encoding="utf-8", errors="ignore") if page["reference"] else None
        tiles_cache: dict[tuple[int, int], list] = {}
        for config in configs:
            tiles = tiles_cache.get(config.grid)
            if tiles is None:
                tiles = tiles_cache[config.grid] = make_tiles(page["image"], grid=config.grid)

            if dry_run:
                if client is None:
                    from src.vision.reader import get_client

                    client = get_client()
                request = build_request(tiles, config)
                error = None
                try:
                    input_tokens = count_input_tokens(client, request)
                except Exception as exc:  # noqa: BLE001 - credencial/rede: registra e segue com a estimativa
                    input_tokens = 0
                    error = f"{type(exc).__name__}: {str(exc)[:160]}"
                usage = {"input_tokens": input_tokens, "output_tokens": 0}
                item = {
                    "config": config.name,
                    "model": config.model,
                    "mode": config.mode,
                    "page": page["id"],
                    "grid": list(config.grid),
                    "tiles": len(tiles),
                    "tiles_tokens_estimate": sum(tile.tokens_estimate for tile in tiles),
                    "tiles_bytes": sum(len(tile.jpeg) for tile in tiles),
                    "usage": usage,
                    "cost_usd": round(estimate_cost(usage, config.model), 6),
                    "cost_usd_batch": round(estimate_cost(usage, config.model, batch=True), 6),
                    "elapsed_s": 0.0,
                    "dry_run": True,
                    "quality": {},
                    "error": error,
                }
                estimate = item["tiles_tokens_estimate"]
                if error:
                    say(f"{page['id']} x {config.name}: {len(tiles)} tiles, ~{estimate:,} tokens estimados; contagem oficial falhou ({error})")
                else:
                    say(f"{page['id']} x {config.name}: {len(tiles)} tiles, {input_tokens:,} tokens de entrada (estimativa por pixel: {estimate:,})")
            else:
                result = read_fn(page["image"], config, client=client, tiles=tiles)
                item = result.to_dict()
                item["quality"] = quality_metrics(result.extraction, reference_text)
                thinking = result.usage.get("thinking_tokens", 0)
                status = f"ERRO {result.error}" if result.error else (
                    f"in={result.usage.get('input_tokens', 0):,} out={result.usage.get('output_tokens', 0):,}"
                    + (f" (think={thinking:,})" if thinking else "")
                    + f" US${result.cost_usd:.4f} {result.elapsed_s:.0f}s"
                    + f" segs={len((result.extraction or {}).get('segments', []))}"
                )
                say(f"{page['id']} x {config.name}: {status}")

            item["reference"] = str(page["reference"]) if page["reference"] else None
            (out_dir / f"{page['id']}__{config.name}.json").write_text(
                json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            results.append(item)

    summary = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "out_dir": str(out_dir),
        "dry_run": dry_run,
        "extrapolate_pages": extrapolate_pages,
        "pages": [item["id"] for item in available],
        "missing_images": missing,
        "configs": [config.name for config in configs],
        "backend": backend or "api",
        "by_config": aggregate(results, extrapolate_pages),
        "results": results,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    table = render_summary_table(summary["by_config"], extrapolate_pages)
    (out_dir / "summary.md").write_text(
        f"# Benchmark visão-primeiro ({summary['created_at']})\n\n"
        f"Páginas: {', '.join(summary['pages']) or '-'}\n\n"
        + (f"Sem imagem local: {', '.join(missing)}\n\n" if missing else "")
        + table
        + "\n",
        encoding="utf-8",
    )
    summary["table"] = table
    return summary
