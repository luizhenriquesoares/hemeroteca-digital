"""Benchmark de recuperação para busca textual, semântica e híbrida."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Optional

from src.config import BASE_DIR
from src.processing.search import (
    buscar_hibrida,
    buscar_semantica,
    buscar_textual_historica,
)

BENCHMARK_DIR = BASE_DIR / "data" / "benchmarks"
BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)

SearchFn = Callable[[str, int, Optional[str]], list[dict]]


@dataclass
class SearchBenchmarkCase:
    id: str
    query: str
    relevant_ids: list[str]
    relevant_pages: list[str]
    notes: str = ""
    filtro_bib: str = ""

    def target_keys(self) -> list[str]:
        if self.relevant_ids:
            return list(dict.fromkeys(self.relevant_ids))
        return list(dict.fromkeys(self.relevant_pages))

    def target_mode(self) -> str:
        return "ids" if self.relevant_ids else "pages"


@dataclass
class SearchBenchmarkQueryResult:
    case_id: str
    query: str
    run_label: str
    mode: str
    ok: bool
    elapsed_ms: float
    n_results: int
    target_mode: str
    target_count: int
    matched_targets: int
    hit_at_k: bool
    recall_at_k: float
    reciprocal_rank: float
    first_relevant_rank: int | None
    ndcg_at_k: float
    top_results: list[dict]
    notes: str = ""
    error: str = ""


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip("-")


def _normalize_page(value) -> str:
    if isinstance(value, dict):
        bib = str(value.get("bib", "")).strip()
        pagina = str(value.get("pagina", "")).strip()
        if bib and pagina:
            return f"{bib}:{pagina}"
        return ""

    raw = str(value or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        bib, pagina = raw.split(":", 1)
        bib = bib.strip()
        pagina = pagina.strip()
        return f"{bib}:{pagina}" if bib and pagina else ""
    return ""


def _coerce_case(index: int, raw: dict) -> SearchBenchmarkCase:
    case_id = str(raw.get("id") or f"case_{index + 1}").strip()
    query = str(raw.get("query") or "").strip()
    relevant_ids = [str(item).strip() for item in raw.get("relevant_ids", []) if str(item).strip()]
    relevant_pages = [_normalize_page(item) for item in raw.get("relevant_pages", [])]
    relevant_pages = [item for item in relevant_pages if item]
    notes = str(raw.get("notes") or "").strip()
    filtro_bib = str(raw.get("filtro_bib") or "").strip()

    if not query:
        raise ValueError(f"Caso {case_id!r} sem query")
    if not relevant_ids and not relevant_pages:
        raise ValueError(
            f"Caso {case_id!r} precisa de ao menos um alvo em relevant_ids ou relevant_pages"
        )

    return SearchBenchmarkCase(
        id=case_id,
        query=query,
        relevant_ids=relevant_ids,
        relevant_pages=relevant_pages,
        notes=notes,
        filtro_bib=filtro_bib,
    )


def load_cases(path: Path) -> list[SearchBenchmarkCase]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_cases = payload.get("cases", payload) if isinstance(payload, dict) else payload
    if not isinstance(raw_cases, list):
        raise ValueError("Arquivo de benchmark deve conter uma lista ou um objeto com chave 'cases'")
    return [_coerce_case(index, raw) for index, raw in enumerate(raw_cases)]


def write_template(path: Path) -> Path:
    template = {
        "description": (
            "Template para benchmark de busca. "
            "Use relevant_ids para chunks/documentos exatos ou relevant_pages para avaliar no nivel de pagina. "
            "Quando relevant_ids estiver preenchido, ele tem precedencia sobre relevant_pages."
        ),
        "cases": [
            {
                "id": "sobrenome_botelho",
                "query": "informacoes sobre os botelhos",
                "relevant_pages": ["029033_02:1"],
                "notes": "Exemplo de pesquisa por sobrenome/familia",
            },
            {
                "id": "antonio_benedicto_araujo",
                "query": "Antonio Benedito de Araujo Pernambuco",
                "relevant_ids": ["029033_02_029033_02_00001_chunk0"],
                "notes": "Exemplo de query nominal exata",
            },
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def default_runs() -> list[tuple[str, str, SearchFn]]:
    return [
        ("textual", "textual_historica", buscar_textual_historica),
        ("semantica", "semantica", buscar_semantica),
        ("hibrida", "hibrida", buscar_hibrida),
    ]


def select_runs(labels: list[str] | tuple[str, ...]) -> list[tuple[str, str, SearchFn]]:
    available = {label: run for label, *run in default_runs()}
    selected = []
    for label in labels:
        if label not in available:
            raise ValueError(f"Run desconhecido: {label}")
        mode, fn = available[label]
        selected.append((label, mode, fn))
    return selected


def _page_key_from_result(result: dict) -> str:
    metadata = result.get("metadata", {}) or {}
    bib = str(metadata.get("bib", "")).strip()
    pagina = str(metadata.get("pagina", "")).strip()
    return f"{bib}:{pagina}" if bib and pagina else ""


def _matches_target(case: SearchBenchmarkCase, result: dict) -> str | None:
    if case.relevant_ids:
        result_id = str(result.get("id", "")).strip()
        return result_id if result_id in case.target_keys() else None

    page_key = _page_key_from_result(result)
    return page_key if page_key in case.target_keys() else None


def _serialize_top_results(results: list[dict], limit: int = 5) -> list[dict]:
    output = []
    for item in results[:limit]:
        metadata = item.get("metadata", {}) or {}
        output.append(
            {
                "id": item.get("id", ""),
                "score": item.get("score", 0.0),
                "modo": item.get("modo", ""),
                "bib": metadata.get("bib", ""),
                "pagina": metadata.get("pagina", ""),
                "jornal": metadata.get("jornal") or metadata.get("periodico") or "",
            }
        )
    return output


def _dcg(binary_relevance: list[int]) -> float:
    total = 0.0
    for index, rel in enumerate(binary_relevance):
        if rel:
            total += rel / math.log2(index + 2)
    return total


def _evaluate_case(
    *,
    case: SearchBenchmarkCase,
    label: str,
    mode: str,
    search_fn: SearchFn,
    n_results: int,
    filtro_bib: str | None,
) -> SearchBenchmarkQueryResult:
    t0 = time.time()
    try:
        results = search_fn(case.query, n_results=n_results, filtro_bib=case.filtro_bib or filtro_bib)
        elapsed_ms = round((time.time() - t0) * 1000, 2)
    except Exception as exc:
        return SearchBenchmarkQueryResult(
            case_id=case.id,
            query=case.query,
            run_label=label,
            mode=mode,
            ok=False,
            elapsed_ms=round((time.time() - t0) * 1000, 2),
            n_results=n_results,
            target_mode=case.target_mode(),
            target_count=len(case.target_keys()),
            matched_targets=0,
            hit_at_k=False,
            recall_at_k=0.0,
            reciprocal_rank=0.0,
            first_relevant_rank=None,
            ndcg_at_k=0.0,
            top_results=[],
            notes=case.notes,
            error=str(exc),
        )

    matched_targets: set[str] = set()
    binary_relevance: list[int] = []
    first_rank = None
    for index, result in enumerate(results[:n_results], start=1):
        matched = _matches_target(case, result)
        is_relevant = matched is not None
        binary_relevance.append(1 if is_relevant else 0)
        if matched:
            matched_targets.add(matched)
            if first_rank is None:
                first_rank = index

    target_count = len(case.target_keys())
    recall = round(len(matched_targets) / target_count, 4) if target_count else 0.0
    reciprocal_rank = round(1 / first_rank, 4) if first_rank else 0.0
    ideal_relevance = [1] * min(target_count, n_results)
    dcg = _dcg(binary_relevance)
    idcg = _dcg(ideal_relevance)
    ndcg = round(dcg / idcg, 4) if idcg else 0.0

    return SearchBenchmarkQueryResult(
        case_id=case.id,
        query=case.query,
        run_label=label,
        mode=mode,
        ok=True,
        elapsed_ms=elapsed_ms,
        n_results=n_results,
        target_mode=case.target_mode(),
        target_count=target_count,
        matched_targets=len(matched_targets),
        hit_at_k=first_rank is not None,
        recall_at_k=recall,
        reciprocal_rank=reciprocal_rank,
        first_relevant_rank=first_rank,
        ndcg_at_k=ndcg,
        top_results=_serialize_top_results(results),
        notes=case.notes,
    )


def _aggregate_results(label: str, mode: str, results: list[SearchBenchmarkQueryResult], n_results: int) -> dict:
    total = len(results)
    if not total:
        return {
            "label": label,
            "mode": mode,
            "queries": 0,
            f"hit_rate_at_{n_results}": 0.0,
            f"mean_recall_at_{n_results}": 0.0,
            "mrr": 0.0,
            f"ndcg_at_{n_results}": 0.0,
            "avg_elapsed_ms": 0.0,
            "errors": 0,
            "results": [],
        }

    return {
        "label": label,
        "mode": mode,
        "queries": total,
        f"hit_rate_at_{n_results}": round(sum(1 for item in results if item.hit_at_k) / total, 4),
        f"mean_recall_at_{n_results}": round(sum(item.recall_at_k for item in results) / total, 4),
        "mrr": round(sum(item.reciprocal_rank for item in results) / total, 4),
        f"ndcg_at_{n_results}": round(sum(item.ndcg_at_k for item in results) / total, 4),
        "avg_elapsed_ms": round(sum(item.elapsed_ms for item in results) / total, 2),
        "errors": sum(1 for item in results if not item.ok),
        "results": [asdict(item) for item in results],
    }


def run_benchmark(
    cases_path: Path,
    *,
    runs: list[tuple[str, str, SearchFn]] | None = None,
    n_results: int = 10,
    filtro_bib: str | None = None,
) -> dict:
    cases = load_cases(cases_path)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = BENCHMARK_DIR / f"{cases_path.stem}_search_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_summaries = []
    for label, mode, search_fn in (runs or default_runs()):
        query_results = [
            _evaluate_case(
                case=case,
                label=label,
                mode=mode,
                search_fn=search_fn,
                n_results=n_results,
                filtro_bib=filtro_bib,
            )
            for case in cases
        ]
        run_summaries.append(_aggregate_results(label, mode, query_results, n_results))

    summary = {
        "cases_path": str(cases_path),
        "out_dir": str(out_dir),
        "n_results": n_results,
        "filtro_bib": filtro_bib or "",
        "cases": [asdict(case) for case in cases],
        "runs": run_summaries,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
