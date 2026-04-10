"""API FastAPI para busca na Hemeroteca Digital PE."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from fastapi import FastAPI, File, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from src.config import IMAGES_DIR, DATA_DIR, CACHE_DIR, TEXT_DIR
from src.structured.repository import StructuredRepository
from src.processing.search import (
    buscar_hibrida,
    buscar_textual_historica,
    normalize_text,
)
from src.web.entity_service import build_fallback_page, serialize_entity, serialize_entity_search_results
from src.web.page_view import render_page_view
from src.web.page_utils import (
    busca_textual as _busca_textual,
    count_page_texts as _count_page_texts_impl,
    load_progress_status as _load_progress_status_impl,
    resolve_image_url as _resolve_image_url_impl,
    serialize_page_record as _serialize_page_record_impl,
)
from src.web.rag_service import build_rag_response, generate_corpus_summary, generate_entity_bio
from src.web.stats_service import build_stats, load_acervos
from src.web.structured_response import (
    build_prosopographic_fallback as _build_prosopographic_fallback,
    extract_person_mentions as _extract_person_mentions,
    parse_structured_answer as _parse_structured_answer,
)

load_dotenv()

logger = logging.getLogger(__name__)

_YEAR_RE = re.compile(r"(1[0-9]{3}|20[0-9]{2})")
_ROUTE_CACHE: dict[tuple, tuple[float, object]] = {}

app = FastAPI(title="Hemeroteca Digital PE", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir imagens estáticas
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")


def _count_page_texts() -> int:
    return _count_page_texts_impl(TEXT_DIR)


def _load_progress_status() -> tuple[int, int]:
    return _load_progress_status_impl(CACHE_DIR)


def _resolve_image_url(bib: str, pagina: str) -> str | None:
    return _resolve_image_url_impl(IMAGES_DIR, bib, pagina)


def _serialize_page_record(page: dict) -> dict:
    return _serialize_page_record_impl(page, IMAGES_DIR)


def _parse_year_value(value: str | int | None) -> int | None:
    if value is None:
        return None
    match = _YEAR_RE.search(str(value))
    return int(match.group(1)) if match else None


def _filter_results_by_year(results: list[dict], year_from: int | None = None, year_to: int | None = None) -> list[dict]:
    if year_from is None and year_to is None:
        return results

    filtered = []
    for item in results:
        metadata = item.get("metadata", {})
        year = _parse_year_value(metadata.get("ano"))
        if year is None:
            continue
        if year_from is not None and year < year_from:
            continue
        if year_to is not None and year > year_to:
            continue
        filtered.append(item)
    return filtered


def _serialize_discovery_overview(overview: dict) -> dict:
    for hotspot in overview.get("document_hotspots", []):
        _attach_page_links(hotspot)
    return overview


def _serialize_entity_comparison(payload: dict) -> dict:
    for page in payload.get("shared_pages", []):
        _attach_page_links(page)
    return payload


def _attach_page_links(item: dict, highlight: str | None = None) -> dict:
    bib = str(item.get("bib") or "")
    pagina = str(item.get("pagina") or "")
    if not bib or not pagina:
        return item
    item["page_api_url"] = f"/api/page/{bib}/{pagina}"
    item["page_view_url"] = f"/page/{bib}/{pagina}"
    if highlight:
        item["page_view_url"] += f"?q={quote(str(highlight))}"
    item["image_url"] = _resolve_image_url(bib, pagina)
    return item


def _serialize_featured_entity(payload: dict) -> dict:
    snippet = payload.get("top_snippet")
    if isinstance(snippet, dict):
        _attach_page_links(
            snippet,
            highlight=snippet.get("surface_form") or payload.get("canonical_name"),
        )
    return payload


def _cache_get(key: tuple, ttl_seconds: float):
    entry = _ROUTE_CACHE.get(key)
    if not entry:
        return None
    created_at, value = entry
    if (time.monotonic() - created_at) > ttl_seconds:
        _ROUTE_CACHE.pop(key, None)
        return None
    return value


def _cache_set(key: tuple, value: object):
    _ROUTE_CACHE[key] = (time.monotonic(), value)
    return value


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve o frontend."""
    frontend_path = Path(__file__).parent.parent.parent / "frontend" / "index.html"
    return frontend_path.read_text(encoding="utf-8")


@app.get("/api/buscar")
async def buscar(
    q: str = Query(..., description="Texto de busca"),
    n: int = Query(10, ge=1, le=50, description="Número de resultados"),
    bib: str = Query(None, description="Filtrar por acervo"),
    modo: str = Query("semantica", description="Modo: semantica ou textual"),
    score_min: float = Query(0.45, ge=0, le=1, description="Score mínimo de relevância"),
    year_from: int = Query(None, ge=1800, le=2100, description="Ano inicial opcional"),
    year_to: int = Query(None, ge=1800, le=2100, description="Ano final opcional"),
):
    """Busca nos jornais (semântica ou textual)."""

    if modo == "textual":
        resultados = buscar_textual_historica(
            q,
            n_results=max(n * 3, 30) if (year_from or year_to) else n,
            filtro_bib=bib,
        )
    else:
        candidate_n = min(max(n * 6, 50), 150) if (year_from or year_to) else min(n * 3, 50)
        resultados = buscar_hibrida(q, n_results=candidate_n, filtro_bib=bib)

    resultados = _filter_results_by_year(resultados, year_from=year_from, year_to=year_to)
    filtrados = [r for r in resultados if r["score"] >= score_min]
    return {"query": q, "total": len(filtrados[:n]), "resultados": filtrados[:n]}

@app.get("/api/stats")
async def get_stats():
    cache_key = ("stats", str(IMAGES_DIR), str(CACHE_DIR), str(TEXT_DIR), str(DATA_DIR))
    cached = _cache_get(cache_key, ttl_seconds=30.0)
    if cached is not None:
        return cached
    stats = await run_in_threadpool(
        build_stats,
        images_dir=IMAGES_DIR,
        cache_dir=CACHE_DIR,
        count_page_texts_fn=_count_page_texts,
        load_progress_status_fn=_load_progress_status,
    )
    return _cache_set(cache_key, stats)


@app.get("/api/rag")
async def rag_search(
    q: str = Query(..., description="Pergunta do usuário"),
    n: int = Query(15, ge=1, le=30, description="Chunks para contexto"),
    bib: str = Query(None, description="Filtrar por acervo"),
    year_from: int = Query(None, ge=1800, le=2100, description="Ano inicial opcional"),
    year_to: int = Query(None, ge=1800, le=2100, description="Ano final opcional"),
):
    candidate_n = max(n * 4, 40) if (year_from or year_to) else n
    resultados = buscar_hibrida(q, n_results=candidate_n, filtro_bib=bib)
    resultados = _filter_results_by_year(resultados, year_from=year_from, year_to=year_to)[:n]
    return build_rag_response(
        question=q,
        resultados=resultados,
        resolve_image_url_fn=_resolve_image_url,
        parse_structured_answer_fn=_parse_structured_answer,
        build_prosopographic_fallback_fn=_build_prosopographic_fallback,
    )


@app.get("/api/acervos")
async def listar_acervos():
    cache_key = ("acervos", str(CACHE_DIR))
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    acervos = await run_in_threadpool(load_acervos, CACHE_DIR)
    return _cache_set(cache_key, acervos)


@app.get("/api/discovery/overview")
async def discovery_overview(
    bib: str = Query(None, description="Filtrar por acervo"),
    year_from: int = Query(None, ge=1800, le=2100, description="Ano inicial opcional"),
    year_to: int = Query(None, ge=1800, le=2100, description="Ano final opcional"),
    limit: int = Query(8, ge=3, le=20, description="Quantidade por bloco"),
):
    cache_key = ("discovery_overview", bib or "", year_from or "", year_to or "", limit)
    cached = _cache_get(cache_key, ttl_seconds=45.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    overview = await run_in_threadpool(
        repo.get_discovery_overview,
        bib=bib,
        year_from=year_from,
        year_to=year_to,
        limit=limit,
    )
    return _cache_set(cache_key, _serialize_discovery_overview(overview))


@app.get("/api/discovery/featured-entity")
async def discovery_featured_entity(
    seed: int = Query(None, description="Seed opcional para seleção determinística"),
):
    effective_seed = int(seed) if seed is not None else int(datetime.now().astimezone().strftime("%j"))
    cache_key = ("featured_entity", effective_seed)
    cached = _cache_get(cache_key, ttl_seconds=3600.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    payload = await run_in_threadpool(repo.get_featured_entity, effective_seed)
    if not payload:
        return {"error": "Nenhuma entidade elegível encontrada"}
    return _cache_set(cache_key, _serialize_featured_entity(payload))


@app.get("/api/discovery/surnames")
async def discovery_surnames(
    q: str = Query(..., description="Sobrenome ou base nominal"),
    n: int = Query(8, ge=1, le=20, description="Número máximo de grupos"),
):
    repo = StructuredRepository()
    groups = await run_in_threadpool(repo.search_by_surname, q, n)
    return {"query": q, "total": len(groups), "groups": groups}


@app.get("/api/discovery/trails")
async def discovery_trails():
    """Lista trilhas temáticas disponíveis."""
    repo = StructuredRepository()
    return await run_in_threadpool(repo.list_trails)


@app.get("/api/discovery/trail/{trail_name}")
async def discovery_trail(
    trail_name: str,
    limit: int = Query(8, ge=3, le=20, description="Quantidade por bloco"),
):
    """Retorna dados de uma trilha temática."""
    cache_key = ("trail", trail_name, limit)
    cached = _cache_get(cache_key, ttl_seconds=120.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    trail = await run_in_threadpool(repo.get_trail, trail_name, limit=limit)
    if not trail:
        return {"error": f"Trilha '{trail_name}' não encontrada"}
    return _cache_set(cache_key, trail)


@app.get("/api/discovery/summary")
async def discovery_summary():
    """Resumo narrativo do acervo gerado por LLM (cacheado 24h)."""
    cache_key = ("corpus_summary",)
    cached = _cache_get(cache_key, ttl_seconds=86400.0)
    if cached is not None:
        return cached

    # Carregar de arquivo se existir
    summary_file = DATA_DIR / "cache" / "corpus_summary.json"
    if summary_file.exists():
        import json as _json
        try:
            data = _json.loads(summary_file.read_text(encoding="utf-8"))
            return _cache_set(cache_key, data)
        except Exception:
            pass

    try:
        repo = StructuredRepository()
        overview = await run_in_threadpool(
            repo.get_discovery_overview, limit=8,
        )
        text = await run_in_threadpool(generate_corpus_summary, overview)
        result = {"summary": text}
        # Persistir em disco
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        summary_file.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return _cache_set(cache_key, result)
    except Exception as exc:
        logger.warning("Falha ao gerar resumo do acervo: %s", exc)
        return {"summary": None, "error": str(exc)}


@app.get("/api/entity/{entity_id}/bio")
async def get_entity_bio(entity_id: int):
    """Retorna ou gera mini-biografia de uma entidade."""
    # Verificar se já existe em disco
    bio_file = DATA_DIR / "structured" / "bios" / f"{entity_id}.json"
    if bio_file.exists():
        import json as _json
        try:
            return _json.loads(bio_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    try:
        repo = StructuredRepository()
        entity = await run_in_threadpool(repo.get_entity, entity_id)
        if not entity:
            return {"error": "Entidade não encontrada"}
        text = await run_in_threadpool(generate_entity_bio, entity)
        result = {"entity_id": entity_id, "bio": text}
        bio_file.parent.mkdir(parents=True, exist_ok=True)
        bio_file.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result
    except Exception as exc:
        logger.warning("Falha ao gerar biografia para entity %s: %s", entity_id, exc)
        return {"entity_id": entity_id, "bio": None, "error": str(exc)}


@app.get("/api/entity/search")
async def entity_search(
    q: str = Query(..., description="Nome ou sobrenome a buscar"),
    n: int = Query(10, ge=1, le=50, description="Número máximo de entidades"),
):
    repo = StructuredRepository()
    results = await run_in_threadpool(repo.search_entities, normalize_text(q), limit=n)
    results = serialize_entity_search_results(results)
    return {"query": q, "total": len(results), "resultados": results}


@app.get("/api/entity/{entity_id}/graph")
async def get_entity_graph(
    entity_id: int,
    depth: int = Query(2, ge=1, le=3, description="Profundidade do subgrafo"),
):
    """Retorna subgrafo centrado em uma entidade para visualização."""
    from src.structured.graph_store import get_entity_subgraph
    return get_entity_subgraph(entity_id, depth=depth)


@app.get("/api/acores/stats")
async def acores_stats():
    """Estatísticas do acervo paroquial dos Açores."""
    from src.acores.graph import get_parish_stats
    cache_key = ("acores_stats",)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    return _cache_set(cache_key, await run_in_threadpool(get_parish_stats))


@app.get("/api/acores/graph")
async def acores_graph():
    """Grafo genealógico completo dos registros paroquiais."""
    from src.acores.graph import build_parish_graph
    cache_key = ("acores_graph",)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    return _cache_set(cache_key, await run_in_threadpool(build_parish_graph))


@app.get("/api/acores/family/{person_name}")
async def acores_family(person_name: str, depth: int = Query(2, ge=1, le=3)):
    """Subgrafo genealógico centrado em uma pessoa."""
    from src.acores.graph import get_family_subgraph
    return await run_in_threadpool(get_family_subgraph, person_name, depth=depth)


@app.get("/api/acores/cross-reference")
async def acores_cross_reference(limit: int = Query(30, ge=5, le=100)):
    """Cruzamento de sobrenomes Açores × Pernambuco."""
    from src.acores.graph import cross_reference_pe
    cache_key = ("acores_crossref", limit)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    matches = await run_in_threadpool(cross_reference_pe)
    return _cache_set(cache_key, {"total": len(matches), "matches": matches[:limit]})


@app.get("/api/acores/family-trees")
async def acores_family_trees(min_descendants: int = Query(3, ge=2, le=20)):
    """Árvores genealógicas automáticas construídas dos registros."""
    from src.acores.graph import build_family_trees
    cache_key = ("acores_trees", min_descendants)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    trees = await run_in_threadpool(build_family_trees, min_descendants=min_descendants)
    return _cache_set(cache_key, {"total": len(trees), "trees": trees[:20]})


@app.get("/api/acores/search")
async def acores_search(q: str = Query(...), limit: int = Query(20, ge=1, le=50)):
    """Busca nos registros paroquiais."""
    from src.acores.repository import ParishRepository
    repo = ParishRepository()
    results = await run_in_threadpool(repo.search_by_surname, q, limit)
    return {"query": q, "total": len(results), "results": results}


@app.get("/api/entity/{entity_id}/family-tree")
async def get_family_tree(entity_id: int):
    """Árvore genealógica centrada em uma entidade."""
    repo = StructuredRepository()
    return await run_in_threadpool(repo.get_family_tree, entity_id)


@app.get("/api/period/{year}")
async def get_period(
    year: int,
    limit: int = Query(8, ge=3, le=20),
):
    """Dados narrativos de um período específico."""
    cache_key = ("period", year, limit)
    cached = _cache_get(cache_key, ttl_seconds=120.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    data = await run_in_threadpool(repo.get_period_narrative, year, limit=limit)
    return _cache_set(cache_key, data)


@app.get("/api/discovery/featured-graph")
async def discovery_featured_graph(
    limit: int = Query(25, ge=5, le=50, description="Número máximo de nós"),
):
    """Grafo das entidades mais conectadas para a homepage."""
    cache_key = ("featured_graph", limit)
    cached = _cache_get(cache_key, ttl_seconds=120.0)
    if cached is not None:
        return cached
    from src.structured.graph_store import get_featured_graph
    graph = await run_in_threadpool(get_featured_graph, limit=limit)
    return _cache_set(cache_key, graph)


@app.get("/api/discovery/graph")
async def discovery_layered_graph(
    layers: str = Query("family,roles,co_mention", description="Camadas separadas por vírgula"),
    limit: int = Query(30, ge=5, le=80, description="Número máximo de nós"),
    focus: int = Query(0, ge=0, description="ID da entidade para ego-network (0 = visão global)"),
    depth: int = Query(2, ge=1, le=3, description="Profundidade do BFS (apenas com focus)"),
):
    """Grafo interativo com filtro por camadas semânticas.

    Sem ``focus``: visão global das entidades mais conectadas.
    Com ``focus=ID``: ego-network centrado na entidade, BFS até ``depth`` saltos.
    """
    layers_list = sorted({l.strip() for l in layers.split(",") if l.strip()})
    focus_id = focus if focus and focus > 0 else None
    cache_key = ("layered_graph", tuple(layers_list), limit, focus_id, depth)
    cached = _cache_get(cache_key, ttl_seconds=120.0)
    if cached is not None:
        return cached
    from src.structured.graph_store import get_layered_graph
    data = await run_in_threadpool(
        get_layered_graph,
        layers=layers_list,
        limit=limit,
        focus_entity_id=focus_id,
        focus_depth=depth,
    )
    return _cache_set(cache_key, data)


@app.get("/api/graph/edge-evidence")
async def graph_edge_evidence(
    source: int = Query(..., ge=1, description="ID da entidade de origem"),
    target: int = Query(..., ge=1, description="ID da entidade de destino"),
):
    """Evidências documentais para uma aresta do grafo (página onde aparecem juntas + relações diretas)."""
    cache_key = ("edge_evidence", source, target)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    from src.structured.graph_store import get_edge_evidence
    data = await run_in_threadpool(get_edge_evidence, source, target)
    return _cache_set(cache_key, data)


@app.get("/api/discovery/surname-cloud")
async def discovery_surname_cloud(
    limit: int = Query(40, ge=10, le=100, description="Número de sobrenomes"),
):
    """Nuvem de sobrenomes mais frequentes."""
    cache_key = ("surname_cloud", limit)
    cached = _cache_get(cache_key, ttl_seconds=300.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    cloud = await run_in_threadpool(repo.get_surname_cloud, limit=limit)
    return _cache_set(cache_key, cloud)


@app.get("/api/entity/{entity_id}")
async def get_entity(entity_id: int):
    repo = StructuredRepository()
    entity = await run_in_threadpool(repo.get_entity, entity_id)
    if not entity:
        return {"error": "Entidade não encontrada"}
    return serialize_entity(entity, _resolve_image_url)


@app.get("/api/entity-compare")
async def compare_entities(
    left_id: int = Query(..., ge=1, description="ID da entidade à esquerda"),
    right_id: int = Query(..., ge=1, description="ID da entidade à direita"),
    limit: int = Query(8, ge=3, le=20, description="Quantidade máxima por seção"),
):
    repo = StructuredRepository()
    payload = await run_in_threadpool(repo.get_entity_comparison, left_id, right_id, limit=limit)
    if not payload:
        return {"error": "Comparação não disponível para as entidades informadas"}
    return _serialize_entity_comparison(payload)


@app.get("/api/review/queue")
async def review_queue(
    n: int = Query(12, ge=1, le=50, description="Quantidade máxima por seção"),
):
    cache_key = ("review_queue", n)
    cached = _cache_get(cache_key, ttl_seconds=20.0)
    if cached is not None:
        return cached
    repo = StructuredRepository()
    queue = await run_in_threadpool(repo.get_review_queue, limit=n)
    payload = {
        "identities_total": len(queue["identities"]),
        "relations_total": len(queue["relations"]),
        "suspects_total": len(queue.get("suspects", [])),
        "merges_total": len(queue.get("merges", [])),
        "identities": queue["identities"],
        "relations": queue["relations"],
        "suspects": queue.get("suspects", []),
        "merges": queue.get("merges", []),
    }
    return _cache_set(cache_key, payload)


@app.post("/api/relation/{relation_id}/review")
async def review_relation(
    relation_id: int,
    status: str = Query(..., description="Novo status: hypothesis, probable, confirmed, rejected"),
    note: str = Query("", description="Observação opcional"),
    reviewer: str = Query("humano", description="Identificador do revisor"),
):
    repo = StructuredRepository()
    review = repo.review_relation(relation_id, review_status=status, reviewer=reviewer, note=note)
    if not review:
        return {"error": "Relação não encontrada"}
    return {"ok": True, "review": review}


@app.post("/api/entity/{entity_id}/identity-review")
async def review_entity_identity(
    entity_id: int,
    status: str = Query(..., description="Novo status de identidade: resolved, contextual, ambiguous, rejected, merged"),
    note: str = Query("", description="Observação opcional"),
    reviewer: str = Query("humano", description="Identificador do revisor"),
):
    repo = StructuredRepository()
    review = repo.review_entity_identity(entity_id, review_status=status, reviewer=reviewer, note=note)
    if not review:
        return {"error": "Entidade não encontrada"}
    return {"ok": True, "review": review}


@app.post("/api/entity/{source_entity_id}/merge-into/{target_entity_id}")
async def merge_entities(
    source_entity_id: int,
    target_entity_id: int,
    note: str = Query("", description="Observação opcional"),
    reviewer: str = Query("humano", description="Identificador do revisor"),
):
    repo = StructuredRepository()
    payload = repo.merge_entities(
        source_entity_id,
        target_entity_id,
        reviewer=reviewer,
        note=note,
    )
    if not payload:
        return {"error": "Não foi possível consolidar essas entidades"}
    return {"ok": True, "merge": payload}


@app.post("/api/entity-merge/{source_entity_id}/{target_entity_id}/review")
async def review_entity_merge(
    source_entity_id: int,
    target_entity_id: int,
    status: str = Query(..., description="Novo status da sugestão: approved ou rejected"),
    note: str = Query("", description="Observação opcional"),
    reviewer: str = Query("humano", description="Identificador do revisor"),
):
    repo = StructuredRepository()
    review = repo.review_entity_merge_suggestion(
        source_entity_id,
        target_entity_id,
        review_status=status,
        reviewer=reviewer,
        note=note,
    )
    if not review:
        return {"error": "Sugestão de merge inválida"}
    return {"ok": True, "review": review}


@app.get("/api/graph/stats")
async def graph_stats():
    """Estatísticas do grafo exportado."""
    import json as _json
    graph_json = DATA_DIR / "graph" / "hemeroteca.json"
    if not graph_json.exists():
        return {"error": "Grafo não exportado. Execute: python main.py exportar-grafo"}
    data = _json.loads(graph_json.read_text(encoding="utf-8"))
    return data.get("stats", {})


@app.get("/api/page/{bib}/{pagina}")
async def get_page(bib: str, pagina: str):
    repo = StructuredRepository()
    page = repo.get_page(bib, pagina)
    if page:
        return _serialize_page_record(page)
    fallback_page = build_fallback_page(bib=bib, pagina=pagina, text_dir=TEXT_DIR, images_dir=IMAGES_DIR)
    if not fallback_page:
        return {"error": "Página não encontrada"}
    return _serialize_page_record(fallback_page)


@app.get("/page/{bib}/{pagina}", response_class=HTMLResponse)
async def view_page(
    bib: str,
    pagina: str,
    q: str = Query("", description="Termo para destacar na transcrição"),
):
    repo = StructuredRepository()
    page = repo.get_page(bib, pagina)
    if page:
        return render_page_view(_serialize_page_record(page), query=q)
    fallback_page = build_fallback_page(bib=bib, pagina=pagina, text_dir=TEXT_DIR, images_dir=IMAGES_DIR)
    if not fallback_page:
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return render_page_view(_serialize_page_record(fallback_page), query=q)


# --- Endpoints temporários de transferência de dados ---
_UPLOAD_SECRET = "hemeroteca2026"

@app.post("/api/_upload_chunk")
async def upload_chunk(
    secret: str = Query(...),
    file: UploadFile = File(...),
    append: bool = Query(False, description="Append ao arquivo existente em vez de sobrescrever"),
):
    if secret != _UPLOAD_SECRET:
        return {"error": "unauthorized"}
    upload_path = DATA_DIR / "_upload.tar.gz"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "ab" if append else "wb"
    with open(upload_path, mode) as f:
        while chunk := await file.read(1024 * 1024):
            f.write(chunk)
    size = upload_path.stat().st_size
    return {"status": "uploaded", "size": size, "mode": "append" if append else "create"}

@app.post("/api/_extract_upload")
async def extract_upload(secret: str = Query(...)):
    if secret != _UPLOAD_SECRET:
        return {"error": "unauthorized"}
    import subprocess
    upload_path = DATA_DIR / "_upload.tar.gz"
    if not upload_path.exists():
        return {"error": "no upload found"}
    result = subprocess.run(
        ["tar", "-xzf", str(upload_path), "--strip-components=1", "-C", str(DATA_DIR)],
        capture_output=True, text=True,
    )
    upload_path.unlink(missing_ok=True)
    return {"status": "extracted", "stdout": result.stdout, "stderr": result.stderr}

@app.post("/api/_clear_upload")
async def clear_upload(secret: str = Query(...)):
    if secret != _UPLOAD_SECRET:
        return {"error": "unauthorized"}
    upload_path = DATA_DIR / "_upload.tar.gz"
    upload_path.unlink(missing_ok=True)
    return {"status": "cleared"}
