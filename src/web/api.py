"""API FastAPI para busca na Hemeroteca Digital PE."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.config import IMAGES_DIR, DATA_DIR, CACHE_DIR, TEXT_DIR
from src.structured.repository import StructuredRepository
from src.processing.search import (
    buscar_hibrida,
    buscar_textual_historica,
    normalize_text,
)
from src.web.entity_service import build_fallback_page, serialize_entity, serialize_entity_search_results
from src.web.page_utils import (
    busca_textual as _busca_textual,
    count_page_texts as _count_page_texts_impl,
    load_progress_status as _load_progress_status_impl,
    resolve_image_url as _resolve_image_url_impl,
    serialize_page_record as _serialize_page_record_impl,
)
from src.web.rag_service import build_rag_response
from src.web.stats_service import build_stats, load_acervos
from src.web.structured_response import (
    build_prosopographic_fallback as _build_prosopographic_fallback,
    extract_person_mentions as _extract_person_mentions,
    parse_structured_answer as _parse_structured_answer,
)

load_dotenv()

logger = logging.getLogger(__name__)

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


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve o frontend."""
    frontend_path = Path(__file__).parent.parent / "frontend" / "index.html"
    return frontend_path.read_text(encoding="utf-8")


@app.get("/api/buscar")
async def buscar(
    q: str = Query(..., description="Texto de busca"),
    n: int = Query(10, ge=1, le=50, description="Número de resultados"),
    bib: str = Query(None, description="Filtrar por acervo"),
    modo: str = Query("semantica", description="Modo: semantica ou textual"),
    score_min: float = Query(0.45, ge=0, le=1, description="Score mínimo de relevância"),
):
    """Busca nos jornais (semântica ou textual)."""

    if modo == "textual":
        resultados = buscar_textual_historica(q, n_results=n, filtro_bib=bib)
    else:
        resultados = buscar_hibrida(q, n_results=min(n * 3, 50), filtro_bib=bib)

    filtrados = [r for r in resultados if r["score"] >= score_min]
    return {"query": q, "total": len(filtrados[:n]), "resultados": filtrados[:n]}

@app.get("/api/stats")
async def get_stats():
    return build_stats(
        images_dir=IMAGES_DIR,
        cache_dir=CACHE_DIR,
        count_page_texts_fn=_count_page_texts,
        load_progress_status_fn=_load_progress_status,
    )


@app.get("/api/rag")
async def rag_search(
    q: str = Query(..., description="Pergunta do usuário"),
    n: int = Query(15, ge=1, le=30, description="Chunks para contexto"),
    bib: str = Query(None, description="Filtrar por acervo"),
):
    resultados = buscar_hibrida(q, n_results=n, filtro_bib=bib)
    return build_rag_response(
        question=q,
        resultados=resultados,
        resolve_image_url_fn=_resolve_image_url,
        parse_structured_answer_fn=_parse_structured_answer,
        build_prosopographic_fallback_fn=_build_prosopographic_fallback,
    )


@app.get("/api/acervos")
async def listar_acervos():
    return load_acervos(CACHE_DIR)


@app.get("/api/entity/search")
async def entity_search(
    q: str = Query(..., description="Nome ou sobrenome a buscar"),
    n: int = Query(10, ge=1, le=50, description="Número máximo de entidades"),
):
    repo = StructuredRepository()
    results = repo.search_entities(normalize_text(q), limit=n)
    results = serialize_entity_search_results(results)
    return {"query": q, "total": len(results), "resultados": results}


@app.get("/api/entity/{entity_id}")
async def get_entity(entity_id: int):
    repo = StructuredRepository()
    entity = repo.get_entity(entity_id)
    if not entity:
        return {"error": "Entidade não encontrada"}
    return serialize_entity(entity, _resolve_image_url)


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
