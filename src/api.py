"""API FastAPI para busca na Hemeroteca Digital PE."""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import openai

from src.config import IMAGES_DIR, DATA_DIR, CACHE_DIR

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
        return _busca_textual(q, n, bib)

    from src.indexer import buscar as _buscar

    # Buscar mais resultados e filtrar por score mínimo
    resultados = _buscar(q, n_results=min(n * 3, 50), filtro_bib=bib)

    filtrados = []
    for r in resultados:
        score = round(1 - r["distancia"], 3)
        if score >= score_min:
            filtrados.append({
                "id": r["id"],
                "texto": r["texto"],
                "score": score,
                "metadata": r["metadata"],
                "modo": "semantica",
            })

    return {
        "query": q,
        "total": len(filtrados[:n]),
        "resultados": filtrados[:n],
    }


def _busca_textual(query: str, n: int, bib: str = None) -> dict:
    """Busca textual (grep) nos arquivos OCR."""
    from src.config import TEXT_DIR
    import re

    query_lower = query.lower()
    terms = query_lower.split()
    resultados = []

    # Buscar em todos os .txt ou filtrar por bib
    if bib:
        search_dirs = [TEXT_DIR / bib]
    else:
        search_dirs = [d for d in TEXT_DIR.iterdir() if d.is_dir()] if TEXT_DIR.exists() else []

    for acervo_dir in search_dirs:
        if not acervo_dir.exists():
            continue
        for txt_file in acervo_dir.glob("*.txt"):
            try:
                content = txt_file.read_text(encoding="utf-8", errors="ignore")
                content_lower = content.lower()

                # Verificar se TODOS os termos aparecem
                if all(term in content_lower for term in terms):
                    # Encontrar o trecho mais relevante
                    best_pos = content_lower.find(terms[0])
                    start = max(0, best_pos - 200)
                    end = min(len(content), best_pos + 800)
                    snippet = content[start:end]

                    # Contar ocorrências para ranking
                    count = sum(content_lower.count(term) for term in terms)

                    bib_code = acervo_dir.name
                    page_id = txt_file.stem

                    resultados.append({
                        "id": f"{bib_code}_{page_id}",
                        "texto": snippet,
                        "score": min(1.0, round(count / 10, 3)),
                        "metadata": {
                            "bib": bib_code,
                            "pagina": page_id,
                        },
                        "modo": "textual",
                        "ocorrencias": count,
                    })
            except Exception:
                continue

    # Ordenar por ocorrências
    resultados.sort(key=lambda x: x["ocorrencias"], reverse=True)

    return {
        "query": query,
        "total": len(resultados[:n]),
        "resultados": resultados[:n],
    }


@app.get("/api/stats")
async def get_stats():
    """Estatísticas do projeto."""
    from src.indexer import stats as idx_stats

    # Contagens de arquivos
    img_count = sum(1 for _ in IMAGES_DIR.rglob("*.jpg")) if IMAGES_DIR.exists() else 0
    txt_count = sum(1 for _ in DATA_DIR.joinpath("text").rglob("*.txt")) if DATA_DIR.joinpath("text").exists() else 0
    acervos_count = len(list(IMAGES_DIR.iterdir())) if IMAGES_DIR.exists() else 0

    # ChromaDB
    try:
        s = idx_stats()
        chunks_indexados = s["total_chunks"]
    except Exception:
        chunks_indexados = 0

    # Pipeline progress
    progress_file = CACHE_DIR / "pipeline_progress.json"
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
        done = len(progress.get("done", []))
        failed = len(progress.get("failed", []))
    else:
        done = failed = 0

    # Acervos totais
    acervos_file = CACHE_DIR / "acervos_pe.json"
    total_acervos = 0
    total_paginas = 0
    if acervos_file.exists():
        with open(acervos_file) as f:
            acervos = json.load(f)
        total_acervos = len(acervos)
        total_paginas = sum(a.get("paginas", 0) for a in acervos)

    return {
        "acervos_total": total_acervos,
        "acervos_concluidos": done,
        "acervos_falhas": failed,
        "paginas_total": total_paginas,
        "imagens": img_count,
        "textos_ocr": txt_count,
        "chunks_indexados": chunks_indexados,
        "progresso_pct": round(img_count * 100 / total_paginas, 1) if total_paginas > 0 else 0,
    }


@app.get("/api/rag")
async def rag_search(
    q: str = Query(..., description="Pergunta do usuário"),
    n: int = Query(15, ge=1, le=30, description="Chunks para contexto"),
    bib: str = Query(None, description="Filtrar por acervo"),
):
    """Busca RAG: recupera trechos relevantes e usa Claude para sintetizar."""
    from src.indexer import buscar as _buscar

    resultados = _buscar(q, n_results=n, filtro_bib=bib)

    if not resultados:
        return {"query": q, "resposta": "Nenhum resultado encontrado nos jornais.", "fontes": []}

    # Montar contexto com os trechos
    contexto_parts = []
    fontes = []
    for i, r in enumerate(resultados, 1):
        meta = r["metadata"]
        score = round(1 - r["distancia"], 3)
        fonte = {
            "bib": meta.get("bib", "?"),
            "pagina": meta.get("pagina", "?"),
            "ano": meta.get("ano", "?"),
            "edicao": meta.get("edicao", "?"),
            "score": score,
        }
        fontes.append(fonte)
        contexto_parts.append(
            f"[Trecho {i} | Acervo: {fonte['bib']} | Página: {fonte['pagina']} | Ano: {fonte['ano']}]\n{r['texto']}"
        )

    contexto = "\n\n---\n\n".join(contexto_parts)

    prompt = f"""Você é um historiador especialista em Pernambuco e no Nordeste brasileiro.
O usuário fez uma pesquisa na Hemeroteca Digital, que contém jornais históricos de Pernambuco dos séculos XIX e XX.

Abaixo estão trechos de jornais encontrados pela busca. O texto pode conter erros de OCR (letras trocadas, palavras cortadas) — interprete o melhor possível.

TRECHOS DOS JORNAIS:
{contexto}

PERGUNTA DO USUÁRIO: {q}

INSTRUÇÕES:
- Analise os trechos e responda a pergunta do usuário de forma clara e informativa.
- Cite as fontes (acervo, página, ano) quando mencionar informações específicas.
- Se os trechos não contêm informação direta sobre a pergunta, diga isso honestamente, mas tente extrair o que for possível.
- Corrija mentalmente erros de OCR ao interpretar o texto (ex: "Pernamlmeo" = "Pernambuco").
- Responda em português brasileiro.
- Se encontrar dados biográficos, datas, eventos, profissões, locais — organize de forma estruturada.
- Não invente informações que não estejam nos trechos."""

    client = openai.OpenAI()

    async def generate():
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=2000,
            stream=True,
            messages=[
                {"role": "system", "content": "Você é um historiador especialista em Pernambuco e no Nordeste brasileiro."},
                {"role": "user", "content": prompt},
            ],
        )
        for chunk in stream:
            delta = chunk.choices[0].delta
            if delta.content:
                yield f"data: {json.dumps({'text': delta.content})}\n\n"
        yield f"data: {json.dumps({'done': True, 'fontes': fontes})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/acervos")
async def listar_acervos():
    """Lista acervos disponíveis."""
    acervos_file = CACHE_DIR / "acervos_pe.json"
    if not acervos_file.exists():
        return []

    with open(acervos_file) as f:
        acervos = json.load(f)

    return [
        {"bib": a["bib"], "nome": a["nome"], "paginas": a.get("paginas", 0)}
        for a in sorted(acervos, key=lambda x: x["nome"])
    ]
