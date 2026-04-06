"""Composição de contexto e streaming do RAG."""

from __future__ import annotations

import json

import openai
from fastapi.responses import StreamingResponse

from src.processing.search import extract_evidence_snippet, focus_query


def _build_prompt(*, question: str, termo_foco: str, contexto: str) -> str:
    return f"""Você é um historiador especialista em Pernambuco e no Nordeste brasileiro.
O usuário fez uma pesquisa na Hemeroteca Digital, que contém jornais históricos de Pernambuco dos séculos XIX e XX.

Abaixo estão trechos de jornais encontrados pela busca. O texto pode conter erros de OCR (letras trocadas, palavras cortadas) — interprete o melhor possível.

TERMO FOCO DA PESQUISA:
{termo_foco}

TRECHOS DOS JORNAIS:
{contexto}

PERGUNTA DO USUÁRIO: {question}

INSTRUÇÕES:
- Analise os trechos e responda a pergunta do usuário de forma clara e informativa.
- Trate variações históricas e nominais como equivalentes quando fizer sentido: Botelho/Botelhos, Benedicto/Benedito, d'Araujo/de Araujo.
- Se a consulta parecer pessoa, família ou sobrenome, priorize nomes próprios, cargos, parentescos, eventos, datas, lugares e periódicos.
- Não confunda sobrenome com substantivo comum só porque a grafia permite.
- Cite as fontes com periódico, acervo, página e ano quando mencionar informações específicas.
- Trabalhe primeiro a partir do bloco "EVIDÊNCIA FOCADA" de cada trecho. Use o "TRECHO COMPLETO" apenas para complementar.
- Inclua trechos reais e curtos dos jornais como evidência textual.
- Se os trechos não contêm informação direta sobre a pergunta, diga isso honestamente, mas ainda liste nomes, instituições, locais e fatos próximos do termo foco.
- Corrija mentalmente erros de OCR ao interpretar o texto (ex: "Pernamlmeo" = "Pernambuco").
- Responda em português brasileiro.
- Organize a resposta nesta estrutura:
  1. Resumo interpretativo
  2. Pessoas e entidades citadas
  3. Evidências dos jornais
  4. Fontes
- Em "Pessoas e entidades citadas", prefira bullets com: nome, papel/cargo, vínculo com o tema, local e periódico/fonte.
- Em "Evidências dos jornais", traga bullets com pequenas citações ou paráfrases muito próximas do texto e a fonte correspondente.
- Quando houver uma família ou sobrenome, agrupe os indivíduos relacionados sob esse sobrenome e destaque distinções entre eles.
- Se houver informação suficiente, mencione explicitamente o periódico onde a pessoa/família aparece.
- Não invente informações que não estejam nos trechos."""


def _build_sources(question: str, resultados: list[dict], resolve_image_url_fn) -> tuple[list[dict], str]:
    contexto_parts = []
    fontes = []
    for index, result in enumerate(resultados, 1):
        meta = result["metadata"]
        evidencia = extract_evidence_snippet(question, result["texto"])
        fonte = {
            "bib": meta.get("bib", "?"),
            "jornal": meta.get("jornal") or meta.get("periodico") or "?",
            "pagina": meta.get("pagina", "?"),
            "ano": meta.get("ano", "?"),
            "edicao": meta.get("edicao", "?"),
            "score": result.get("score", 0),
            "evidencia": evidencia,
            "image_url": resolve_image_url_fn(meta.get("bib", "?"), str(meta.get("pagina", "?"))),
            "page_api_url": f"/api/page/{meta.get('bib', '?')}/{meta.get('pagina', '?')}",
        }
        fontes.append(fonte)
        contexto_parts.append(
            f"[Trecho {index} | Periódico: {fonte['jornal']} | Acervo: {fonte['bib']} | Página: {fonte['pagina']} | Ano: {fonte['ano']} | Score: {fonte['score']}]\n"
            f"EVIDÊNCIA FOCADA:\n{evidencia}\n\nTRECHO COMPLETO:\n{result['texto']}"
        )
    return fontes, "\n\n---\n\n".join(contexto_parts)


def build_rag_response(
    *,
    question: str,
    resultados: list[dict],
    resolve_image_url_fn,
    parse_structured_answer_fn,
    build_prosopographic_fallback_fn,
):
    if not resultados:
        return {"query": question, "resposta": "Nenhum resultado encontrado nos jornais.", "fontes": []}

    termo_foco = focus_query(question)
    fontes, contexto = _build_sources(question, resultados, resolve_image_url_fn)
    prompt = _build_prompt(question=question, termo_foco=termo_foco, contexto=contexto)
    client = openai.OpenAI()

    async def generate():
        full_text = ""
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
                full_text += delta.content
                yield f"data: {json.dumps({'text': delta.content})}\n\n"

        structured = parse_structured_answer_fn(full_text, fontes)
        fallback = build_prosopographic_fallback_fn(question, fontes)
        if not structured["pessoas"]:
            structured["pessoas"] = fallback["pessoas"]
        if not structured["evidencias"]:
            structured["evidencias"] = fallback["evidencias"]
        yield f"data: {json.dumps({'done': True, 'fontes': fontes, 'structured': structured})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")
