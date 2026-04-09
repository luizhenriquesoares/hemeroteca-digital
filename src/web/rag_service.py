"""Composição de contexto e streaming do RAG."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from urllib.parse import quote

import openai
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse

from src.processing.search import extract_evidence_snippet, focus_query

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logger = logging.getLogger(__name__)

# --- Provider config ---
RAG_PROVIDER = os.getenv("RAG_PROVIDER", "openai").lower()
RAG_MODEL = os.getenv("RAG_MODEL", "gpt-4o")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")

_PROVIDER_DEFAULTS = {
    "openai": "gpt-4o",
    "huggingface": "meta-llama/Llama-3.3-70B-Instruct",
    "anthropic": "claude-sonnet-4-20250514",
    "gemini": "gemini-2.0-flash",
}


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


def _build_sources(question: str, termo_foco: str, resultados: list[dict], resolve_image_url_fn) -> tuple[list[dict], str]:
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
            "page_view_url": f"/page/{meta.get('bib', '?')}/{meta.get('pagina', '?')}?q={quote(termo_foco or question)}",
        }
        fontes.append(fonte)
        contexto_parts.append(
            f"[Trecho {index} | Periódico: {fonte['jornal']} | Acervo: {fonte['bib']} | Página: {fonte['pagina']} | Ano: {fonte['ano']} | Score: {fonte['score']}]\n"
            f"EVIDÊNCIA FOCADA:\n{evidencia}\n\nTRECHO COMPLETO:\n{result['texto']}"
        )
    return fontes, "\n\n---\n\n".join(contexto_parts)


def _get_active_provider() -> str:
    return RAG_PROVIDER


def _get_active_model() -> str:
    provider = _get_active_provider()
    return RAG_MODEL or _PROVIDER_DEFAULTS.get(provider, "gpt-4o-mini")


_SYSTEM_MSG = "Voce e um historiador especialista em Pernambuco e no Nordeste brasileiro."


def _stream_huggingface(prompt: str, model: str):
    """Stream via HuggingFace Inference API (gratuito)."""
    from huggingface_hub import InferenceClient

    token = HF_API_TOKEN
    if not token:
        raise ValueError("HF_API_TOKEN não configurada no .env")

    client = InferenceClient(token=token)
    for chunk in client.chat_completion(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_MSG},
            {"role": "user", "content": prompt},
        ],
        max_tokens=2000,
        temperature=0.3,
        stream=True,
    ):
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


def _stream_anthropic(prompt: str, model: str):
    """Stream via Anthropic API."""
    import anthropic

    client = anthropic.Anthropic()
    with client.messages.stream(
        model=model,
        max_tokens=2000,
        system=_SYSTEM_MSG,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            yield text


def _stream_openai(prompt: str, model: str):
    """Stream via OpenAI API."""
    client = openai.OpenAI()
    stream = client.chat.completions.create(
        model=model,
        max_tokens=2000,
        stream=True,
        messages=[
            {"role": "system", "content": _SYSTEM_MSG},
            {"role": "user", "content": prompt},
        ],
    )
    for chunk in stream:
        delta = chunk.choices[0].delta
        if delta.content:
            yield delta.content


def _stream_gemini(prompt: str, model: str):
    """Stream via Google Gemini API."""
    import google.generativeai as genai

    api_key = GEMINI_API_KEY
    if not api_key:
        raise ValueError("GEMINI_API_KEY não configurada no .env")

    genai.configure(api_key=api_key)
    gen_model = genai.GenerativeModel(
        model_name=model,
        system_instruction="Voce e um historiador especialista em Pernambuco e no Nordeste brasileiro.",
    )
    response = gen_model.generate_content(
        prompt,
        stream=True,
        generation_config=genai.GenerationConfig(
            max_output_tokens=2000,
            temperature=0.3,
        ),
    )
    for chunk in response:
        if chunk.text:
            yield chunk.text


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
    fontes, contexto = _build_sources(question, termo_foco, resultados, resolve_image_url_fn)
    prompt = _build_prompt(question=question, termo_foco=termo_foco, contexto=contexto)

    provider = _get_active_provider()
    model = _get_active_model()
    logger.info("RAG usando provider=%s model=%s", provider, model)

    _stream_map = {
        "huggingface": _stream_huggingface,
        "anthropic": _stream_anthropic,
        "gemini": _stream_gemini,
        "openai": _stream_openai,
    }
    stream_fn = _stream_map.get(provider, _stream_openai)

    async def generate():
        full_text = ""
        try:
            for text_chunk in stream_fn(prompt, model):
                full_text += text_chunk
                yield f"data: {json.dumps({'text': text_chunk})}\n\n"
        except Exception as e:
            full_text = f"Erro ao gerar resposta ({provider}/{model}): {e}"
            yield f"data: {json.dumps({'text': full_text})}\n\n"

        structured = parse_structured_answer_fn(full_text, fontes)
        fallback = build_prosopographic_fallback_fn(question, fontes)
        if not structured["pessoas"]:
            structured["pessoas"] = fallback["pessoas"]
        if not structured["evidencias"]:
            structured["evidencias"] = fallback["evidencias"]
        yield f"data: {json.dumps({'done': True, 'fontes': fontes, 'structured': structured})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


def _llm_complete(prompt: str, system: str = "", max_tokens: int = 500) -> str:
    """Chamada não-streaming para gerar texto via provider ativo."""
    provider = _get_active_provider()
    model = _get_active_model()
    sys_msg = system or "Você é um historiador especialista em Pernambuco."

    if provider == "huggingface":
        from huggingface_hub import InferenceClient
        client = InferenceClient(token=HF_API_TOKEN)
        response = client.chat_completion(
            model=model,
            messages=[{"role": "system", "content": sys_msg}, {"role": "user", "content": prompt}],
            max_tokens=max_tokens, temperature=0.3,
        )
        return response.choices[0].message.content.strip()

    if provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=model, max_tokens=max_tokens, system=sys_msg,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()

    if provider == "gemini":
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        gen_model = genai.GenerativeModel(model_name=model, system_instruction=sys_msg)
        response = gen_model.generate_content(
            prompt, generation_config=genai.GenerationConfig(max_output_tokens=max_tokens, temperature=0.3),
        )
        return response.text.strip()

    # OpenAI (default)
    client = openai.OpenAI()
    messages = [{"role": "system", "content": sys_msg}, {"role": "user", "content": prompt}]
    response = client.chat.completions.create(model=model, max_tokens=max_tokens, messages=messages)
    return response.choices[0].message.content.strip()


def generate_corpus_summary(overview: dict) -> str:
    """Gera um resumo narrativo do acervo baseado nos dados estruturados."""
    scope = overview.get("scope", {})
    timeline = overview.get("timeline", [])
    people = overview.get("top_people", [])
    institutions = overview.get("top_institutions", [])
    places = overview.get("top_places", [])
    roles = overview.get("top_roles", [])
    peak_years = overview.get("peak_years", [])

    years = [t["year"] for t in timeline if t.get("year")]
    year_range = f"{min(years)} a {max(years)}" if years else "período desconhecido"
    total_pages = scope.get("pages", 0)
    total_mentions = scope.get("mentions", 0)

    people_list = ", ".join(p["canonical_name"] for p in people[:5])
    institution_list = ", ".join(i["canonical_name"] for i in institutions[:5])
    place_list = ", ".join(p["canonical_name"] for p in places[:4])
    role_list = ", ".join(f"{r['role']} ({r['evidences']})" for r in roles[:5])

    peak_info = ""
    if peak_years:
        top = peak_years[0]
        peak_info = f"O pico de atividade editorial ocorre em {top['year']}, com {top['mentions']:,} menções."

    prompt = f"""Escreva um parágrafo de 4-5 frases resumindo o conteúdo de um acervo de jornais históricos de Pernambuco.

DADOS DO ACERVO:
- Período: {year_range}
- {total_pages:,} páginas processadas com {total_mentions:,} menções estruturadas
- Pessoas mais citadas: {people_list or 'nenhuma identificada'}
- Instituições mais presentes: {institution_list or 'nenhuma identificada'}
- Lugares recorrentes: {place_list or 'nenhum identificado'}
- Cargos em circulação: {role_list or 'nenhum identificado'}
- {peak_info}

INSTRUÇÕES:
- Escreva em português brasileiro, tom historiográfico acessível.
- Não invente dados além dos fornecidos.
- Destaque o que é mais revelador sobre o período.
- Mencione as figuras e instituições mais presentes.
- Termine com uma frase que convide à exploração.
- Não use markdown, apenas texto corrido."""

    return _llm_complete(prompt, system="Você é um historiador especialista em Pernambuco.", max_tokens=400)


def generate_entity_bio(entity_data: dict) -> str:
    """Gera uma mini-biografia para uma entidade baseada nos dados estruturados."""
    name = entity_data.get("canonical_name", "?")
    mentions = entity_data.get("mentions_count", 0)
    story = entity_data.get("story", {})
    relations = entity_data.get("relations", [])
    aliases = entity_data.get("aliases", [])

    # Montar contexto
    period = ""
    timeline = story.get("timeline", [])
    if timeline:
        years = [t["year"] for t in timeline]
        period = f"{min(years)} a {max(years)}" if len(years) > 1 else str(years[0])

    role_relations = [r for r in relations if r.get("predicate") == "holds_role"]
    family_relations = [r for r in relations if r.get("predicate") in {"spouse_of", "child_of", "parent_of", "widow_of"}]
    connections = story.get("connections", {})
    people_connections = connections.get("people", [])[:5]
    journals = story.get("journals", [])

    roles_text = ", ".join(r.get("object_name") or r.get("object_literal", "") for r in role_relations[:3])
    family_text = "; ".join(
        f"{r.get('predicate', '?').replace('_', ' ')} {r.get('object_name') or r.get('subject_name', '?')}"
        for r in family_relations[:3]
    )
    connections_text = ", ".join(p.get("canonical_name", "") for p in people_connections)
    journals_text = ", ".join(f"{j.get('jornal', j.get('bib', '?'))} ({j.get('mentions', 0)} menções)" for j in journals[:3])

    # Menções representativas
    top_mentions = entity_data.get("mentions", [])[:3]
    snippets = [m.get("snippet", "") for m in top_mentions if m.get("snippet")]
    snippets_text = "\n".join(f"- {s[:200]}" for s in snippets[:2])
    snippets_block = f"- Trechos representativos:\n{snippets_text}" if snippets_text else ""

    prompt = f"""Escreva uma mini-biografia de 3-4 frases sobre {name}, baseada exclusivamente nos dados abaixo.

DADOS:
- Nome: {name}
- Variantes: {', '.join(aliases[:4]) if aliases else 'nenhuma'}
- Período de presença nos jornais: {period or 'desconhecido'}
- Menções totais: {mentions}
- Cargos/títulos: {roles_text or 'nenhum identificado'}
- Relações familiares: {family_text or 'nenhuma identificada'}
- Pessoas associadas: {connections_text or 'nenhuma'}
- Periódicos: {journals_text or 'desconhecido'}
{snippets_block}

INSTRUÇÕES:
- Escreva em português brasileiro, tom historiográfico.
- Não invente informações além dos dados fornecidos.
- Se os dados forem escassos, diga o que se sabe e o que merece investigação.
- Não use markdown, apenas texto corrido."""

    return _llm_complete(prompt, system="Você é um historiador especialista em Pernambuco.", max_tokens=250)
