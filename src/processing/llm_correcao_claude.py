from __future__ import annotations

"""Correção pós-OCR usando a API Anthropic.

Mantém o provider ``claude`` estável no CLI do projeto, mas evita depender
de sessão autenticada do Claude Code CLI.
"""

import logging
import os
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MODEL_ALIASES = {
    "opus": "claude-opus-4-1-20250805",
    "sonnet": "claude-sonnet-4-20250514",
    "haiku": "claude-3-5-haiku-20241022",
}

SYSTEM_PROMPT = """Você é especialista em transcrição de jornais históricos brasileiros do século XIX.

Corrija erros de OCR em texto extraído de jornal antigo de Pernambuco.

REGRAS CRÍTICAS:
1. PRESERVE TODO o conteúdo (não corte linhas, não resuma)
2. MANTENHA ortografia da época: "Commandante", "pharmacia", "Alfandega", "dous", "theatro", "assignantes", "Escripturario" são CORRETOS
3. PRESERVE nomes próprios exatamente como aparecem
4. Corrija apenas erros claros de OCR (letras trocadas, símbolos estranhos no meio de palavras)
5. Junte palavras quebradas por coluna (fim de linha com hífen)
6. Remova separadores de coluna (|, ])
7. Retorne APENAS o texto corrigido, sem explicações, sem cortar conteúdo"""

_client: Anthropic | None = None


def _load_api_key() -> str | None:
    """Carrega ANTHROPIC_API_KEY do ambiente ou do .env do projeto."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key

    env_file = Path(__file__).resolve().parents[2] / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("ANTHROPIC_API_KEY="):
                return line.split("=", 1)[1].strip()
    return None


def _resolve_model(model: str) -> str:
    return MODEL_ALIASES.get(model, model)


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        api_key = _load_api_key()
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY não encontrada para o provider claude")
        _client = Anthropic(api_key=api_key)
    return _client


def _extract_text_block(response) -> str:
    parts: list[str] = []
    for block in getattr(response, "content", []):
        text = getattr(block, "text", None)
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def corrigir_texto(texto: str, model: str = "opus", timeout: int = 180) -> str | None:
    """Corrige texto OCR via API Anthropic."""
    if not texto or len(texto.strip()) < 20:
        return texto

    try:
        client = _get_client()
        response = client.messages.create(
            model=_resolve_model(model),
            system=SYSTEM_PROMPT,
            max_tokens=4096,
            temperature=0,
            messages=[
                {
                    "role": "user",
                    "content": f"TEXTO A CORRIGIR:\n---\n{texto}\n---",
                }
            ],
            timeout=timeout,
        )
        corrigido = _extract_text_block(response)
        if not corrigido:
            logger.warning("Anthropic retornou resposta vazia")
            return None
        return corrigido
    except Exception as exc:
        logger.error("Erro na correção Anthropic: %s", exc)
        return None


def corrigir_arquivo(txt_path: Path, model: str = "opus", force: bool = False) -> bool:
    """Corrige um arquivo .txt salvando resultado em .txt_corrigido."""
    out_path = txt_path.parent / txt_path.name.replace(".txt", "_corrigido.txt")

    if out_path.exists() and not force:
        return True

    try:
        original = txt_path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Erro lendo %s: %s", txt_path, exc)
        return False

    words = original.split()
    if words:
        real_words = [w for w in words if len(w) >= 3 and any(c.isalpha() for c in w)]
        ratio = len(real_words) / len(words)
        if ratio < 0.4 or len(real_words) < 15:
            logger.warning("OCR ilegível em %s (%.0f%% palavras reais), pulando", txt_path.name, ratio * 100)
            out_path.write_text(original, encoding="utf-8")
            return True

    if len(original) > 15000:
        partes = _dividir_texto(original, 12000)
        corrigidos = []
        for indice, parte in enumerate(partes, start=1):
            corrigido = corrigir_texto(parte, model=model)
            if corrigido is None:
                logger.warning("Parte %s/%s falhou, usando original", indice, len(partes))
                corrigido = parte
            corrigidos.append(corrigido)
        final = "\n\n".join(corrigidos)
    else:
        final = corrigir_texto(original, model=model)
        if final is None:
            return False

    out_path.write_text(final, encoding="utf-8")
    return True


def _dividir_texto(texto: str, max_chars: int) -> list[str]:
    """Divide texto em partes respeitando parágrafos."""
    paragrafos = texto.split("\n\n")
    partes: list[str] = []
    atual = ""
    for paragrafo in paragrafos:
        if len(atual) + len(paragrafo) > max_chars and atual:
            partes.append(atual.strip())
            atual = paragrafo
        else:
            atual += "\n\n" + paragrafo if atual else paragrafo
    if atual.strip():
        partes.append(atual.strip())
    return partes if partes else [texto]
