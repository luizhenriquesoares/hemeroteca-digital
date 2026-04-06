"""Pós-processamento de texto OCR usando OpenAI."""

import logging
from pathlib import Path

from dotenv import load_dotenv
import openai

load_dotenv()

logger = logging.getLogger(__name__)
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = openai.OpenAI()
    return _client


SYSTEM_PROMPT = """Você é especialista em transcrição de jornais históricos brasileiros do século XIX.

Corrija erros de OCR em texto extraído de jornal antigo de Pernambuco.

REGRAS CRÍTICAS:
1. PRESERVE TODO o conteúdo útil do texto. Não resuma.
2. MANTENHA a ortografia da época: "Commandante", "pharmacia", "Alfandega", "dous", "theatro", "assignantes", "Escripturario" podem estar corretos.
3. PRESERVE nomes próprios, topônimos, cargos, datas e números.
4. Corrija apenas erros claros de OCR: letras trocadas, símbolos estranhos, palavras quebradas por hifenização de coluna.
5. Remova lixo evidente de OCR quando for ininteligível.
6. Remova separadores de coluna e artefatos como pipes e colchetes isolados.
7. Se uma palavra continuar muito corrompida, preserve a forma mais provável sem inventar informação.
8. Retorne APENAS o texto corrigido, sem comentários."""


def corrigir_texto_ocr(texto_bruto: str, model: str = DEFAULT_OPENAI_MODEL, max_tokens: int = 4000) -> str:
    """Corrige erros de OCR usando OpenAI.

    Args:
        texto_bruto: Texto bruto do OCR
        model: modelo OpenAI
        max_tokens: Máximo de tokens na resposta

    Returns:
        Texto corrigido
    """
    if not texto_bruto or len(texto_bruto.strip()) < 20:
        return texto_bruto

    client = _get_client()

    # Se texto muito longo, dividir em pedaços
    if len(texto_bruto) > 6000:
        partes = _dividir_texto(texto_bruto, 5000)
        corrigidos = []
        for parte in partes:
            corrigido = _corrigir_parte(client, parte, model, max_tokens)
            corrigidos.append(corrigido)
        return "\n\n".join(corrigidos)

    return _corrigir_parte(client, texto_bruto, model, max_tokens)


def _corrigir_parte(client, texto: str, model: str, max_tokens: int) -> str:
    """Corrige uma parte do texto."""
    try:
        kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Corrija o texto OCR abaixo:\n\n{texto}"},
            ],
        }

        # GPT-5 models use max_completion_tokens instead of max_tokens.
        if model.startswith("gpt-5"):
            kwargs["max_completion_tokens"] = max_tokens
        else:
            kwargs["max_tokens"] = max_tokens
            kwargs["temperature"] = 0.1

        response = client.chat.completions.create(**kwargs)
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Erro na correção LLM: {e}")
        return texto


def corrigir_arquivo(
    txt_path: Path,
    model: str = DEFAULT_OPENAI_MODEL,
    force: bool = False,
    max_tokens: int = 4000,
) -> bool:
    """Corrige um arquivo .txt salvando resultado em _corrigido.txt."""
    out_path = txt_path.parent / txt_path.name.replace(".txt", "_corrigido.txt")

    if out_path.exists() and not force:
        return True

    try:
        original = txt_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Erro lendo {txt_path}: {e}")
        return False

    words = original.split()
    if words:
        real_words = [w for w in words if len(w) >= 3 and any(c.isalpha() for c in w)]
        ratio = len(real_words) / len(words)
        if ratio < 0.4 or len(real_words) < 15:
            logger.warning(f"OCR ilegível em {txt_path.name} ({ratio:.0%} palavras reais), pulando")
            out_path.write_text(original, encoding="utf-8")
            return True

    corrigido = corrigir_texto_ocr(original, model=model, max_tokens=max_tokens)
    if not corrigido:
        return False

    out_path.write_text(corrigido, encoding="utf-8")
    return True


def _dividir_texto(texto: str, max_chars: int) -> list[str]:
    """Divide texto em partes respeitando parágrafos."""
    paragrafos = texto.split("\n\n")
    partes = []
    atual = ""

    for p in paragrafos:
        if len(atual) + len(p) > max_chars and atual:
            partes.append(atual.strip())
            atual = p
        else:
            atual += "\n\n" + p if atual else p

    if atual.strip():
        partes.append(atual.strip())

    return partes if partes else [texto]
