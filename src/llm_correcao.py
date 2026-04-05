"""Pós-processamento de texto OCR usando LLM (GPT-4o-mini).

Corrige erros de OCR em textos extraídos de jornais históricos.
"""

import os
import logging
from dotenv import load_dotenv
import openai

load_dotenv()

logger = logging.getLogger(__name__)

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = openai.OpenAI()
    return _client


SYSTEM_PROMPT = """Você é um especialista em transcrição de jornais históricos brasileiros do século XIX e XX.

Sua tarefa é corrigir erros de OCR em textos extraídos de jornais antigos de Pernambuco.

REGRAS:
1. Corrija APENAS erros óbvios de OCR (letras trocadas, palavras cortadas, caracteres estranhos)
2. MANTENHA a ortografia da época (ex: "dous" não é erro, é português antigo; "pharmacia", "theatro", "assignantes" são corretos para a época)
3. REMOVA linhas de lixo (sequências de caracteres sem sentido, como "SA O, A E ARO & EN Boo ea ET")
4. MANTENHA a estrutura do texto (parágrafos, separações)
5. NÃO adicione informações que não estejam no texto
6. NÃO modernize a ortografia — preserve o português da época
7. Se uma palavra está muito corrompida e você não consegue deduzir, mantenha como está entre [?]
8. Remova separadores de coluna (|, pipes) que são artefatos do OCR
9. Junte palavras que foram quebradas pela coluna (ex: "com-|panhia" → "companhia")
10. Retorne APENAS o texto corrigido, sem explicações."""


def corrigir_texto_ocr(texto_bruto: str, max_tokens: int = 4000) -> str:
    """Corrige erros de OCR usando GPT-4o-mini.

    Args:
        texto_bruto: Texto bruto do OCR
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
            corrigido = _corrigir_parte(client, parte, max_tokens)
            corrigidos.append(corrigido)
        return "\n\n".join(corrigidos)

    return _corrigir_parte(client, texto_bruto, max_tokens)


def _corrigir_parte(client, texto: str, max_tokens: int) -> str:
    """Corrige uma parte do texto."""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=max_tokens,
            temperature=0.1,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Corrija o texto OCR abaixo:\n\n{texto}"},
            ],
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Erro na correção LLM: {e}")
        return texto


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
