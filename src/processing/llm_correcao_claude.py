from __future__ import annotations

"""Correção pós-OCR usando Claude Opus 4.6 via Claude Code CLI.

Usa a sessão autenticada do Claude Code Max (assinatura flat-rate),
então não há custo variável por chamada.

Uso:
    from src.llm_correcao_claude import corrigir_texto
    texto_bom = corrigir_texto(texto_ocr)
"""

import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

def _load_api_key() -> str | None:
    """Carrega ANTHROPIC_API_KEY do .env ou env vars."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("ANTHROPIC_API_KEY="):
                return line.split("=", 1)[1].strip()
    return None

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


def corrigir_texto(texto: str, model: str = "opus", timeout: int = 180) -> str | None:
    """Corrige texto OCR via Claude CLI.

    Args:
        texto: texto OCR a corrigir
        model: "opus", "sonnet" ou "haiku"
        timeout: timeout em segundos (default 10min)

    Returns:
        texto corrigido ou None em caso de erro
    """
    if not texto or len(texto.strip()) < 20:
        return texto

    prompt = f"{SYSTEM_PROMPT}\n\nTEXTO A CORRIGIR:\n---\n{texto}\n---"

    try:
        cmd = ["claude", "--model", model, "-p", prompt, "--output-format", "text"]
        env = os.environ.copy()
        api_key = _load_api_key()
        if api_key:
            env["ANTHROPIC_API_KEY"] = api_key
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode != 0:
            logger.error(f"Claude CLI falhou: {result.stderr[:500]}")
            return None
        corrigido = result.stdout.strip()
        if not corrigido:
            logger.warning("Claude CLI retornou vazio")
            return None
        return corrigido
    except subprocess.TimeoutExpired:
        logger.error(f"Claude CLI timeout ({timeout}s)")
        return None
    except Exception as e:
        logger.error(f"Erro Claude CLI: {e}")
        return None


def corrigir_arquivo(txt_path: Path, model: str = "opus", force: bool = False) -> bool:
    """Corrige um arquivo .txt salvando resultado em .txt_corrigido.

    Args:
        txt_path: caminho do texto original
        model: modelo Claude a usar
        force: reprocessar mesmo se já existe

    Returns:
        True se corrigiu com sucesso
    """
    out_path = txt_path.parent / txt_path.name.replace(".txt", "_corrigido.txt")

    if out_path.exists() and not force:
        return True

    try:
        original = txt_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Erro lendo {txt_path}: {e}")
        return False

    # Pular páginas com OCR muito ruim (< 30% palavras reais de 3+ letras)
    words = original.split()
    if words:
        real_words = [w for w in words if len(w) >= 3 and any(c.isalpha() for c in w)]
        ratio = len(real_words) / len(words)
        if ratio < 0.4 or len(real_words) < 15:
            logger.warning(f"OCR ilegível em {txt_path.name} ({ratio:.0%} palavras reais), pulando")
            out_path.write_text(original, encoding="utf-8")  # salva original como corrigido
            return True

    # Para textos muito grandes, dividir em partes
    if len(original) > 15000:
        partes = _dividir_texto(original, 12000)
        corrigidos = []
        for i, p in enumerate(partes):
            c = corrigir_texto(p, model=model)
            if c is None:
                logger.warning(f"Parte {i+1}/{len(partes)} falhou, usando original")
                c = p
            corrigidos.append(c)
        corrigido = "\n\n".join(corrigidos)
    else:
        corrigido = corrigir_texto(original, model=model)
        if corrigido is None:
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
