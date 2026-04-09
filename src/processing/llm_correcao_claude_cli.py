from __future__ import annotations

"""Correção pós-OCR usando Claude Code CLI."""

import logging
import subprocess
import time

logger = logging.getLogger(__name__)

CLI_CHUNK_SIZE = 900
CLI_CHUNK_THRESHOLD = 1000
CLI_TIMEOUTS = (60, 120, 180)

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
    if not texto or len(texto.strip()) < 20:
        return texto

    if len(texto) > CLI_CHUNK_THRESHOLD:
        partes = _dividir_texto(texto, CLI_CHUNK_SIZE)
        corrigidos = []
        for indice, parte in enumerate(partes, start=1):
            corrigido = _corrigir_parte(parte, model=model, timeout=timeout)
            if corrigido is None:
                logger.warning("Chunk %s/%s falhou no Claude CLI, usando original", indice, len(partes))
                corrigido = parte
            corrigidos.append(corrigido)
        return "\n\n".join(corrigidos)

    return _corrigir_parte(texto, model=model, timeout=timeout)


def _corrigir_parte(texto: str, model: str, timeout: int) -> str | None:
    prompt = f"{SYSTEM_PROMPT}\n\nTEXTO A CORRIGIR:\n---\n{texto}\n---"

    timeouts = _timeouts_for(timeout)
    last_error = ""
    for tentativa, current_timeout in enumerate(timeouts, start=1):
        try:
            cmd = ["claude", "--model", model, "-p", prompt, "--output-format", "text"]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=current_timeout,
            )
            if result.returncode != 0:
                stderr = (result.stderr or "").strip()
                stdout = (result.stdout or "").strip()
                last_error = stderr or stdout or f"returncode={result.returncode}"
                logger.warning(
                    "Claude CLI tentativa %s/%s falhou: %s",
                    tentativa,
                    len(timeouts),
                    last_error[:300],
                )
                if tentativa < len(timeouts):
                    time.sleep(min(tentativa, 3))
                continue
            corrigido = (result.stdout or "").strip()
            if not corrigido:
                last_error = "resposta vazia"
                logger.warning(
                    "Claude CLI tentativa %s/%s retornou vazio",
                    tentativa,
                    len(timeouts),
                )
                if tentativa < len(timeouts):
                    time.sleep(min(tentativa, 3))
                continue
            return corrigido
        except subprocess.TimeoutExpired:
            last_error = f"timeout ({current_timeout}s)"
            logger.warning(
                "Claude CLI tentativa %s/%s excedeu timeout (%ss)",
                tentativa,
                len(timeouts),
                current_timeout,
            )
            if tentativa < len(timeouts):
                time.sleep(min(tentativa, 3))
        except Exception as exc:
            last_error = str(exc)
            logger.error("Erro Claude CLI tentativa %s/%s: %s", tentativa, len(timeouts), exc)
            if tentativa < len(timeouts):
                time.sleep(min(tentativa, 3))

    logger.error("Claude CLI falhou após %s tentativas: %s", len(timeouts), last_error[:500])
    return None


def corrigir_arquivo(txt_path: Path, model: str = "opus", force: bool = False) -> bool:
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

    final = corrigir_texto(original, model=model)
    if final is None:
        return False

    out_path.write_text(final, encoding="utf-8")
    return True


def _timeouts_for(base_timeout: int) -> tuple[int, ...]:
    if base_timeout <= CLI_TIMEOUTS[0]:
        return CLI_TIMEOUTS
    return tuple(sorted({CLI_TIMEOUTS[0], CLI_TIMEOUTS[1], CLI_TIMEOUTS[2], base_timeout}))


def _dividir_texto(texto: str, max_chars: int) -> list[str]:
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
