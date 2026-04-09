"""Interface unificada para correção pós-OCR por provider."""

from __future__ import annotations

from pathlib import Path

from src.config import TEXT_DIR

OPENAI_DEFAULT_MODEL = "gpt-4o-mini"
OPENAI_MAX_MODEL = "gpt-5"
CLAUDE_DEFAULT_MODEL = "opus"
PROVIDERS = ("openai", "claude", "claude-cli", "claude-api")


def list_pending_files(bib: str | None = None, force: bool = False) -> list[Path]:
    if bib:
        txt_dir = TEXT_DIR / bib
        if not txt_dir.exists():
            return []
        arquivos = list(txt_dir.glob("*.txt"))
    else:
        arquivos = list(TEXT_DIR.rglob("*.txt"))

    arquivos = [a for a in arquivos if not a.name.endswith("_corrigido.txt")]
    if not force:
        arquivos = [
            a for a in arquivos
            if not (a.parent / a.name.replace(".txt", "_corrigido.txt")).exists()
        ]
    return sorted(arquivos)


def corrigir_arquivo(
    txt_path: Path,
    provider: str = "openai",
    model: str | None = None,
    force: bool = False,
) -> bool:
    provider = provider.lower()
    if provider == "openai":
        from src.processing.llm_correcao import corrigir_arquivo as corrigir_openai
        return corrigir_openai(
            txt_path,
            model=model or OPENAI_DEFAULT_MODEL,
            force=force,
        )
    if provider in {"claude", "claude-cli"}:
        from src.processing.llm_correcao_claude_cli import corrigir_arquivo as corrigir_claude_cli
        return corrigir_claude_cli(
            txt_path,
            model=model or CLAUDE_DEFAULT_MODEL,
            force=force,
        )
    if provider == "claude-api":
        from src.processing.llm_correcao_claude import corrigir_arquivo as corrigir_claude
        return corrigir_claude(
            txt_path,
            model=model or CLAUDE_DEFAULT_MODEL,
            force=force,
        )
    raise ValueError(f"Provider inválido: {provider}")


def corrigir_texto(
    texto: str,
    provider: str = "openai",
    model: str | None = None,
) -> str | None:
    provider = provider.lower()
    if provider == "openai":
        from src.processing.llm_correcao import corrigir_texto_ocr
        return corrigir_texto_ocr(texto, model=model or OPENAI_DEFAULT_MODEL)
    if provider in {"claude", "claude-cli"}:
        from src.processing.llm_correcao_claude_cli import corrigir_texto
        return corrigir_texto(texto, model=model or CLAUDE_DEFAULT_MODEL)
    if provider == "claude-api":
        from src.processing.llm_correcao_claude import corrigir_texto
        return corrigir_texto(texto, model=model or CLAUDE_DEFAULT_MODEL)
    raise ValueError(f"Provider inválido: {provider}")
