"""Módulo de chunking: divide textos em pedaços para indexação RAG."""

import json
import logging
from pathlib import Path
from typing import Optional

from langchain_text_splitters import RecursiveCharacterTextSplitter

from src import config
from src.config import CHUNK_SIZE, CHUNK_OVERLAP

logger = logging.getLogger(__name__)

# Permite monkeypatch direto no módulo sem perder o fallback para src.config.
TEXT_DIR: Optional[Path] = None
CHUNKS_DIR: Optional[Path] = None


def _text_dir() -> Path:
    return TEXT_DIR or config.TEXT_DIR


def _chunks_dir() -> Path:
    return CHUNKS_DIR or config.CHUNKS_DIR


def limpar_chunks_acervo(bib: str) -> dict[str, int]:
    """Remove os artefatos de chunking de um acervo específico."""
    chunk_dir = _chunks_dir() / bib
    if not chunk_dir.exists():
        return {"files_removed": 0, "chunks_removed": 0}

    files = sorted(path for path in chunk_dir.rglob("*") if path.is_file())
    chunk_file = chunk_dir / "chunks.jsonl"
    chunks_removed = 0

    if chunk_file.exists():
        with open(chunk_file, encoding="utf-8") as f:
            chunks_removed = sum(1 for _ in f)

    for path in files:
        path.unlink()

    nested_dirs = sorted(
        (path for path in chunk_dir.rglob("*") if path.is_dir()),
        reverse=True,
    )
    for path in nested_dirs:
        path.rmdir()

    if chunk_dir.exists():
        chunk_dir.rmdir()

    logger.info(
        "Acervo %s: %s arquivo(s) de chunk removidos (%s chunks antigos)",
        bib,
        len(files),
        chunks_removed,
    )
    return {"files_removed": len(files), "chunks_removed": chunks_removed}


def criar_chunks_acervo(bib: str, force: bool = False) -> int:
    """
    Divide os textos de um acervo em chunks para RAG.

    Cada chunk inclui metadados: bib, jornal, página, ano, edição.

    Returns:
        número de chunks criados
    """
    txt_dir = _text_dir() / bib
    chunk_dir = _chunks_dir() / bib
    chunk_dir.mkdir(parents=True, exist_ok=True)

    chunk_file = chunk_dir / "chunks.jsonl"

    if chunk_file.exists() and not force:
        # Contar chunks existentes (reprocessar se vazio)
        with open(chunk_file) as f:
            count = sum(1 for _ in f)
        if count > 0:
            return count

    if not txt_dir.exists():
        logger.warning(f"Diretório de texto não encontrado: {txt_dir}")
        return 0

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""],
        length_function=len,
    )

    total_chunks = 0

    with open(chunk_file, "w", encoding="utf-8") as out:
        # Ignorar arquivos _corrigido.txt (serão carregados via preferência)
        todos_txt = sorted([
            p for p in txt_dir.glob("*.txt")
            if not p.name.endswith("_corrigido.txt")
        ])
        for txt_path in todos_txt:
            # Preferir versão corrigida se existir
            corrigido = txt_path.parent / txt_path.name.replace(".txt", "_corrigido.txt")
            fonte = corrigido if corrigido.exists() else txt_path
            texto = fonte.read_text(encoding="utf-8").strip()
            if len(texto) < 50:
                continue

            # Carregar metadados se existirem
            meta_path = txt_dir / f"{txt_path.stem}.json"
            metadata = {}
            if meta_path.exists():
                with open(meta_path) as f:
                    metadata = json.load(f)
            # Marcar como corrigido se aplicável
            metadata["corrigido_llm"] = corrigido.exists()

            # Criar chunks
            chunks = splitter.split_text(texto)

            for i, chunk_text in enumerate(chunks):
                chunk_doc = {
                    "id": f"{bib}_{txt_path.stem}_chunk{i}",
                    "text": chunk_text,
                    "metadata": {
                        "bib": bib,
                        "pagina": txt_path.stem,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                        **{k: v for k, v in metadata.items()
                           if k not in ("caracteres", "palavras", "arquivo_texto")},
                    },
                }
                out.write(json.dumps(chunk_doc, ensure_ascii=False) + "\n")
                total_chunks += 1

    logger.info(f"Acervo {bib}: {total_chunks} chunks criados")
    return total_chunks


def criar_chunks_todos(force: bool = False) -> dict:
    """Cria chunks para todos os acervos processados."""
    stats = {}

    if not _text_dir().exists():
        logger.warning("Nenhum diretório de texto encontrado")
        return stats

    for acervo_dir in sorted(_text_dir().iterdir()):
        if acervo_dir.is_dir():
            bib = acervo_dir.name
            count = criar_chunks_acervo(bib, force=force)
            stats[bib] = count

    return stats


def carregar_chunks(bib: str = None) -> list[dict]:
    """Carrega chunks de um acervo específico ou de todos."""
    chunks = []

    if bib:
        chunk_file = _chunks_dir() / bib / "chunks.jsonl"
        if chunk_file.exists():
            chunks = _ler_jsonl(chunk_file)
    else:
        for chunk_file in sorted(_chunks_dir().glob("*/chunks.jsonl")):
            chunks.extend(_ler_jsonl(chunk_file))

    return chunks


def _ler_jsonl(path: Path) -> list[dict]:
    """Lê arquivo JSONL."""
    docs = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    return docs
