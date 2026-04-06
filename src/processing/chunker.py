"""Módulo de chunking: divide textos em pedaços para indexação RAG."""

import json
import logging
from pathlib import Path

from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.config import TEXT_DIR, CHUNKS_DIR, CHUNK_SIZE, CHUNK_OVERLAP

logger = logging.getLogger(__name__)


def criar_chunks_acervo(bib: str, force: bool = False) -> int:
    """
    Divide os textos de um acervo em chunks para RAG.

    Cada chunk inclui metadados: bib, jornal, página, ano, edição.

    Returns:
        número de chunks criados
    """
    txt_dir = TEXT_DIR / bib
    chunk_dir = CHUNKS_DIR / bib
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

    if not TEXT_DIR.exists():
        logger.warning("Nenhum diretório de texto encontrado")
        return stats

    for acervo_dir in sorted(TEXT_DIR.iterdir()):
        if acervo_dir.is_dir():
            bib = acervo_dir.name
            count = criar_chunks_acervo(bib, force=force)
            stats[bib] = count

    return stats


def carregar_chunks(bib: str = None) -> list[dict]:
    """Carrega chunks de um acervo específico ou de todos."""
    chunks = []

    if bib:
        chunk_file = CHUNKS_DIR / bib / "chunks.jsonl"
        if chunk_file.exists():
            chunks = _ler_jsonl(chunk_file)
    else:
        for chunk_file in sorted(CHUNKS_DIR.glob("*/chunks.jsonl")):
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
