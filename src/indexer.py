"""Módulo de indexação vetorial: indexa chunks no ChromaDB para busca RAG."""

import logging

import chromadb
from chromadb.config import Settings

from src.config import CHROMA_DIR, CHROMA_COLLECTION, EMBEDDING_MODEL
from src.chunker import carregar_chunks

logger = logging.getLogger(__name__)

_client = None
_collection = None


def get_client() -> chromadb.ClientAPI:
    """Retorna (ou cria) o cliente ChromaDB persistente."""
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(
            path=str(CHROMA_DIR),
            settings=Settings(anonymized_telemetry=False),
        )
    return _client


def get_collection(name: str = CHROMA_COLLECTION) -> chromadb.Collection:
    """Retorna (ou cria) a coleção ChromaDB com embedding multilíngue."""
    global _collection
    if _collection is None:
        client = get_client()
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        embedding_fn = SentenceTransformerEmbeddingFunction(
            model_name=EMBEDDING_MODEL,
        )
        _collection = client.get_or_create_collection(
            name=name,
            embedding_function=embedding_fn,
            metadata={"hnsw:space": "cosine"},
        )
    return _collection


def indexar_acervo(bib: str, batch_size: int = 100) -> int:
    """
    Indexa os chunks de um acervo no ChromaDB.

    Returns:
        número de chunks indexados
    """
    chunks = carregar_chunks(bib)
    if not chunks:
        logger.warning(f"Nenhum chunk encontrado para acervo {bib}")
        return 0

    collection = get_collection()

    # Verificar quais chunks já estão indexados
    existing_ids = set()
    try:
        all_ids = [c["id"] for c in chunks]
        # ChromaDB get só retorna existentes
        result = collection.get(ids=all_ids)
        existing_ids = set(result["ids"])
    except Exception:
        pass

    # Filtrar apenas novos
    new_chunks = [c for c in chunks if c["id"] not in existing_ids]

    if not new_chunks:
        logger.info(f"Acervo {bib}: todos os {len(chunks)} chunks já indexados")
        return 0

    # Indexar em batches
    indexed = 0
    for i in range(0, len(new_chunks), batch_size):
        batch = new_chunks[i:i + batch_size]

        ids = [c["id"] for c in batch]
        documents = [c["text"] for c in batch]
        metadatas = [c["metadata"] for c in batch]

        collection.add(ids=ids, documents=documents, metadatas=metadatas)
        indexed += len(batch)
        logger.debug(f"  Indexados {indexed}/{len(new_chunks)} chunks")

    logger.info(f"Acervo {bib}: {indexed} novos chunks indexados (total: {collection.count()})")
    return indexed


def indexar_todos(batch_size: int = 100) -> dict:
    """Indexa chunks de todos os acervos."""
    from src.config import CHUNKS_DIR
    stats = {}

    for acervo_dir in sorted(CHUNKS_DIR.iterdir()):
        if acervo_dir.is_dir():
            bib = acervo_dir.name
            count = indexar_acervo(bib, batch_size=batch_size)
            stats[bib] = count

    return stats


def buscar(query: str, n_results: int = 10, filtro_bib: str = None) -> list[dict]:
    """
    Busca semântica nos jornais indexados.

    Args:
        query: texto de busca
        n_results: número de resultados
        filtro_bib: filtrar por código de acervo específico

    Returns:
        lista de resultados com texto e metadados
    """
    collection = get_collection()

    where = None
    if filtro_bib:
        where = {"bib": filtro_bib}

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where,
        include=["documents", "metadatas", "distances"],
    )

    output = []
    for i in range(len(results["ids"][0])):
        output.append({
            "id": results["ids"][0][i],
            "texto": results["documents"][0][i],
            "metadata": results["metadatas"][0][i],
            "distancia": results["distances"][0][i],
        })

    return output


def stats() -> dict:
    """Retorna estatísticas do índice."""
    collection = get_collection()
    return {
        "total_chunks": collection.count(),
        "collection": CHROMA_COLLECTION,
        "embedding_model": EMBEDDING_MODEL,
    }
