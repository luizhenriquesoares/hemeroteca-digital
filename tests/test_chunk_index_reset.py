import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.processing import chunker, indexer


class _FakeCollection:
    def __init__(self):
        self.docs = {
            "a1": {"bib": "029033_01"},
            "a2": {"bib": "029033_01"},
            "a3": {"bib": "029033_01"},
            "b1": {"bib": "029033_02"},
        }

    def get(self, ids=None, where=None, limit=None, include=None):
        if ids is not None:
            ids = ids if isinstance(ids, list) else [ids]
            found = [doc_id for doc_id in ids if doc_id in self.docs]
            return {"ids": found}

        if where is not None:
            bib = where.get("bib")
            found = [doc_id for doc_id, metadata in self.docs.items() if metadata.get("bib") == bib]
            if limit is not None:
                found = found[:limit]
            return {"ids": found}

        return {"ids": []}

    def delete(self, ids=None, where=None, where_document=None, limit=None):
        ids = ids or []
        for doc_id in ids:
            self.docs.pop(doc_id, None)
        return {"ids": ids}


class ChunkResetTests(unittest.TestCase):
    def test_limpar_chunks_acervo_remove_arquivos_do_bib(self):
        with tempfile.TemporaryDirectory() as tmp:
            chunks_dir = Path(tmp)
            bib_dir = chunks_dir / "029033_01"
            bib_dir.mkdir(parents=True)
            (bib_dir / "chunks.jsonl").write_text('{"id":"1"}\n{"id":"2"}\n{"id":"3"}\n', encoding="utf-8")
            (bib_dir / "manifest.json").write_text('{"status":"snapshot"}\n', encoding="utf-8")

            with patch.object(chunker, "CHUNKS_DIR", chunks_dir):
                result = chunker.limpar_chunks_acervo("029033_01")

            self.assertEqual(result["chunks_removed"], 3)
            self.assertEqual(result["files_removed"], 2)
            self.assertFalse(bib_dir.exists())


class IndexResetTests(unittest.TestCase):
    def test_limpar_indexacao_acervo_remove_em_batches(self):
        collection = _FakeCollection()

        with patch.object(indexer, "get_collection", return_value=collection):
            deleted = indexer.limpar_indexacao_acervo("029033_01", batch_size=2)

        self.assertEqual(deleted, 3)
        self.assertEqual(set(collection.docs.keys()), {"b1"})

    def test_reindexar_acervo_limpa_antes_de_indexar(self):
        with patch.object(indexer, "limpar_indexacao_acervo", return_value=7) as mock_clean, \
             patch.object(indexer, "indexar_acervo", return_value=11) as mock_index:
            result = indexer.reindexar_acervo("029033_01", batch_size=123)

        self.assertEqual(result, {"deleted": 7, "indexed": 11})
        mock_clean.assert_called_once_with("029033_01", batch_size=500)
        mock_index.assert_called_once_with("029033_01", batch_size=123)


if __name__ == "__main__":
    unittest.main()
