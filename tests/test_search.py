import importlib
import sys
import unittest
from types import ModuleType
from unittest.mock import patch


fake_chunker = ModuleType("src.chunker")
fake_chunker.carregar_chunks = lambda bib=None: []
sys.modules["src.chunker"] = fake_chunker

fake_indexer = ModuleType("src.indexer")
fake_indexer.buscar = lambda query, n_results=10, filtro_bib=None: []
sys.modules["src.indexer"] = fake_indexer

search = importlib.import_module("src.search")


class SearchTests(unittest.TestCase):
    def test_enrich_metadata_usa_cache_de_acervos(self):
        with patch("src.search._load_acervo_cache", return_value={"029033_02": {"bib": "029033_02", "nome": "Diário de Pernambuco"}}):
            enriched = search._enrich_metadata({"bib": "029033_02", "pagina": "1"})

        self.assertEqual(enriched["jornal"], "Diário de Pernambuco")
        self.assertEqual(enriched["periodico"], "Diário de Pernambuco")

    def test_normalizacao_historica(self):
        normalized = search.normalize_text("Capitão Antonio Benedicto d'Araujo Pernambuco")
        self.assertIn("capitao", normalized)
        self.assertIn("antonio", normalized)
        self.assertIn("benedito", normalized)
        self.assertIn("de araujo", normalized)

    def test_focus_query_remove_ruido_e_plural(self):
        focus = search.focus_query("busque informacoes sobre os botelhos")
        self.assertEqual(focus, "botelhos")

    def test_extract_evidence_snippet_foca_no_termo(self):
        text = (
            "Anúncios diversos sem relação. "
            "O Capitão Antonio Benedicto d'Araujo Pernambuco foi citado em nota oficial "
            "sobre a administração provincial. "
            "Outro anúncio sem relação no final."
        )
        snippet = search.extract_evidence_snippet("Antonio Benedito de Araujo Pernambuco", text, max_chars=120)
        self.assertIn("Antonio Benedicto", snippet)
        self.assertNotEqual(snippet, text[:120])

    def test_busca_textual_historica_recupera_variacao_de_nome(self):
        docs = [
            {
                "id": "x1",
                "texto": "Capitão Antonio Benedicto d'Araujo Pernambuco assumiu o comando.",
                "metadata": {"bib": "029033_02", "pagina": "1"},
            },
            {
                "id": "x2",
                "texto": "Outro texto sem relação.",
                "metadata": {"bib": "029033_02", "pagina": "2"},
            },
        ]

        with patch("src.search._load_search_docs", return_value=docs):
            results = search.buscar_textual_historica(
                "Antonio Benedito de Araujo Pernambuco",
                n_results=5,
                filtro_bib="029033_02",
            )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "x1")
        self.assertGreaterEqual(results[0]["score"], 0.75)

    def test_busca_textual_historica_recupera_sobrenome_no_plural(self):
        docs = [
            {
                "id": "x1",
                "texto": "João Botelho foi nomeado para o cargo.",
                "metadata": {"bib": "029033_02", "pagina": "1"},
            }
        ]

        with patch("src.search._load_search_docs", return_value=docs):
            results = search.buscar_textual_historica("informacoes sobre os botelhos", n_results=5)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "x1")

    def test_busca_hibrida_faz_merge_de_textual_e_semantica(self):
        textual = [
            {
                "id": "x1",
                "texto": "Capitão Antonio Benedicto d'Araujo Pernambuco.",
                "metadata": {"bib": "029033_02", "pagina": "1"},
                "score": 0.91,
                "modo": "textual_historico",
                "matched_tokens": ["antonio", "benedito", "araujo", "pernambuco"],
            }
        ]
        semantica = [
            {
                "id": "x1",
                "texto": "Capitão Antonio Benedicto d'Araujo Pernambuco.",
                "metadata": {"bib": "029033_02", "pagina": "1"},
                "score": 0.62,
                "modo": "semantica",
            }
        ]

        with patch("src.search.buscar_textual_historica", return_value=textual), \
             patch("src.search.buscar_semantica", return_value=semantica):
            results = search.buscar_hibrida("Antonio Benedito de Araujo Pernambuco", n_results=5)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["modo"], "hibrida")
        self.assertGreater(results[0]["score"], 0.91)


if __name__ == "__main__":
    unittest.main()
