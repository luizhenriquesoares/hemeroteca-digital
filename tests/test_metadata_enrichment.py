import importlib
import unittest

meta = importlib.import_module("src.metadata_enrichment")


class MetadataEnrichmentTests(unittest.TestCase):
    def test_merge_metadata_preserves_existing_and_updates_fresh_values(self):
        existing = {
            "bib": "029033_02",
            "arquivo_texto": "029033_02_00001.txt",
            "caracteres": 1000,
            "palavras": 200,
        }
        fresh = {
            "jornal": "Diário de Pernambuco",
            "ano": "1839",
            "edicao": "42",
            "pagina_fisica": "7",
        }

        merged = meta._merge_metadata(existing, fresh)
        self.assertEqual(merged["arquivo_texto"], "029033_02_00001.txt")
        self.assertEqual(merged["caracteres"], 1000)
        self.assertEqual(merged["jornal"], "Diário de Pernambuco")
        self.assertEqual(merged["ano"], "1839")


if __name__ == "__main__":
    unittest.main()
