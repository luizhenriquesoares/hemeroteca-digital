import importlib
import sys
import unittest
from types import ModuleType


tesserocr = ModuleType("tesserocr")
sys.modules["tesserocr"] = tesserocr

hires = importlib.import_module("src.hires_pipeline")


class FakeDriver:
    def execute_script(self, script):
        return {
            "pasta": "1889\\Ed. 42",
            "pagAtual": 7,
            "pagFis": "7",
        }


class HiresMetadataTests(unittest.TestCase):
    def test_get_page_metadata_parseia_pasta_e_periodico(self):
        metadata = hires._get_page_metadata(FakeDriver(), "029033_02", "Diário de Pernambuco", 17)

        self.assertEqual(metadata["bib"], "029033_02")
        self.assertEqual(metadata["jornal"], "Diário de Pernambuco")
        self.assertEqual(metadata["periodico"], "Diário de Pernambuco")
        self.assertEqual(metadata["pagina"], 17)
        self.assertEqual(metadata["pagina_logica"], 7)
        self.assertEqual(metadata["pagina_fisica"], "7")
        self.assertEqual(metadata["ano"], "1889")
        self.assertEqual(metadata["edicao"], "Ed. 42")


if __name__ == "__main__":
    unittest.main()
