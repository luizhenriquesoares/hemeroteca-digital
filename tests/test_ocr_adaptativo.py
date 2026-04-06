import importlib
import sys
import unittest
from types import ModuleType


fake_tesserocr = ModuleType("tesserocr")
fake_tesserocr.image_to_text = lambda *args, **kwargs: ""
sys.modules.setdefault("tesserocr", fake_tesserocr)

ocr = importlib.import_module("src.ocr")


class OCRAdaptativoTestCase(unittest.TestCase):
    def test_score_prefere_texto_legivel(self):
        bom = (
            "Diario de Pernambuco\n"
            "João Affonso Botelho foi nomeado para o cargo de escrivão da mesa.\n"
            "A notícia segue em texto corrido, com palavras inteiras e boa estrutura.\n"
        )
        ruim = "NUMER0 !!\nJ0RNL\nX7 %%\nA8B C9D\n"

        score_bom = ocr._score_ocr_text(bom)
        score_ruim = ocr._score_ocr_text(ruim)

        self.assertGreater(score_bom["score"], score_ruim["score"])
        self.assertGreater(score_bom["valid_word_ratio"], score_ruim["valid_word_ratio"])

    def test_select_best_ocr_result_escolhe_maior_score(self):
        candidates = [
            ocr.OCRResult(text="texto ruim", score=0.31, variant="a", metrics={"valid_word_ratio": 0.2, "chars": 50}),
            ocr.OCRResult(text="texto melhor", score=0.82, variant="b", metrics={"valid_word_ratio": 0.7, "chars": 300}),
            ocr.OCRResult(text="texto medio", score=0.61, variant="c", metrics={"valid_word_ratio": 0.5, "chars": 200}),
        ]

        best = ocr._select_best_ocr_result(candidates)
        self.assertEqual(best.variant, "b")
        self.assertEqual(best.text, "texto melhor")

    def test_compare_with_existing_keeps_old_when_new_not_clearly_better(self):
        existing = (
            "ALFANDEGA DAS FAZENDAS.\n"
            "Vicente Thomaz Pires de Figueredo Camargo faz saber que no dia 2 de Janeiro.\n"
        )
        new = ocr.OCRResult(
            text="ALFANDEGA DAS PAZENDAS.\nVicente Thomaz Pires de Figueredo Camargo faz saber q no dia 2 de Janeiro.\n",
            score=0.99,
            variant="lighter_psm6",
            metrics={
                "score": 0.99,
                "chars": 100,
                "valid_word_ratio": 0.99,
                "short_line_ratio": 0.0,
                "odd_char_ratio": 0.02,
            },
        )

        comparison = ocr.compare_with_existing(existing, new)
        self.assertEqual(comparison.selected_source, "existing")
        self.assertEqual(comparison.reason, "existing_ocr_kept")

    def test_compare_with_existing_promotes_new_when_gain_is_clear(self):
        existing = (
            "5 bb AAA eo TAP NT TAS a eso So CSS SO 6 E Ba DA\n"
            "%%% ### @@ ~~ //\n"
            "ALFANDIEGA DAS FAZENDAS.\n"
            "Fscripturario Theo!oro\n"
        )
        new = ocr.OCRResult(
            text=(
                "ALFANDEGA DAS FAZENDAS.\n"
                "Vicente Thomaz Pires de Figueredo Camargo faz saber que no dia 2 de Janeiro.\n"
            ),
            score=0.95,
            variant="balanced_psm6",
            metrics={
                "score": 0.95,
                "chars": 110,
                "valid_word_ratio": 0.95,
                "short_line_ratio": 0.0,
                "odd_char_ratio": 0.001,
            },
        )

        comparison = ocr.compare_with_existing(existing, new)
        self.assertEqual(comparison.selected_source, "new")
        self.assertEqual(comparison.reason, "new_ocr_won")


if __name__ == "__main__":
    unittest.main()
