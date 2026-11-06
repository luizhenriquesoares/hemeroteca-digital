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


class ScoreNaoPremiaLinhaLongaTestCase(unittest.TestCase):
    """Regressão: o score premiava caracteres por linha.

    Isso fazia a seleção preferir a variante que lê um jornal de duas colunas
    como uma coluna só — a pior segmentação possível — porque ela produz linhas
    longuíssimas. Mesmas palavras, só a quebra de linha muda: o score tem de ser
    o mesmo.
    """

    PALAVRAS = (
        "Faltando nesta cidade assaz populosa um Diario de Annuncios por meio do "
        "qual se facilitassem as transacções e se communicassem ao publico noticias "
        "que a cada um em particular podem interessar o administrador da Typographia "
        "se propoz a publicar todos os dias da semana excepto os Domingos somente o "
        "presente Diario no qual debaixo dos titulos de Compras Vendas Leiloes "
        "Alugueis Arrendamentos Aforamento Roubos Perdas Achados Fugidas"
    )

    def test_mesmas_palavras_quebradas_diferente_pontuam_igual(self):
        palavras = self.PALAVRAS.split()
        por_linha = 10
        curtas = "\n".join(
            " ".join(palavras[i:i + por_linha]) for i in range(0, len(palavras), por_linha)
        )
        longas = " ".join(palavras)

        score_curtas = ocr._score_ocr_text(curtas)
        score_longas = ocr._score_ocr_text(longas)

        # nenhuma linha tem menos de 8 letras: short_line_ratio é 0 nos dois
        self.assertEqual(score_curtas["short_line_ratio"], 0.0)
        self.assertEqual(score_longas["short_line_ratio"], 0.0)
        self.assertEqual(score_curtas["score"], score_longas["score"])

    def test_leitura_de_coluna_unica_nao_vence_por_linha_longa(self):
        """A variante de coluna única produzia linhas longas e menos texto."""
        palavras = self.PALAVRAS.split()
        colunar = "\n".join(
            " ".join(palavras[i:i + 8]) for i in range(0, len(palavras), 8)
        )
        coluna_unica = " ".join(palavras)
        self.assertLessEqual(
            ocr._score_ocr_text(coluna_unica)["score"],
            ocr._score_ocr_text(colunar)["score"],
        )


class ConsensoEntreLeiturasTestCase(unittest.TestCase):
    """A escolha entre variantes usa concordância, não plausibilidade isolada."""

    def _resultado(self, texto, score, variante):
        return ocr.OCRResult(
            text=texto, score=score, variant=variante, metrics=ocr._score_ocr_text(texto)
        )

    def test_maioria_vence_outlier_de_score_alto(self):
        certo = "João Parreira da Cunha ao largo da Matriz taes annuncios "
        errado = "João Ferreira da Cunha no largo da Matriz estes anuncios "
        candidatos = [
            self._resultado(certo * 8, 0.50, "maioria_1"),
            self._resultado(certo * 8, 0.50, "maioria_2"),
            self._resultado(errado * 8, 0.99, "outlier"),
        ]
        escolhido = ocr._select_best_by_consensus(candidatos)
        self.assertIn(escolhido.variant, {"maioria_1", "maioria_2"})

    def test_texto_curto_nao_vence_por_omissao(self):
        longo = "palavra " * 120
        curto = "palavra " * 6
        candidatos = [
            self._resultado(longo, 0.80, "longo_1"),
            self._resultado(longo, 0.80, "longo_2"),
            self._resultado(curto, 0.99, "curto"),
        ]
        self.assertNotEqual(ocr._select_best_by_consensus(candidatos).variant, "curto")

    def test_reordenar_colunas_derruba_a_concordancia(self):
        from src.processing.ocr_quality import text_agreement

        colunar = "primeira coluna continua aqui segunda coluna comeca depois"
        reordenado = "segunda coluna comeca depois primeira coluna continua aqui"
        self.assertEqual(text_agreement(colunar, colunar), 1.0)
        self.assertLess(text_agreement(colunar, reordenado), 0.6)

    def test_candidato_unico_e_lista_vazia(self):
        unico = self._resultado("texto unico", 0.7, "so")
        self.assertIs(ocr._select_best_by_consensus([unico]), unico)
        vazio = ocr._select_best_by_consensus([])
        self.assertEqual(vazio.text, "")
        self.assertEqual(vazio.variant, "empty")


class PreprocessamentoClaheTestCase(unittest.TestCase):
    """O caminho padrão é CLAHE sem binarização; o antigo segue disponível."""

    def _pagina(self):
        import tempfile
        from pathlib import Path
        from PIL import Image, ImageDraw

        imagem = Image.new("L", (400, 300), color=205)
        desenho = ImageDraw.Draw(imagem)
        for y in range(20, 280, 20):
            desenho.line((10, y, 390, y), fill=70, width=2)
        caminho = Path(tempfile.mkdtemp()) / "pagina.png"
        imagem.save(caminho)
        return caminho

    def test_clahe_preserva_tamanho_e_nao_binariza(self):
        caminho = self._pagina()
        resultado = ocr.preprocessar_imagem(caminho, method="clahe", upscale=1)
        self.assertEqual(resultado.size, (400, 300))
        self.assertEqual(resultado.mode, "L")
        # sem binarização: mais de dois níveis de cinza na saída
        self.assertGreater(sum(1 for n in resultado.histogram() if n), 2)

    def test_binarize_legado_continua_disponivel_e_binariza(self):
        caminho = self._pagina()
        resultado = ocr.preprocessar_imagem(caminho, method="binarize", upscale=1)
        self.assertEqual(resultado.size, (400, 300))
        self.assertLessEqual(sum(1 for n in resultado.histogram() if n), 2)

    def test_metodo_invalido_falha_alto(self):
        caminho = self._pagina()
        with self.assertRaises(ValueError):
            ocr.preprocessar_imagem(caminho, method="inventado")

    def test_variantes_usam_clahe_por_padrao(self):
        metodos = {variante.get("method") for variante in ocr.OCR_VARIANTS}
        self.assertIn("clahe", metodos)
        # ao menos uma variante legada permanece, para o consenso ter
        # uma leitura independente da família CLAHE
        self.assertIn("binarize", metodos)


if __name__ == "__main__":
    unittest.main()