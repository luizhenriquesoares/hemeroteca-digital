"""Regressão: captura por ID precisa pular de página de verdade.

Bug observado em 2026-09-18: `processar_paginas_hires` capturou 11 vezes a mesma
imagem (hashes idênticos) porque `_process_page_with_retry` só força a recarga
(`hPagFis`) como *fallback* — com um `src` já válido em tela, ele baixa a imagem
da página anterior. Na varredura sequencial isso não acontece, porque
`_proxima_pagina` avança a página antes de cada leitura.

O teste fixa o contrato: para cada página alvo, a navegação acontece **antes** da
leitura, e o `prev_src` passado é o src anterior ao pulo — é ele que faz a espera
exigir uma imagem diferente, ou seja, a da página certa.
"""

import unittest
from unittest.mock import patch

from src.scraping import hires_pipeline


class PageJumpTestCase(unittest.TestCase):
    def _rodar(self, paginas):
        chamadas = {"refresh": [], "processa": [], "srcs": [], "setup_start": []}
        estado = {"src": None}

        def fake_setup(driver, bib, nome, start_page=1):
            # em produção o setup já carrega a PRIMEIRA página alvo em tela
            chamadas["setup_start"].append(start_page)
            estado["src"] = f"cache/pagina_{start_page}.JPG"
            return "low.JPG", estado["src"]

        def fake_get_cache_url(driver):
            return estado["src"]

        def fake_refresh(driver, page_num=None):
            chamadas["refresh"].append(page_num)
            # o DocReader passa a devolver a imagem da página pedida
            estado["src"] = f"cache/pagina_{page_num}.JPG"

        def fake_processa(**kwargs):
            chamadas["processa"].append(
                {"global_page": kwargs["global_page"], "prev_src": kwargs["prev_src"]}
            )
            chamadas["srcs"].append(estado["src"])
            return True, estado["src"], ""

        with patch.object(hires_pipeline, "_setup_acervo", side_effect=fake_setup), \
             patch.object(hires_pipeline, "_get_cache_url", side_effect=fake_get_cache_url), \
             patch.object(hires_pipeline, "_refresh_hires_view", side_effect=fake_refresh), \
             patch.object(hires_pipeline, "_process_page_with_retry", side_effect=fake_processa), \
             patch.object(hires_pipeline, "_page_already_done", return_value=False), \
             patch.object(hires_pipeline, "_get_page_metadata", return_value={}), \
             patch.object(hires_pipeline, "_clear_failed_page"), \
             patch.object(hires_pipeline, "_mark_failed_page"), \
             patch("src.scraping.driver.create_driver", return_value=type("D", (), {"quit": lambda s: None})()):
            stats = hires_pipeline.processar_paginas_hires(
                paginas, headless=True, keep_images=False
            )
        return chamadas, stats

    def test_navega_antes_de_cada_pagina_a_partir_da_segunda(self):
        chamadas, _ = self._rodar(["029033_01_06891", "029033_01_09202", "029033_01_10549"])
        # a primeira já está em tela pelo setup; as demais são navegadas
        self.assertEqual(chamadas["setup_start"], [6891])
        self.assertEqual(chamadas["refresh"], [9202, 10549])
        self.assertEqual([c["global_page"] for c in chamadas["processa"]], [6891, 9202, 10549])

    def test_prev_src_e_o_src_anterior_ao_pulo(self):
        """É o que obriga a espera a aceitar só uma imagem diferente."""
        chamadas, _ = self._rodar(["029033_01_06891", "029033_01_09202", "029033_01_10549"])
        prevs = [c["prev_src"] for c in chamadas["processa"]]
        # primeira: nada a esperar. Seguintes: o src da página anterior.
        self.assertIsNone(prevs[0])
        self.assertEqual(prevs[1], "cache/pagina_6891.JPG")
        self.assertEqual(prevs[2], "cache/pagina_9202.JPG")

    def test_cada_pagina_le_a_imagem_da_propria_pagina(self):
        chamadas, _ = self._rodar(["029033_01_06891", "029033_01_09202", "029033_01_10549"])
        self.assertEqual(
            chamadas["srcs"],
            ["cache/pagina_6891.JPG", "cache/pagina_9202.JPG", "cache/pagina_10549.JPG"],
        )

    def test_agrupa_por_acervo_e_ordena(self):
        chamadas, stats = self._rodar(
            ["029033_04_00041", "029033_01_09202", "029033_04_00002", "029033_01_06891"]
        )
        # cada acervo preserva a ordem crescente de páginas
        paginas_01 = [
            c["global_page"] for c in chamadas["processa"] if c["global_page"] in (6891, 9202)
        ]
        self.assertEqual(paginas_01, [6891, 9202])
        self.assertIn("029033_01", stats)
        self.assertIn("029033_04", stats)

    def test_primeira_pagina_nao_e_pulada_de_novo(self):
        """`_setup_acervo` já carrega a primeira página alvo.

        Pedir a MESMA página e esperar um src diferente trava a espera até
        esgotar as tentativas — foi o que derrubou 029033_10_00002,
        029033_13_00006 e 709611_00002, que são as primeiras de cada acervo.
        """
        chamadas, _ = self._rodar(["029033_10_00002", "029033_10_00006", "029033_10_00012"])
        # a primeira página não gera refresh nem exige src novo
        self.assertEqual(chamadas["refresh"], [6, 12])
        self.assertIsNone(chamadas["processa"][0]["prev_src"])
        self.assertNotEqual(chamadas["processa"][1]["prev_src"], chamadas["processa"][0]["prev_src"])

    def test_pagina_unica_nao_dispara_refresh(self):
        chamadas, _ = self._rodar(["709611_00002"])
        self.assertEqual(chamadas["refresh"], [])
        self.assertIsNone(chamadas["processa"][0]["prev_src"])
        self.assertEqual(chamadas["processa"][0]["global_page"], 2)


class ParsePageIdsTestCase(unittest.TestCase):
    def test_formato_valido(self):
        self.assertEqual(
            hires_pipeline.parse_page_ids(["029033_01_09202", "709611_00002"]),
            {"029033_01": [9202], "709611": [2]},
        )

    def test_agrupa_e_ordena(self):
        self.assertEqual(
            hires_pipeline.parse_page_ids(["029033_01_10549", "029033_01_06891"]),
            {"029033_01": [6891, 10549]},
        )

    def test_ignora_vazios(self):
        self.assertEqual(hires_pipeline.parse_page_ids(["", "  ", "029033_01_00001"]),
                         {"029033_01": [1]})

    def test_recusa_id_malformado(self):
        for invalido in ["sem_formato", "029033_01_abc", "_00001"]:
            with self.assertRaises(ValueError):
                hires_pipeline.parse_page_ids([invalido])


if __name__ == "__main__":
    unittest.main()
