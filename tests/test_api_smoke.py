import asyncio
import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch


def _install_test_stubs():
    dotenv = ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    sys.modules["dotenv"] = dotenv

    openai = ModuleType("openai")
    openai.OpenAI = object
    sys.modules["openai"] = openai

    fastapi = ModuleType("fastapi")

    class FastAPI:
        def __init__(self, *args, **kwargs):
            self.routes = []

        def add_middleware(self, *args, **kwargs):
            return None

        def mount(self, *args, **kwargs):
            return None

        def get(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    fastapi.FastAPI = FastAPI
    fastapi.Query = lambda default=None, **kwargs: default
    sys.modules["fastapi"] = fastapi

    middleware = ModuleType("fastapi.middleware")
    cors = ModuleType("fastapi.middleware.cors")
    cors.CORSMiddleware = object
    sys.modules["fastapi.middleware"] = middleware
    sys.modules["fastapi.middleware.cors"] = cors

    responses = ModuleType("fastapi.responses")
    responses.HTMLResponse = str
    responses.StreamingResponse = object
    sys.modules["fastapi.responses"] = responses

    staticfiles = ModuleType("fastapi.staticfiles")

    class StaticFiles:
        def __init__(self, *args, **kwargs):
            pass

    staticfiles.StaticFiles = StaticFiles
    sys.modules["fastapi.staticfiles"] = staticfiles


_install_test_stubs()

import src.config as config

api = importlib.import_module("src.api")


class ApiSmokeTests(unittest.TestCase):
    def test_extract_person_mentions_from_fontes(self):
        fontes = [
            {
                "jornal": "Diário de Pernambuco",
                "bib": "029033_02",
                "pagina": "0001",
                "evidencia": "Capitão Antonio Benedicto d'Araujo Pernambuco foi citado em nota oficial.",
            }
        ]
        people = api._extract_person_mentions("Antonio Benedito de Araujo Pernambuco", fontes)

        self.assertEqual(len(people), 1)
        self.assertIn("Antonio Benedicto d'Araujo Pernambuco", people[0])

    def test_parse_structured_answer(self):
        answer = """
1. Resumo interpretativo
Os Botelhos aparecem como grupo familiar ligado à administração provincial.

2. Pessoas e entidades citadas
- João Botelho, negociante citado no Diário de Pernambuco.
- Capitão Botelho, militar mencionado em nota oficial.

3. Evidências dos jornais
- "João Botelho" aparece em anúncio comercial.
- "Capitão Botelho" surge em comunicação administrativa.

4. Fontes
"""
        structured = api._parse_structured_answer(
            answer,
            [{"bib": "029033_02", "pagina": "0001", "jornal": "Diário de Pernambuco", "ano": "1889", "edicao": "42", "evidencia": "João Botelho"}],
        )

        self.assertIn("grupo familiar", structured["resumo"])
        self.assertEqual(len(structured["pessoas"]), 2)
        self.assertEqual(len(structured["evidencias"]), 2)
        self.assertEqual(structured["fontes"][0]["jornal"], "Diário de Pernambuco")

    def test_build_prosopographic_fallback(self):
        fontes = [
            {
                "jornal": "Diário de Pernambuco",
                "bib": "029033_02",
                "pagina": "0001",
                "evidencia": "João Botelho aparece em anúncio comercial do Recife.",
            }
        ]
        fallback = api._build_prosopographic_fallback("informacoes sobre os botelhos", fontes)

        self.assertTrue(fallback["evidencias"])

    def test_busca_textual_prefere_arquivo_corrigido(self):
        with tempfile.TemporaryDirectory() as tmp:
            text_dir = Path(tmp) / "text"
            acervo_dir = text_dir / "029033_02"
            acervo_dir.mkdir(parents=True)

            (acervo_dir / "029033_02_00001.txt").write_text(
                "texto original sem a expressão alvo",
                encoding="utf-8",
            )
            (acervo_dir / "029033_02_00001_corrigido.txt").write_text(
                "João Affonso Botelho aparece no texto corrigido",
                encoding="utf-8",
            )

            with patch.object(config, "TEXT_DIR", text_dir), \
                 patch.object(api, "TEXT_DIR", text_dir):
                result = api._busca_textual("joão affonso", n=10, bib="029033_02")

            self.assertEqual(result["total"], 1)
            self.assertEqual(result["resultados"][0]["metadata"]["pagina"], "029033_02_00001")
            self.assertIn("texto corrigido", result["resultados"][0]["texto"])

    def test_get_stats_usa_hires_progress_e_exclui_corrigidos(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            images_dir = base / "images"
            text_dir = base / "text"
            cache_dir = base / "cache"
            data_dir = base

            images_dir.mkdir()
            (images_dir / "029033_02").mkdir()

            acervo_dir = text_dir / "029033_02"
            acervo_dir.mkdir(parents=True)
            (acervo_dir / "029033_02_00001.txt").write_text("pagina 1", encoding="utf-8")
            (acervo_dir / "029033_02_00001_corrigido.txt").write_text("pagina 1 corrigida", encoding="utf-8")
            (acervo_dir / "029033_02_00002.txt").write_text("pagina 2", encoding="utf-8")

            cache_dir.mkdir()
            (cache_dir / "acervos_pe.json").write_text(
                json.dumps([
                    {"bib": "029033_02", "nome": "Diario", "paginas": 10},
                    {"bib": "029033_03", "nome": "Outro", "paginas": 5},
                ]),
                encoding="utf-8",
            )
            (cache_dir / "hires_progress.json").write_text(
                json.dumps(
                    {
                        "done": ["029033_02"],
                        "failed_pages": {"029033_03": [1, 2]},
                        "stats": {},
                    }
                ),
                encoding="utf-8",
            )

            fake_indexer = ModuleType("src.indexer")
            fake_indexer.stats = lambda: {"total_chunks": 7}

            with patch.object(api, "IMAGES_DIR", images_dir), \
                 patch.object(api, "DATA_DIR", data_dir), \
                 patch.object(api, "CACHE_DIR", cache_dir), \
                 patch.object(api, "TEXT_DIR", text_dir), \
                 patch.dict(sys.modules, {"src.indexer": fake_indexer}):
                stats = asyncio.run(api.get_stats())

            self.assertEqual(stats["acervos_total"], 2)
            self.assertEqual(stats["acervos_concluidos"], 1)
            self.assertEqual(stats["acervos_falhas"], 1)
            self.assertEqual(stats["textos_ocr"], 2)
            self.assertEqual(stats["paginas_processadas"], 2)
            self.assertEqual(stats["chunks_indexados"], 7)
            self.assertEqual(stats["progresso_pct"], 13.3)


if __name__ == "__main__":
    unittest.main()
