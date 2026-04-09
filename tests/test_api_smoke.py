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
    dotenv.load_dotenv = lambda *args, **kwargs: None
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

        def post(self, *args, **kwargs):
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

    starlette = ModuleType("starlette")
    concurrency = ModuleType("starlette.concurrency")

    async def run_in_threadpool(func, *args, **kwargs):
        return func(*args, **kwargs)

    concurrency.run_in_threadpool = run_in_threadpool
    sys.modules["starlette"] = starlette
    sys.modules["starlette.concurrency"] = concurrency


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
            chunk_dir = Path(tmp) / "chunks"
            acervo_dir = text_dir / "029033_02"
            acervo_dir.mkdir(parents=True)
            chunk_dir.mkdir(parents=True)

            (acervo_dir / "029033_02_00001.txt").write_text(
                "texto original sem a expressão alvo",
                encoding="utf-8",
            )
            (acervo_dir / "029033_02_00001_corrigido.txt").write_text(
                "João Affonso Botelho aparece no texto corrigido",
                encoding="utf-8",
            )

            with patch.object(config, "TEXT_DIR", text_dir), \
                 patch.object(config, "CHUNKS_DIR", chunk_dir), \
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

    def test_build_fallback_page_substitui_nome_generico_e_limpa_ano(self):
        from src.web.entity_service import build_fallback_page
        from src.web.page_utils import serialize_page_record

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            text_dir = base / "text"
            images_dir = base / "images"
            cache_dir = base / "cache"
            bib_dir = text_dir / "029033_02"
            bib_dir.mkdir(parents=True)
            (images_dir / "029033_02").mkdir(parents=True)
            cache_dir.mkdir()

            (cache_dir / "acervos_pe.json").write_text(
                json.dumps([{"bib": "029033_02", "nome": "Diario de Pernambuco (PE) - 1840 a 1849"}]),
                encoding="utf-8",
            )
            (bib_dir / "029033_02_00001.txt").write_text("texto", encoding="utf-8")
            (bib_dir / "029033_02_00001.json").write_text(
                json.dumps(
                    {
                        "bib": "029033_02",
                        "pagina": 1,
                        "jornal": "Acervo 029033_02",
                        "periodico": "Acervo 029033_02",
                        "ano": "Ano 1840",
                        "edicao": "Edição 00001",
                    }
                ),
                encoding="utf-8",
            )

            page = build_fallback_page(bib="029033_02", pagina="1", text_dir=text_dir, images_dir=images_dir)
            self.assertEqual(page["jornal"], "Diario de Pernambuco (PE) - 1840 a 1849")

            with patch.object(config, "CACHE_DIR", cache_dir):
                serialized = serialize_page_record(page, images_dir)

            self.assertEqual(serialized["jornal"], "Diario de Pernambuco (PE) - 1840 a 1849")
            self.assertEqual(serialized["ano"], "1840")
            self.assertEqual(serialized["edicao"], "00001")

    def test_view_page_renderiza_html_documental(self):
        page = {
            "bib": "029033_02",
            "pagina": "1",
            "jornal": "Diario de Pernambuco (PE) - 1840 a 1849",
            "ano": "1840",
            "edicao": "00001",
            "image_url": None,
            "text_path": None,
            "image_path": None,
            "ocr_text": "PERNAMBUCO\\n\\nALFANDEGA DAS FAZENDAS.",
        }

        with patch.object(api, "StructuredRepository") as repo_cls, \
             patch.object(api, "_serialize_page_record", return_value=page):
            repo = repo_cls.return_value
            repo.get_page.return_value = {
                "bib": "029033_02",
                "pagina": "1",
                "jornal": "Diario de Pernambuco (PE) - 1840 a 1849",
                "ano": "1840",
                "edicao": "00001",
                "text_path": None,
                "image_path": None,
            }
            html = asyncio.run(api.view_page("029033_02", "1", q="Alfandega"))

        self.assertIn("Visualização documental", html)
        self.assertIn("Diario de Pernambuco", html)
        self.assertIn("ALFANDEGA DAS FAZENDAS", html)
        self.assertIn("Copiar citação", html)
        self.assertIn("Destacar termo nesta página", html)
        self.assertIn("Alfandega", html)

    def test_serialize_entity_adds_highlighted_page_view_url(self):
        from src.web.entity_service import serialize_entity

        entity = {
            "id": 1,
            "canonical_name": "João Affonso Botelho",
            "aliases_json": "[]",
            "attributes_json": "{}",
            "mentions": [
                {
                    "surface_form": "João Affonso Botelho",
                    "bib": "029033_02",
                    "pagina": "1",
                }
            ],
            "relations": [],
            "evidences": [],
        }

        serialized = serialize_entity(entity, lambda bib, pagina: None)
        self.assertIn("/page/029033_02/1?q=Jo%C3%A3o%20Affonso%20Botelho", serialized["mentions"][0]["page_view_url"])

    def test_review_relation_endpoint(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.review_relation.return_value = {
                "relation_id": 12,
                "review_status": "confirmed",
                "reviewer": "humano",
                "note": "",
                "created_at": "2026-04-06 10:00:00",
            }
            payload = asyncio.run(api.review_relation(12, status="confirmed", note="", reviewer="humano"))

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["review"]["review_status"], "confirmed")

    def test_review_entity_identity_endpoint(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.review_entity_identity.return_value = {
                "entity_id": 7,
                "review_status": "resolved",
                "reviewer": "humano",
                "note": "",
                "created_at": "2026-04-06 10:00:00",
            }
            payload = asyncio.run(api.review_entity_identity(7, status="resolved", note="", reviewer="humano"))

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["review"]["review_status"], "resolved")

    def test_review_queue_endpoint(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.get_review_queue.return_value = {
                "identities": [{"id": 1, "canonical_name": "João Botelho"}],
                "relations": [{"id": 2, "predicate": "spouse_of"}],
                "suspects": [{"id": 3, "canonical_name": "Deos Guarde"}],
                "merges": [{"source_id": 7, "target_id": 3}],
            }
            payload = asyncio.run(api.review_queue(n=5))

        self.assertEqual(payload["identities_total"], 1)
        self.assertEqual(payload["relations_total"], 1)
        self.assertEqual(payload["suspects_total"], 1)
        self.assertEqual(payload["merges_total"], 1)

    def test_discovery_featured_entity_endpoint_serializes_page_links(self):
        api._ROUTE_CACHE.clear()
        with patch.object(api, "StructuredRepository") as repo_cls, \
             patch.object(api, "_resolve_image_url", return_value="/images/029033_02/0001.jpg"):
            repo = repo_cls.return_value
            repo.get_featured_entity.return_value = {
                "id": 7,
                "canonical_name": "João Affonso Botelho",
                "top_snippet": {
                    "surface_form": "João Affonso Botelho",
                    "snippet": "João Affonso Botelho foi citado em nota oficial.",
                    "bib": "029033_02",
                    "pagina": "0001",
                    "jornal": "Diário de Pernambuco",
                    "ano": "Ano 1828",
                },
            }
            payload = asyncio.run(api.discovery_featured_entity(seed=91))

        self.assertEqual(payload["id"], 7)
        self.assertEqual(payload["top_snippet"]["page_api_url"], "/api/page/029033_02/0001")
        self.assertIn("/page/029033_02/0001", payload["top_snippet"]["page_view_url"])
        self.assertEqual(payload["top_snippet"]["image_url"], "/images/029033_02/0001.jpg")

    def test_discovery_surnames_endpoint_returns_grouped_payload(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.search_by_surname.return_value = [
                {
                    "group_key": "joao botelho",
                    "entity_id": 7,
                    "canonical_name": "João Botelho",
                    "total_mentions": 12,
                    "members": [{"id": 7, "canonical_name": "João Botelho"}],
                }
            ]
            payload = asyncio.run(api.discovery_surnames(q="Botelho", n=4))

        self.assertEqual(payload["query"], "Botelho")
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["groups"][0]["canonical_name"], "João Botelho")

    def test_merge_entities_endpoint(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.merge_entities.return_value = {
                "source_id": 7,
                "target_id": 3,
                "source_name": "João Botelho",
                "target_name": "Capitão João Botelho",
                "moved_mentions": 2,
                "merged_relations": 1,
                "dropped_self_relations": 0,
            }
            payload = asyncio.run(api.merge_entities(7, 3, note="consolidação manual", reviewer="humano"))

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["merge"]["target_id"], 3)

    def test_review_entity_merge_endpoint(self):
        with patch.object(api, "StructuredRepository") as repo_cls:
            repo = repo_cls.return_value
            repo.review_entity_merge_suggestion.return_value = {
                "source_entity_id": 7,
                "target_entity_id": 3,
                "review_status": "rejected",
                "reviewer": "humano",
                "note": "homônimos distintos",
                "created_at": "2026-04-08 12:00:00",
            }
            payload = asyncio.run(
                api.review_entity_merge(7, 3, status="rejected", note="homônimos distintos", reviewer="humano")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["review"]["review_status"], "rejected")


if __name__ == "__main__":
    unittest.main()
