import tempfile
import unittest
from pathlib import Path

from src.structured.repository import StructuredRepository
from src.structured.service import process_chunk


class DiscoveryFeaturesTestCase(unittest.TestCase):
    def _build_repo_with_sample_data(self):
        tmpdir = tempfile.TemporaryDirectory()
        repo = StructuredRepository(Path(tmpdir.name) / "hemeroteca.db")

        chunks = [
            {
                "id": "029033_02_00001_chunk0",
                "text": (
                    "Capitão João Affonso Botelho compareceu com Manoel Botelho à "
                    "Associação Commercial do Recife em Olinda."
                ),
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00001",
                    "jornal": "Diário de Pernambuco",
                    "ano": "Ano 1827",
                    "edicao": "Edição 00001",
                },
            },
            {
                "id": "029033_02_00002_chunk0",
                "text": (
                    "João Affonso Botelho, morador em Olinda, e Maria Thereza Botelho "
                    "assinaram requerimento perante a Camara."
                ),
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00002",
                    "jornal": "Diário de Pernambuco",
                    "ano": "Ano 1828",
                    "edicao": "Edição 00002",
                },
            },
            {
                "id": "029033_02_00003_chunk0",
                "text": (
                    "Na sessão da Camara, João Affonso Botelho foi citado novamente com "
                    "Manoel Botelho e a Associação Commercial do Recife."
                ),
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00003",
                    "jornal": "Diário de Pernambuco",
                    "ano": "Ano 1828",
                    "edicao": "Edição 00003",
                },
            },
        ]

        for offset in range(4, 16):
            page = f"029033_02_{offset:05d}"
            year = 1827 if offset % 2 == 0 else 1828
            chunks.append(
                {
                    "id": f"{page}_chunk0",
                    "text": (
                        "João Affonso Botelho, morador em Olinda, apresentou "
                        "requerimento perante a Camara e foi citado novamente."
                    ),
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": page,
                        "jornal": "Diário de Pernambuco",
                        "ano": f"Ano {year}",
                        "edicao": f"Edição {offset:05d}",
                    },
                }
            )

        for chunk in chunks:
            process_chunk(chunk, repo)

        return tmpdir, repo

    def test_discovery_overview_returns_timeline_hotspots_and_questions(self):
        tmpdir, repo = self._build_repo_with_sample_data()
        self.addCleanup(tmpdir.cleanup)

        overview = repo.get_discovery_overview(
            bib="029033_02",
            year_from=1827,
            year_to=1828,
            limit=5,
        )

        self.assertEqual(overview["scope"]["pages"], 15)
        self.assertTrue(overview["timeline"])
        self.assertEqual([item["year"] for item in overview["timeline"]], [1827, 1828])
        self.assertTrue(overview["top_people"])
        self.assertTrue(overview["document_hotspots"])
        self.assertTrue(overview["research_questions"])
        self.assertTrue(any("João Affonso Botelho" in item for item in overview["research_questions"]))
        self.assertIsNotNone(overview["period_snapshot"])

    def test_get_entity_includes_story_connections_and_milestones(self):
        tmpdir, repo = self._build_repo_with_sample_data()
        self.addCleanup(tmpdir.cleanup)

        entity_id = repo.search_entities("joao affonso", limit=1)[0]["id"]
        entity = repo.get_entity(entity_id)

        self.assertIn("story", entity)
        self.assertTrue(entity["story"]["timeline"])
        self.assertTrue(entity["story"]["milestones"])
        self.assertTrue(entity["story"]["research_questions"])
        self.assertTrue(
            entity["story"]["connections"]["people"]
            or entity["story"]["connections"]["places"]
            or entity["story"]["connections"]["institutions"]
        )

    def test_entity_comparison_returns_overlap_bridges_and_pages(self):
        tmpdir, repo = self._build_repo_with_sample_data()
        self.addCleanup(tmpdir.cleanup)

        comparison = None
        for left in repo.search_entities("joao affonso", limit=10):
            for right in repo.search_entities("manoel botelho", limit=5):
                candidate = repo.get_entity_comparison(left["id"], right["id"], limit=5)
                if candidate and candidate["overlap"]["shared_pages"] > 0:
                    comparison = candidate
                    break
            if comparison:
                break

        self.assertIsNotNone(comparison)
        self.assertTrue(comparison["timeline"])
        self.assertGreaterEqual(comparison["overlap"]["shared_pages"], 1)
        self.assertTrue(comparison["shared_pages"])
        self.assertTrue(comparison["research_questions"])

    def test_featured_entity_returns_curated_daily_profile(self):
        tmpdir, repo = self._build_repo_with_sample_data()
        self.addCleanup(tmpdir.cleanup)

        featured = repo.get_featured_entity(seed=0)

        self.assertIsNotNone(featured)
        self.assertEqual(featured["canonical_name"], "João Affonso Botelho")
        self.assertGreaterEqual(featured["mentions"], 10)
        self.assertGreaterEqual(featured["relation_count"], 1)
        self.assertTrue(featured["top_snippet"])
        self.assertIn("João Affonso Botelho", featured["discovery_question"])

    def test_search_by_surname_groups_variants_and_roles(self):
        tmpdir, repo = self._build_repo_with_sample_data()
        self.addCleanup(tmpdir.cleanup)

        groups = repo.search_by_surname("Botelho", limit=5)

        self.assertTrue(groups)
        self.assertEqual(groups[0]["canonical_name"], "João Affonso Botelho")
        self.assertTrue(any(member["canonical_name"] == "João Affonso Botelho" for member in groups[0]["members"]))
        self.assertGreaterEqual(groups[0]["total_mentions"], 10)
        self.assertGreaterEqual(groups[0]["variant_count"], 1)


if __name__ == "__main__":
    unittest.main()
