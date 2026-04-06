import tempfile
import unittest
from pathlib import Path

from src.entities import extract_entities, normalize_name
from src.evidence_store import StructuredRepository
from src.relations import extract_relations
from src.structured_service import process_chunk


class StructuredExtractionTestCase(unittest.TestCase):
    def test_extract_entities_detects_person_and_title(self):
        entities = extract_entities(
            "Capitão Antonio Benedicto d'Araujo Pernambuco compareceu ao acto com João Affonso Botelho."
        )

        names = {entity.canonical_name: entity for entity in entities}
        self.assertIn("Antonio Benedicto d'Araujo Pernambuco", names)
        self.assertIn("João Affonso Botelho", names)
        self.assertEqual(names["Antonio Benedicto d'Araujo Pernambuco"].role, "Capitão")
        self.assertEqual(
            normalize_name("Antonio Benedicto d'Araujo Pernambuco"),
            normalize_name("Antonio Benedito de Araujo Pernambuco"),
        )

    def test_extract_entities_detects_institution_and_place(self):
        entities = extract_entities(
            "João Affonso Botelho compareceu à Associação Commercial do Recife em Olinda."
        )

        by_type = {(entity.entity_type, entity.canonical_name) for entity in entities}
        self.assertIn(("institution", "Associação Commercial do Recife"), by_type)
        self.assertIn(("place", "Olinda"), by_type)

    def test_extract_relations_detects_family_and_role(self):
        text = (
            "João Affonso Botelho, filho de Manoel Botelho, compareceu ao acto. "
            "João Affonso Botelho e sua esposa Maria Thereza Botelho assinaram o requerimento."
        )
        entities = extract_entities(text)
        relations = extract_relations(text, entities)

        predicates = {(item.subject_name, item.predicate, item.object_name or item.object_literal) for item in relations}
        self.assertIn(("João Affonso Botelho", "child_of", "Manoel Botelho"), predicates)
        self.assertIn(("João Affonso Botelho", "spouse_of", "Maria Thereza Botelho"), predicates)
        self.assertIn(("João Affonso Botelho", "mentioned_with", "Manoel Botelho"), predicates)

    def test_extract_relations_detects_institution_and_residence(self):
        text = (
            "João Affonso Botelho, sócio da Associação Commercial do Recife, "
            "morador em Olinda, requereu certidão."
        )
        entities = extract_entities(text)
        relations = extract_relations(text, entities)

        predicates = {(item.subject_name, item.predicate, item.object_name or item.object_literal) for item in relations}
        self.assertIn(("João Affonso Botelho", "member_of", "Associação Commercial do Recife"), predicates)
        self.assertIn(("João Affonso Botelho", "resident_of", "Olinda"), predicates)


class StructuredRepositoryTestCase(unittest.TestCase):
    def test_process_chunk_persists_page_entity_relation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00001_chunk0",
                "text": (
                    "Capitão João Affonso Botelho, filho de Manoel Botelho, compareceu. "
                    "João Affonso Botelho e sua esposa Maria Thereza Botelho assinaram."
                ),
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00001",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            summary = process_chunk(chunk, repo)
            self.assertEqual(summary["entities"], 3)
            self.assertGreaterEqual(summary["relations"], 2)

            results = repo.search_entities("joao affonso", limit=5)
            self.assertEqual(len(results), 1)

            entity = repo.get_entity(results[0]["id"])
            self.assertEqual(entity["canonical_name"], "João Affonso Botelho")
            self.assertTrue(entity["mentions"])
            self.assertTrue(entity["relations"])

            page = repo.get_page("029033_02", "029033_02_00001")
            self.assertIsNotNone(page)
            self.assertEqual(page["jornal"], "Diário de Pernambuco")

    def test_process_chunk_links_institution_and_place_relations(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00002_chunk0",
                "text": (
                    "João Affonso Botelho, sócio da Associação Commercial do Recife, "
                    "morador em Olinda, apresentou requerimento."
                ),
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00002",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(chunk, repo)
            results = repo.search_entities("joao affonso", limit=5)
            entity = repo.get_entity(results[0]["id"])

            predicates = {(item["predicate"], item["object_name"] or item["object_literal"]) for item in entity["relations"]}
            self.assertIn(("member_of", "Associação Commercial do Recife"), predicates)
            self.assertIn(("resident_of", "Olinda"), predicates)
            self.assertTrue(entity["mentions"][0]["snippet"])


if __name__ == "__main__":
    unittest.main()
