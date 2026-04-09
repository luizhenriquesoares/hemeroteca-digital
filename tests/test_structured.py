import tempfile
import unittest
from pathlib import Path

from src.entities import extract_entities, normalize_name
from src.evidence_store import StructuredRepository
from src.relations import extract_relations
from src.structured.identity import resolve_entity_identity
from src.structured_service import process_chunk
from src.structured_models import PageReference


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


    def test_extract_relations_detects_appointment_death_travel_signature(self):
        text = (
            "O Coronel Joze Fernandes Silva, nomeado para o cargo de Inspector das Obras Publicas. "
            "Na Barbada falleceo o Capitão Antonio Pereira de Mello depois de longa enfermidade. "
            "O Tenente Antonio Caetano Ferraz embarcou no porto de Recife. "
            "Este documento foi assinado por Joze Antonio da Silva Maia."
        )
        entities = extract_entities(text)
        relations = extract_relations(text, entities)

        predicates = {(item.subject_name, item.predicate) for item in relations}
        # Nomeação
        appointed = [r for r in relations if r.predicate == "appointed_to"]
        self.assertTrue(any("Inspector" in (r.object_literal or "") for r in appointed))
        # Óbito
        deceased = [r for r in relations if r.predicate == "deceased"]
        self.assertTrue(len(deceased) >= 1)
        # Viagem
        traveled = [r for r in relations if r.predicate == "traveled_to"]
        self.assertTrue(any("Recife" in (r.object_literal or "") for r in traveled))
        # Assinatura
        signed = [r for r in relations if r.predicate == "signed_by"]
        self.assertTrue(len(signed) >= 1)
        # Na Barbada não deve ser subject de deceased
        deceased_subjects = [r.subject_name for r in deceased]
        self.assertFalse(any("Barbada" in s for s in deceased_subjects))


class StructuredRepositoryTestCase(unittest.TestCase):
    def test_identity_resolution_disambiguates_short_or_titled_people(self):
        entities = extract_entities(
            "Capitão João Botelho compareceu à Associação Commercial do Recife."
        )
        person = next(entity for entity in entities if entity.entity_type == "person")
        page = PageReference(bib="029033_02", pagina="1", jornal="Diário", ano="1889", edicao="42")

        resolution = resolve_entity_identity(person, page, entities)

        self.assertIn("title:capitao", resolution.identity_key)
        self.assertIn("inst:associacao commercial do recife", resolution.identity_key)
        self.assertEqual(resolution.status, "contextual")

    def test_identity_resolution_uses_family_cooccurrence_for_short_names(self):
        entities = extract_entities(
            "João Botelho compareceu com Manoel Botelho ao acto oficial."
        )
        person = next(entity for entity in entities if entity.canonical_name == "João Botelho")
        page = PageReference(bib="029033_02", pagina="1", jornal="Diário", ano="1889", edicao="42")

        resolution = resolve_entity_identity(person, page, entities)

        self.assertIn("family:manoel botelho", resolution.identity_key)
        self.assertEqual(resolution.status, "contextual")

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

    def test_process_chunk_persists_identity_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00003_chunk0",
                "text": "Capitão João Botelho compareceu à Associação Commercial do Recife.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00003",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(chunk, repo)
            results = repo.search_entities("joao botelho", limit=5)
            entity = repo.get_entity(results[0]["id"])

            self.assertIn("identity_status", entity["attributes_json"])
            self.assertIn("identity_key", entity["attributes_json"])

    def test_search_entities_considers_alias_variants(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00004_chunk0",
                "text": "João Affonso Botelho compareceu ao acto official.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00004",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(chunk, repo)
            results = repo.search_entities("João Afonso Botelho", limit=5)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["canonical_name"], "João Affonso Botelho")

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

    def test_relation_review_overrides_effective_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00005_chunk0",
                "text": "João Affonso Botelho e sua esposa Maria Thereza Botelho assinaram o requerimento.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00005",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(chunk, repo)
            entity = repo.get_entity(repo.search_entities("joao affonso", limit=1)[0]["id"])
            spouse_relation = next(item for item in entity["relations"] if item["predicate"] == "spouse_of")

            repo.review_relation(spouse_relation["id"], review_status="confirmed")
            refreshed = repo.get_entity(entity["id"])
            spouse_relation = next(item for item in refreshed["relations"] if item["predicate"] == "spouse_of")

            self.assertEqual(spouse_relation["effective_status"], "confirmed")
            self.assertEqual(spouse_relation["review_status"], "confirmed")

    def test_entity_identity_review_overrides_effective_identity_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00006_chunk0",
                "text": "Capitão João Botelho compareceu à Associação Commercial do Recife.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00006",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(chunk, repo)
            entity_id = repo.search_entities("joao botelho", limit=1)[0]["id"]
            repo.review_entity_identity(entity_id, review_status="resolved", note="homônimo consolidado manualmente")
            entity = repo.get_entity(entity_id)

            self.assertIsNotNone(entity["identity_review"])
            self.assertIn("effective_identity_status", entity["attributes_json"])
            self.assertIn("resolved", entity["attributes_json"])

    def test_review_queue_lists_pending_identity_and_relation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            chunk = {
                "id": "029033_02_029033_02_00007_chunk0",
                "text": "Capitão João Botelho e sua esposa Maria Thereza Botelho assinaram o requerimento.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00007",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }
            process_chunk(chunk, repo)

            queue = repo.get_review_queue(limit=10)
            self.assertTrue(queue["identities"])
            self.assertTrue(queue["relations"])

    def test_merge_entities_consolidates_mentions_and_hides_source_from_search(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            contextual_chunk = {
                "id": "029033_02_029033_02_00008_chunk0",
                "text": "Capitão João Botelho compareceu à Associação Commercial do Recife.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00008",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }
            ambiguous_chunk = {
                "id": "029033_02_029033_02_00009_chunk0",
                "text": "João Botelho compareceu ao acto oficial.",
                "metadata": {
                    "bib": "029033_02",
                    "pagina": "029033_02_00009",
                    "jornal": "Diário de Pernambuco",
                    "ano": "1889",
                    "edicao": "42",
                },
            }

            process_chunk(contextual_chunk, repo)
            process_chunk(ambiguous_chunk, repo)

            matches = repo.search_entities("joão botelho", limit=10)
            self.assertGreaterEqual(len(matches), 2)

            candidates = {
                item["id"]: item
                for item in matches
            }
            source_id = next(
                item["id"] for item in matches
                if '"identity_key": "joao botelho::year:1889"' in repo.get_entity(item["id"])["attributes_json"]
            )
            target_id = next(item_id for item_id in candidates if item_id != source_id)

            merged = repo.merge_entities(source_id, target_id, note="consolidação manual")
            self.assertIsNotNone(merged)
            self.assertEqual(merged["moved_mentions"], 1)

            refreshed = repo.search_entities("joão botelho", limit=10)
            self.assertEqual(len(refreshed), 1)

            target = repo.get_entity(target_id)
            self.assertGreaterEqual(len(target["mentions"]), 2)

            source = repo.get_entity(source_id)
            self.assertEqual(source["identity_review"]["review_status"], "merged")

    def test_review_queue_exposes_merge_candidates_and_noise_suspects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            process_chunk(
                {
                    "id": "029033_02_029033_02_00010_chunk0",
                    "text": "Capitão João Botelho compareceu à Associação Commercial do Recife.",
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": "029033_02_00010",
                        "jornal": "Diário de Pernambuco",
                        "ano": "1889",
                        "edicao": "42",
                    },
                },
                repo,
            )
            process_chunk(
                {
                    "id": "029033_02_029033_02_00011_chunk0",
                    "text": "João Botelho requereu providências.",
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": "029033_02_00011",
                        "jornal": "Diário de Pernambuco",
                        "ano": "1889",
                        "edicao": "42",
                    },
                },
                repo,
            )
            process_chunk(
                {
                    "id": "029033_02_029033_02_00012_chunk0",
                    "text": "Deos Guarde.",
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": "029033_02_00012",
                        "jornal": "Diário de Pernambuco",
                        "ano": "1889",
                        "edicao": "42",
                    },
                },
                repo,
            )

            queue = repo.get_review_queue(limit=10)
            joao = next(item for item in queue["identities"] if item["canonical_name"] == "João Botelho")
            self.assertTrue(joao["merge_candidates"])
            self.assertIn("noise_assessment", joao)

            suspect_names = {item["canonical_name"] for item in queue["suspects"]}
            self.assertIn("Deos Guarde", suspect_names)
            self.assertTrue(queue["merges"])

    def test_merge_review_queue_hides_rejected_pair(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = StructuredRepository(Path(tmpdir) / "hemeroteca.db")
            process_chunk(
                {
                    "id": "029033_02_029033_02_00013_chunk0",
                    "text": "Capitão João Botelho compareceu à Associação Commercial do Recife.",
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": "029033_02_00013",
                        "jornal": "Diário de Pernambuco",
                        "ano": "1889",
                        "edicao": "42",
                    },
                },
                repo,
            )
            process_chunk(
                {
                    "id": "029033_02_029033_02_00014_chunk0",
                    "text": "João Botelho requereu providências.",
                    "metadata": {
                        "bib": "029033_02",
                        "pagina": "029033_02_00014",
                        "jornal": "Diário de Pernambuco",
                        "ano": "1889",
                        "edicao": "42",
                    },
                },
                repo,
            )

            merges = repo.get_merge_review_queue(limit=10)
            self.assertTrue(merges)
            first = merges[0]

            review = repo.review_entity_merge_suggestion(
                first["source_id"],
                first["target_id"],
                review_status="rejected",
                note="homônimos distintos",
            )
            self.assertEqual(review["review_status"], "rejected")

            refreshed = repo.get_merge_review_queue(limit=10)
            pair_keys = {(item["source_id"], item["target_id"]) for item in refreshed}
            self.assertNotIn((first["source_id"], first["target_id"]), pair_keys)


if __name__ == "__main__":
    unittest.main()
