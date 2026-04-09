import json
import tempfile
import unittest
from pathlib import Path

from src.structured.graph_store import build_graph, get_entity_subgraph
from src.structured.repository import StructuredRepository
from src.structured.service import process_chunk


class GraphStoreTests(unittest.TestCase):
    def _seed_repository(self, db_path: Path) -> StructuredRepository:
        repo = StructuredRepository(db_path)
        chunk = {
            "id": "029033_02_029033_02_00001_chunk0",
            "text": (
                "Capitão João Affonso Botelho, filho de Manoel Botelho, compareceu ao acto. "
                "João Affonso Botelho e sua esposa Maria Thereza Botelho assinaram o requerimento."
            ),
            "metadata": {
                "bib": "029033_02",
                "pagina": "029033_02_00001",
                "jornal": "Diario de Pernambuco (PE) - 1840 a 1849",
                "ano": "1840",
                "edicao": "00001",
            },
        }
        process_chunk(chunk, repo)
        return repo

    def test_build_graph_includes_documentary_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hemeroteca.db"
            self._seed_repository(db_path)

            graph = build_graph(db_path=db_path)

            node_types = {data["node_type"] for _, data in graph.nodes(data=True)}
            self.assertIn("person", node_types)
            self.assertIn("publication", node_types)
            self.assertIn("page", node_types)
            self.assertIn("role", node_types)

            predicates = {data["predicate"] for _, _, data in graph.edges(data=True)}
            self.assertIn("mentioned_in", predicates)
            self.assertIn("published_in", predicates)
            self.assertIn("spouse_of", predicates)

    def test_build_graph_edges_keep_provenance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hemeroteca.db"
            self._seed_repository(db_path)

            graph = build_graph(db_path=db_path)
            relation_edges = [
                data for _, _, data in graph.edges(data=True)
                if data.get("predicate") == "spouse_of"
            ]

            self.assertTrue(relation_edges)
            edge = relation_edges[0]
            self.assertTrue(edge["quote"])
            self.assertTrue(edge["page_view_url"])
            self.assertTrue(json.loads(edge["source_page_ids"]))
            self.assertTrue(json.loads(edge["chunk_ids"]))

    def test_get_entity_subgraph_exposes_page_navigation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hemeroteca.db"
            repo = self._seed_repository(db_path)
            entity_id = repo.search_entities("joao affonso", limit=1)[0]["id"]

            subgraph = get_entity_subgraph(entity_id, db_path=db_path, depth=2)

            page_nodes = [node for node in subgraph["nodes"] if node["type"] == "page"]
            self.assertTrue(page_nodes)
            self.assertTrue(page_nodes[0]["page_view_url"].startswith("/page/029033_02/"))

            mention_edges = [edge for edge in subgraph["edges"] if edge["predicate"] == "mentioned_in"]
            self.assertTrue(mention_edges)
            self.assertTrue(mention_edges[0]["page_view_url"].startswith("/page/029033_02/"))

            relation_edges = [edge for edge in subgraph["edges"] if edge["predicate"] == "spouse_of"]
            self.assertTrue(relation_edges)
            self.assertTrue(relation_edges[0]["evidences"])
            self.assertIn("page_view_url", relation_edges[0]["evidences"][0])


if __name__ == "__main__":
    unittest.main()
