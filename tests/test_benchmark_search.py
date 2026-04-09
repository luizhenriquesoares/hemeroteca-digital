import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

bench = importlib.import_module("src.benchmark_search")


class BenchmarkSearchTests(unittest.TestCase):
    def test_write_template_creates_valid_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "search_cases.template.json"
            bench.write_template(target)

            self.assertTrue(target.exists())
            payload = json.loads(target.read_text(encoding="utf-8"))
            self.assertIn("cases", payload)
            self.assertGreaterEqual(len(payload["cases"]), 2)

    def test_run_benchmark_writes_summary_and_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cases_path = base / "cases.json"
            cases_path.write_text(
                json.dumps(
                    {
                        "cases": [
                            {
                                "id": "sobrenome",
                                "query": "botelhos",
                                "relevant_pages": ["029033_02:1"],
                            },
                            {
                                "id": "nome_exato",
                                "query": "antonio benedicto",
                                "relevant_ids": ["doc-2"],
                            },
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_textual(query, n_results=10, filtro_bib=None):
                if query == "botelhos":
                    return [
                        {
                            "id": "doc-1",
                            "texto": "João Botelho foi nomeado.",
                            "metadata": {"bib": "029033_02", "pagina": "1", "jornal": "Diário"},
                            "score": 0.93,
                            "modo": "textual_historico",
                        }
                    ]
                return [
                    {
                        "id": "doc-x",
                        "texto": "Resultado irrelevante.",
                        "metadata": {"bib": "029033_02", "pagina": "9"},
                        "score": 0.11,
                        "modo": "textual_historico",
                    }
                ]

            def fake_semantica(query, n_results=10, filtro_bib=None):
                if query == "antonio benedicto":
                    return [
                        {
                            "id": "doc-2",
                            "texto": "Antonio Benedicto d'Araujo Pernambuco.",
                            "metadata": {"bib": "029033_02", "pagina": "4"},
                            "score": 0.87,
                            "modo": "semantica",
                        }
                    ]
                return []

            def fake_hibrida(query, n_results=10, filtro_bib=None):
                if query == "botelhos":
                    return [
                        {
                            "id": "doc-1",
                            "texto": "João Botelho foi nomeado.",
                            "metadata": {"bib": "029033_02", "pagina": "1"},
                            "score": 0.95,
                            "modo": "hibrida",
                        }
                    ]
                if query == "antonio benedicto":
                    return [
                        {
                            "id": "doc-2",
                            "texto": "Antonio Benedicto d'Araujo Pernambuco.",
                            "metadata": {"bib": "029033_02", "pagina": "4"},
                            "score": 0.89,
                            "modo": "hibrida",
                        }
                    ]
                return []

            with patch("src.benchmark_search.BENCHMARK_DIR", base / "bench"), \
                 patch("src.benchmark_search.buscar_textual_historica", side_effect=fake_textual), \
                 patch("src.benchmark_search.buscar_semantica", side_effect=fake_semantica), \
                 patch("src.benchmark_search.buscar_hibrida", side_effect=fake_hibrida):
                (base / "bench").mkdir(parents=True, exist_ok=True)
                summary = bench.run_benchmark(cases_path, n_results=5)

            summary_file = Path(summary["out_dir"]) / "summary.json"
            self.assertTrue(summary_file.exists())
            payload = json.loads(summary_file.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["runs"]), 3)

            textual = next(item for item in payload["runs"] if item["label"] == "textual")
            semantica = next(item for item in payload["runs"] if item["label"] == "semantica")
            hibrida = next(item for item in payload["runs"] if item["label"] == "hibrida")

            self.assertEqual(textual["queries"], 2)
            self.assertEqual(textual["hit_rate_at_5"], 0.5)
            self.assertEqual(semantica["hit_rate_at_5"], 0.5)
            self.assertEqual(hibrida["hit_rate_at_5"], 1.0)
            self.assertEqual(hibrida["mrr"], 1.0)
            self.assertEqual(hibrida["ndcg_at_5"], 1.0)


if __name__ == "__main__":
    unittest.main()
