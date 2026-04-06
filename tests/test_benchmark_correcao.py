import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

bench = importlib.import_module("src.benchmark_correcao")


class BenchmarkCorrecaoTests(unittest.TestCase):
    def test_run_benchmark_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            src = base / "sample.txt"
            src.write_text("Texto OCR de teste com Antonio Benedicto d'Araujo.", encoding="utf-8")

            with patch("src.benchmark_correcao.BENCHMARK_DIR", base / "bench"), \
                 patch("src.benchmark_correcao.corrigir_texto", side_effect=["saida mini", "saida max", "saida claude"]):
                (base / "bench").mkdir(parents=True, exist_ok=True)
                summary = bench.run_benchmark(src, sample_chars=0)

            self.assertEqual(len(summary["results"]), 3)
            summary_file = Path(summary["out_dir"]) / "summary.json"
            self.assertTrue(summary_file.exists())
            data = json.loads(summary_file.read_text(encoding="utf-8"))
            self.assertEqual(len(data["results"]), 3)


if __name__ == "__main__":
    unittest.main()
