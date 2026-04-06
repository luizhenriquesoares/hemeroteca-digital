import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

fake_tesserocr = ModuleType("tesserocr")
fake_tesserocr.image_to_text = lambda *args, **kwargs: ""
sys.modules.setdefault("tesserocr", fake_tesserocr)

bench = importlib.import_module("src.benchmark_ocr")


class BenchmarkOCRTests(unittest.TestCase):
    def test_run_benchmark_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            image = base / "sample.jpg"
            saved = base / "sample.txt"
            corrected = base / "sample_corrigido.txt"
            image.write_bytes(b"fake")
            saved.write_text("Texto OCR ruim", encoding="utf-8")
            corrected.write_text("Texto OCR corrigido", encoding="utf-8")

            fake_result = type(
                "FakeOCRResult",
                (),
                {
                    "text": "Texto OCR adaptativo",
                    "variant": "balanced_psm6",
                },
            )()

            with patch("src.benchmark_ocr.BENCHMARK_DIR", base / "bench"), \
                 patch("src.benchmark_ocr.extrair_texto_com_qualidade", return_value=fake_result):
                (base / "bench").mkdir(parents=True, exist_ok=True)
                summary = bench.run_benchmark(image, saved_ocr_path=saved, corrected_path=corrected)

            self.assertEqual(len(summary["results"]), 3)
            summary_file = Path(summary["out_dir"]) / "summary.json"
            self.assertTrue(summary_file.exists())
            data = json.loads(summary_file.read_text(encoding="utf-8"))
            self.assertEqual(len(data["results"]), 3)


if __name__ == "__main__":
    unittest.main()
