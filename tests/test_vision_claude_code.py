import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from src.vision import claude_code_reader as cc
from src.vision.reader import CONFIGS
from src.vision.tiles import make_tiles


def _page(width=600, height=900):
    return Image.new("L", (width, height), 255)


def _payload(structured, output_tokens=5000, is_error=False):
    return json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "is_error": is_error,
            "stop_reason": "end_turn",
            "total_cost_usd": 0.42,
            "usage": {
                "input_tokens": 10,
                "output_tokens": output_tokens,
                "cache_creation_input_tokens": 30000,
                "cache_read_input_tokens": 33000,
            },
            "structured_output": structured,
            "result": json.dumps(structured),
        }
    )


class ClaudeCodeReaderTests(unittest.TestCase):
    def test_build_command_and_prompt(self):
        tiles = make_tiles(_page(), grid=(2, 2))
        with tempfile.TemporaryDirectory() as tmp:
            paths = cc.write_tiles(tiles, "029033_01_00001", Path(tmp))
            self.assertEqual(len(paths), 4)
            self.assertTrue(all(path.exists() for path in paths))
            prompt = cc.build_prompt(tiles, paths, CONFIGS["sonnet-structure"])
        self.assertIn(str(paths[3]), prompt)
        self.assertIn("2 colunas por 2 faixas", prompt)
        self.assertIn("Modo ESTRUTURA", prompt)
        cmd = cc.build_command(prompt, CONFIGS["opus-full"], {"type": "object"})
        self.assertEqual(cmd[:3], ["claude", "-p", prompt])
        self.assertIn("opus", cmd)
        self.assertIn("--json-schema", cmd)
        self.assertIn("Read", cmd[cmd.index("--allowedTools") + 1])

    def test_read_page_parses_structured_output_and_costs(self):
        structured = {
            "newspaper": "Diario de Pernambuco",
            "segments": [
                {"type": "edital", "text": "Edital da Camara", "people": [{"name": "Joao Botelho"}], "tiles": [0]}
            ],
        }
        calls = []

        def fake_runner(cmd, timeout):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout=_payload(structured, output_tokens=8000), stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp) / "029033_01_00001.jpg"
            _page().save(image)
            config = CONFIGS["sonnet-full"].with_backend("claude-code")
            result = cc.read_page_claude_code(image, config, work_dir=Path(tmp), runner=fake_runner)

        self.assertIsNone(result.error)
        self.assertEqual(result.backend, "claude-code")
        self.assertEqual(result.extraction["segments"][0]["people"][0]["name"], "Joao Botelho")
        self.assertEqual(result.usage["output_tokens"], 8000)
        self.assertEqual(result.cost_usd_reported, 0.42)
        expected = (result.tiles_tokens_estimate + cc.PROMPT_TOKENS_ESTIMATE) * 2 / 1e6 + 8000 * 10 / 1e6
        self.assertAlmostEqual(result.cost_usd, round(expected, 6))
        self.assertAlmostEqual(result.cost_usd_batch, round(expected / 2, 6))
        self.assertEqual(len(calls), 1)

    def test_read_page_reports_cli_failure_and_bad_schema(self):
        def failing_runner(cmd, timeout):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="Not logged in")

        def bad_schema_runner(cmd, timeout):
            return subprocess.CompletedProcess(cmd, 0, stdout=_payload({"segments": [{"type": "invalido"}]}), stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp) / "029033_01_00002.jpg"
            _page().save(image)
            config = CONFIGS["haiku-structure"].with_backend("claude-code")
            failed = cc.read_page_claude_code(image, config, work_dir=Path(tmp), runner=failing_runner)
            bad = cc.read_page_claude_code(image, config, work_dir=Path(tmp), runner=bad_schema_runner)

        self.assertIn("Not logged in", failed.error)
        self.assertIn("fora do schema", bad.error)
        self.assertIsNone(bad.extraction)

    def test_reader_dispatches_by_backend(self):
        from src.vision import reader

        def fake_runner(cmd, timeout):
            return subprocess.CompletedProcess(cmd, 0, stdout=_payload({"segments": []}), stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp) / "029033_01_00003.jpg"
            _page().save(image)
            from unittest.mock import patch

            with patch.object(cc, "run_claude_code", fake_runner), patch.object(cc, "TILES_DIR", Path(tmp)):
                result = reader.read_page(image, CONFIGS["opus-structure"].with_backend("claude-code"))
        self.assertEqual(result.backend, "claude-code")
        self.assertEqual(result.extraction, {
            "newspaper": None, "issue_date": None, "issue_number": None, "page_number": None,
            "columns": None, "segments": [], "illegible_notes": None,
        })

    def test_with_backend_validates(self):
        with self.assertRaises(ValueError):
            CONFIGS["opus-full"].with_backend("gemini")


if __name__ == "__main__":
    unittest.main()
