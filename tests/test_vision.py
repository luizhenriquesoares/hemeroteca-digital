import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from src.vision import benchmark as bench
from src.vision import tiles as tiles_mod
from src.vision.reader import CONFIGS, ReadConfig, ReadResult, build_request, estimate_cost
from src.vision.schema import PageExtraction


def _synthetic_page(width: int = 1200, height: int = 1800) -> Image.Image:
    image = Image.new("RGB", (width, height), "white")
    for y in range(0, height, 40):
        for x in range(0, width, 3):
            image.putpixel((x, y), (0, 0, 0))
    return image


class TilesTests(unittest.TestCase):
    def test_parse_grid(self):
        self.assertEqual(tiles_mod.parse_grid("4x3"), (4, 3))
        self.assertEqual(tiles_mod.parse_grid("2X2"), (2, 2))
        with self.assertRaises(ValueError):
            tiles_mod.parse_grid("abc")
        with self.assertRaises(ValueError):
            tiles_mod.parse_grid("0x3")

    def test_fit_size_respects_long_edge_and_pixels(self):
        self.assertEqual(tiles_mod.fit_size(800, 600), (800, 600))
        w, h = tiles_mod.fit_size(6491, 8940)
        self.assertLessEqual(max(w, h), tiles_mod.MAX_LONG_EDGE)
        self.assertLessEqual(w * h, tiles_mod.MAX_PIXELS)
        w, h = tiles_mod.fit_size(1500, 1500)  # lado ok, pixels demais
        self.assertLessEqual(w * h, tiles_mod.MAX_PIXELS)
        self.assertLess(max(w, h), 1500)

    def test_tile_boxes_cover_page_with_overlap(self):
        boxes = tiles_mod.tile_boxes(1000, 900, cols=4, rows=3, overlap=0.05)
        self.assertEqual(len(boxes), 12)
        self.assertEqual(boxes[0][2][:2], (0, 0))
        self.assertEqual(boxes[-1][2][2:], (1000, 900))
        first, second = boxes[0][2], boxes[1][2]
        self.assertGreater(first[2], second[0])  # sobreposição horizontal
        self.assertEqual([(c, r) for c, r, _ in boxes[:5]], [(0, 0), (1, 0), (2, 0), (3, 0), (0, 1)])

    def test_make_tiles_downscales_and_estimates_tokens(self):
        tiles = tiles_mod.make_tiles(_synthetic_page(4000, 6000), grid=(2, 2))
        self.assertEqual(len(tiles), 4)
        for tile in tiles:
            self.assertLessEqual(max(tile.width, tile.height), tiles_mod.MAX_LONG_EDGE)
            self.assertLessEqual(tile.width * tile.height, tiles_mod.MAX_PIXELS)
            self.assertTrue(tile.jpeg.startswith(b"\xff\xd8"))
            self.assertGreater(tile.tokens_estimate, 0)
        self.assertEqual(tiles_mod.page_tokens_estimate(tiles), sum(t.tokens_estimate for t in tiles))
        self.assertTrue(tiles[0].b64())


class ReaderTests(unittest.TestCase):
    def test_estimate_cost_sonnet_and_batch(self):
        usage = {"input_tokens": 10_000, "output_tokens": 2_000}
        self.assertAlmostEqual(estimate_cost(usage, "claude-sonnet-5"), 0.04)
        self.assertAlmostEqual(estimate_cost(usage, "claude-sonnet-5", batch=True), 0.02)
        usage_cache = {"input_tokens": 0, "cache_read_input_tokens": 10_000, "output_tokens": 0}
        self.assertAlmostEqual(estimate_cost(usage_cache, "claude-opus-5"), 0.005)
        self.assertEqual(estimate_cost(usage, "modelo-desconhecido"), 0.0)

    def test_build_request_includes_tiles_mode_and_effort(self):
        tiles = tiles_mod.make_tiles(_synthetic_page(), grid=(2, 3))
        request = build_request(tiles, CONFIGS["opus-structure"])
        self.assertEqual(request["model"], "claude-opus-5")
        self.assertEqual(request["output_config"], {"effort": "medium"})
        content = request["messages"][0]["content"]
        images = [block for block in content if block["type"] == "image"]
        self.assertEqual(len(images), 6)
        self.assertEqual(images[0]["source"]["media_type"], "image/jpeg")
        self.assertIn("Modo ESTRUTURA", content[-1]["text"])
        self.assertIn("2 colunas por 3 faixas", content[0]["text"])

    def test_haiku_config_has_no_effort_and_full_mode_text(self):
        tiles = tiles_mod.make_tiles(_synthetic_page(), grid=(1, 1))
        request = build_request(tiles, CONFIGS["haiku-full"])
        self.assertNotIn("output_config", request)
        self.assertIn("TRANSCRIÇÃO COMPLETA", request["messages"][0]["content"][-1]["text"])

    def test_invalid_mode_rejected(self):
        tiles = tiles_mod.make_tiles(_synthetic_page(), grid=(1, 1))
        with self.assertRaises(ValueError):
            build_request(tiles, ReadConfig("x", "claude-opus-5", mode="resumo"))

    def test_schema_roundtrip(self):
        payload = {
            "newspaper": "Diario de Pernambuco",
            "issue_date": "7 de Novembro de 1825",
            "segments": [
                {
                    "type": "fuga_escravizado",
                    "text": "Fugio hum escravo por nome Antonio...",
                    "people": [{"name": "Antonio", "role": "escravo", "context": "fugitivo"}],
                    "relations": [
                        {"subject": "Antonio", "predicate": "slave_of", "object": "Manoel da Silva", "quote": "escravo de Manoel da Silva"}
                    ],
                    "tiles": [0, 4],
                }
            ],
        }
        page = PageExtraction.model_validate(payload)
        self.assertEqual(page.segments[0].type.value, "fuga_escravizado")
        self.assertEqual(page.segments[0].relations[0].predicate.value, "slave_of")
        self.assertEqual(page.full_text(), "Fugio hum escravo por nome Antonio...")
        self.assertEqual(page.model_dump(mode="json")["segments"][0]["type"], "fuga_escravizado")


class ClientAuthTests(unittest.TestCase):
    def test_get_client_prefers_env_api_key(self):
        from src.vision import reader

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-ant-teste"}, clear=False), \
             patch.object(reader, "_ant_access_token", side_effect=AssertionError("não deveria consultar o ant")):
            client = reader.get_client()
        self.assertEqual(client.api_key, "sk-ant-teste")

    def test_get_client_falls_back_to_ant_profile(self):
        from src.vision import reader

        env = {k: v for k, v in __import__("os").environ.items() if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}
        with patch.dict("os.environ", env, clear=True), \
             patch.object(reader, "_ant_access_token", return_value="sk-ant-oat01-abc"):
            client = reader.get_client()
        self.assertEqual(client.auth_token, "sk-ant-oat01-abc")
        self.assertEqual(client.default_headers.get("anthropic-beta"), reader.OAUTH_BETA_HEADER)

    def test_get_client_raises_without_any_credential(self):
        from src.vision import reader

        env = {k: v for k, v in __import__("os").environ.items() if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}
        with patch.dict("os.environ", env, clear=True), patch.object(reader, "_ant_access_token", return_value=None):
            with self.assertRaises(RuntimeError):
                reader.get_client()


class BenchmarkTests(unittest.TestCase):
    def test_resolve_page_id_without_image(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            with patch.object(bench, "IMAGES_DIR", base / "images"), patch.object(bench, "TEXT_DIR", base / "text"):
                resolved = bench.resolve_page("029033_01_00044")
        self.assertEqual(resolved["id"], "029033_01_00044")
        self.assertEqual(resolved["bib"], "029033_01")
        self.assertIsNone(resolved["image"])
        self.assertIsNone(resolved["reference"])

    def test_quality_metrics_overlap(self):
        extraction = {
            "segments": [
                {"type": "edital", "text": "Edital da Camara Municipal do Recife sobre limpeza das ruas",
                 "people": [{"name": "Joao Botelho"}], "relations": [{"subject": "a", "predicate": "other", "object": "b", "quote": "c"}]}
            ]
        }
        metrics = bench.quality_metrics(extraction, "edital camara municipal recife limpeza ruas sujeira")
        self.assertEqual(metrics["segments"], 1)
        self.assertEqual(metrics["people"], 1)
        self.assertEqual(metrics["relations"], 1)
        self.assertGreater(metrics["valid_word_ratio"], 0.9)
        self.assertAlmostEqual(metrics["ref_word_overlap"], 6 / 7, places=3)
        self.assertEqual(bench.quality_metrics(None, None)["segments"], 0)

    def test_select_configs_validates_and_applies_grid(self):
        configs = bench.select_configs(["sonnet-full"], grid=(3, 3))
        self.assertEqual(configs[0].grid, (3, 3))
        self.assertEqual(len(bench.select_configs(None)), len(CONFIGS))
        with self.assertRaises(ValueError):
            bench.select_configs(["inexistente"])

    def test_run_benchmark_with_fake_reader(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            image_dir = base / "images" / "029033_01"
            image_dir.mkdir(parents=True)
            _synthetic_page(600, 900).save(image_dir / "029033_01_00001.jpg")
            text_dir = base / "text" / "029033_01"
            text_dir.mkdir(parents=True)
            (text_dir / "029033_01_00001_corrigido.txt").write_text("Diario de Pernambuco edital camara", encoding="utf-8")

            def fake_read(image_path, config, client=None, tiles=None):
                result = ReadResult(
                    config=config.name, model=config.model, mode=config.mode, page=Path(image_path).stem,
                    grid=config.grid, tiles=len(tiles), tiles_tokens_estimate=100, tiles_bytes=10,
                    usage={"input_tokens": 20_000, "output_tokens": 8_000}, elapsed_s=12.0, stop_reason="end_turn",
                )
                result.cost_usd = estimate_cost(result.usage, config.model)
                result.cost_usd_batch = estimate_cost(result.usage, config.model, batch=True)
                result.extraction = {"segments": [{"type": "edital", "text": "Diario de Pernambuco edital da camara", "people": [], "relations": []}]}
                return result

            with patch.object(bench, "IMAGES_DIR", base / "images"), patch.object(bench, "TEXT_DIR", base / "text"):
                summary = bench.run_benchmark(
                    ["029033_01_00001", "029033_01_99999"],
                    ["sonnet-full", "haiku-structure"],
                    out_dir=base / "bench",
                    extrapolate_pages=1000,
                    read_fn=fake_read,
                )

            self.assertEqual(summary["missing_images"], ["029033_01_99999"])
            self.assertEqual(summary["pages"], ["029033_01_00001"])
            sonnet = summary["by_config"]["sonnet-full"]
            self.assertEqual(sonnet["pages_ok"], 1)
            self.assertAlmostEqual(sonnet["cost_usd_page"], 0.12)
            self.assertAlmostEqual(sonnet["cost_usd_total_batch"], 60.0)
            self.assertAlmostEqual(sonnet["ref_word_overlap"], 1.0)
            self.assertTrue((base / "bench" / "summary.json").exists())
            self.assertIn("sonnet-full", (base / "bench" / "summary.md").read_text(encoding="utf-8"))
            self.assertTrue((base / "bench" / "029033_01_00001__haiku-structure.json").exists())
            data = json.loads((base / "bench" / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(len(data["results"]), 2)


if __name__ == "__main__":
    unittest.main()
