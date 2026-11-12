import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from click.testing import CliRunner

from src.vision.layout import detect_regions
from src.vision.tiles import make_tiles, tiles_manifest
from src.vision.reader import CONFIGS, build_request
from src.vision.claude_code_reader import build_prompt
from src.vision.benchmark import select_configs
from src.vision.preview import preview_page


def newspaper():
    page = Image.new('L', (1200, 1600), 235)
    draw = ImageDraw.Draw(page)
    # Spanning masthead, a rule, then two independently readable columns.
    for x in range(100, 1080, 38):
        draw.rectangle((x, 80, x+25, 112), fill=40)
    draw.line((60, 155, 1140, 155), fill=20, width=3)
    for y in range(230, 1500, 36):
        for left, right in ((70, 550), (660, 1130)):
            for x in range(left, right, 24):
                draw.rectangle((x, y, x+13, y+17), fill=45)
    draw.line((600, 215, 600, 1520), fill=20, width=3)
    return page


class LayoutTests(unittest.TestCase):
    def test_spanning_header_and_columns_survive_rules_and_skew(self):
        for angle in (0, .6, -.6):
            page = newspaper().rotate(angle, fillcolor=235)
            regions, glyph = detect_regions(page)
            self.assertIsNotNone(glyph)
            self.assertTrue(any(x0 == 0 and x1 == 1200 and y1 < 500 for x0, y0, x1, y1 in regions), regions)
            self.assertTrue(any(x0 == 0 and 540 < x1 < 660 and y1 == 1600 for x0, y0, x1, y1 in regions), regions)
            self.assertTrue(any(540 < x0 < 660 and x1 == 1200 and y1 == 1600 for x0, y0, x1, y1 in regions), regions)

    def test_all_pixels_covered_and_overview_is_not_a_detail(self):
        page = newspaper()
        tiles = make_tiles(page, strategy='layout')
        self.assertEqual(tiles[0].kind, 'overview')
        self.assertLessEqual(max(tiles[0].width, tiles[0].height), 1000)
        covered = np.zeros((page.height, page.width), dtype=bool)
        for t in tiles[1:]:
            x0, y0, x1, y1 = t.box
            covered[y0:y1, x0:x1] = True
            self.assertLessEqual(t.width * t.height, 1150000)
            self.assertLessEqual(max(t.width, t.height), 1568)
        self.assertTrue(covered.all())
        self.assertEqual(len({t.index for t in tiles}), len(tiles))
        self.assertEqual(tiles_manifest(tiles)[0]['kind'], 'overview')

    def test_single_column_is_not_split_at_word_spaces(self):
        page = Image.new('L', (900, 1300), 255)
        draw = ImageDraw.Draw(page)
        for y in range(80, 1240, 30):
            for x in range(65, 840, 20):
                draw.rectangle((x, y, x+12, y+18), fill=0)
        regions, _ = detect_regions(page)
        self.assertEqual(regions, [(0, 0, 900, 1300)])

    def test_blank_and_tiny_pages_are_valid(self):
        for size in ((1, 1), (20, 10), (1200, 1600)):
            tiles = make_tiles(Image.new('L', size, 255), strategy='layout')
            self.assertGreaterEqual(len(tiles), 2)
            self.assertTrue(all(t.width > 0 and t.height > 0 for t in tiles))

    def test_layout_in_both_prompts_and_config_copy(self):
        tiles = make_tiles(newspaper(), strategy='layout')
        config = select_configs(['opus-full'], tiling='layout', effort='medium')[0]
        self.assertEqual(config.with_grid((2, 2)).with_backend('claude-code').tiling, 'layout')
        request = build_request(tiles, config)
        blocks = request['messages'][0]['content']
        self.assertIn('VISÃO GERAL', blocks[0]['text'])
        self.assertEqual(sum(b['type'] == 'image' for b in blocks), len(tiles))
        prompt = build_prompt(tiles, [Path(f'/tmp/{t.index}.jpg') for t in tiles], config)
        self.assertIn('caixa', prompt)
        self.assertNotIn('grade de', prompt)
        self.assertEqual(config.effort, 'medium')
        self.assertEqual(select_configs(['sonnet-structure'], effort='low')[0].effort, 'low')
        with self.assertRaises(ValueError):
            select_configs(['haiku-full'], effort='low')

    def test_preview_is_offline_and_serializes_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / 'page.jpg'
            newspaper().save(path)
            result = preview_page(path, root / 'preview')
            manifest = json.loads((root / 'preview' / 'manifest.json').read_text())
            self.assertEqual(result, manifest)
            self.assertTrue((root / 'preview' / 'layout.jpg').exists())
            self.assertEqual(len(list((root / 'preview').glob('tile_*.jpg'))), len(manifest['tiles']))
            # Exercise CLI too: preview must not need credentials or instantiate a client.
            import click
            from unittest.mock import patch
            from src.cli.commands.vision import register_vision_commands
            @click.group()
            def cli():
                pass
            register_vision_commands(cli)
            with patch('src.vision.reader.get_client', side_effect=AssertionError('network forbidden')):
                run = CliRunner().invoke(cli, ['benchmark-visao', '--page', str(path), '--tiling', 'layout',
                                              '--preview', '--out', str(root / 'cli')])
            self.assertEqual(run.exit_code, 0, run.output)
