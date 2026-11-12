"""Offline previews: exact model images, source coordinates and comparison costs."""
from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from src.vision.tiles import make_tiles, tiles_manifest, page_tokens_estimate


def preview_page(image_path: Path, out: Path, *, grid=(4, 3), strategy="layout") -> dict:
    out.mkdir(parents=True, exist_ok=True)
    tiles = make_tiles(image_path, grid=grid, strategy=strategy)
    baseline = make_tiles(image_path, grid=grid)
    with Image.open(image_path) as source:
        overlay = source.convert("RGB")
    source_size = overlay.size
    overlay.thumbnail((1400, 1800))
    sx, sy = overlay.width / source_size[0], overlay.height / source_size[1]
    draw = ImageDraw.Draw(overlay)
    font = ImageFont.load_default(size=24)
    colors = ["#e11d48", "#2563eb", "#059669", "#9333ea", "#d97706"]
    for t in tiles:
        (out / f"tile_{t.index:02d}.jpg").write_bytes(t.jpeg)
        if t.kind == "overview":
            continue
        x0, y0, x1, y1 = t.box
        box = (round(x0*sx), round(y0*sy), round(x1*sx)-1, round(y1*sy)-1)
        color = colors[t.index % len(colors)]
        draw.rectangle(box, outline=color, width=3)
        draw.text((box[0]+8, box[1]+8), str(t.index), fill=color, font=font, stroke_width=2, stroke_fill="white")
    overlay.save(out / "layout.jpg")
    estimated, original = page_tokens_estimate(tiles), page_tokens_estimate(baseline)
    payload = {"image": str(image_path.resolve()), "source_size": list(source_size), "tiling": strategy,
               "tiles": tiles_manifest(tiles), "image_tokens_estimate": estimated,
               "grid_baseline": list(grid), "grid_image_tokens_estimate": original,
               "estimated_image_token_reduction_pct": round((1-estimated/original)*100, 1),
               "note": "Estimativa por pixels, inclui visão geral; não mede fidelidade ou custo real de API."}
    (out / "manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
