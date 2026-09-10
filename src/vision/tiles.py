"""Recorte de páginas hi-res em tiles legíveis por modelos de visão.

O Claude reduz qualquer imagem para no máximo 1568 px no lado maior e ~1,15 MP.
Uma folha de jornal de 6,5k x 9k px viraria 1140 x 1568 px, ilegível para tipos
de corpo pequeno. Recortamos a página numa grade com leve sobreposição e
redimensionamos cada recorte nós mesmos para caber nos limites, assim o custo em
tokens é previsível (~ largura x altura / 750) e o texto continua legível.
"""

from __future__ import annotations

import base64
import io
import math
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

MAX_LONG_EDGE = 1568
MAX_PIXELS = 1_150_000
TOKENS_PER_PIXEL = 1 / 750
DEFAULT_GRID = (4, 3)
DEFAULT_OVERLAP = 0.03


@dataclass(frozen=True)
class Tile:
    index: int
    col: int
    row: int
    box: tuple[int, int, int, int]  # (x0, y0, x1, y1) em px da página original
    width: int  # após redimensionar
    height: int
    jpeg: bytes

    @property
    def tokens_estimate(self) -> int:
        return math.ceil(self.width * self.height * TOKENS_PER_PIXEL)

    def b64(self) -> str:
        return base64.standard_b64encode(self.jpeg).decode("ascii")


def parse_grid(spec: str) -> tuple[int, int]:
    """'4x3' -> (4, 3): colunas x faixas."""
    try:
        cols, rows = spec.lower().split("x")
        grid = (int(cols), int(rows))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Grade inválida: {spec!r} (use CxR, ex. 4x3)") from exc
    if grid[0] < 1 or grid[1] < 1:
        raise ValueError(f"Grade inválida: {spec!r}")
    return grid


def fit_size(
    width: int,
    height: int,
    *,
    max_long_edge: int = MAX_LONG_EDGE,
    max_pixels: int = MAX_PIXELS,
) -> tuple[int, int]:
    """Maior tamanho <= original que respeita lado maior e total de pixels."""
    scale = 1.0
    long_edge = max(width, height)
    if long_edge > max_long_edge:
        scale = max_long_edge / long_edge
    if width * height * scale * scale > max_pixels:
        scale = math.sqrt(max_pixels / (width * height))
    if scale >= 1.0:
        return width, height
    return max(1, int(width * scale)), max(1, int(height * scale))


def tile_boxes(
    width: int,
    height: int,
    cols: int,
    rows: int,
    overlap: float = DEFAULT_OVERLAP,
) -> list[tuple[int, int, tuple[int, int, int, int]]]:
    """Caixas (col, row, box) que cobrem a página com sobreposição relativa."""
    tile_w = width / cols
    tile_h = height / rows
    pad_w = tile_w * overlap
    pad_h = tile_h * overlap
    boxes = []
    for row in range(rows):
        for col in range(cols):
            x0 = max(0, int(col * tile_w - pad_w))
            y0 = max(0, int(row * tile_h - pad_h))
            x1 = min(width, int(math.ceil((col + 1) * tile_w + pad_w)))
            y1 = min(height, int(math.ceil((row + 1) * tile_h + pad_h)))
            boxes.append((col, row, (x0, y0, x1, y1)))
    return boxes


def make_tiles(
    image: Image.Image | Path | str,
    *,
    grid: tuple[int, int] = DEFAULT_GRID,
    overlap: float = DEFAULT_OVERLAP,
    quality: int = 85,
) -> list[Tile]:
    """Recorta a página numa grade CxR e devolve tiles JPEG em tons de cinza."""
    if not isinstance(image, Image.Image):
        image = Image.open(image)
    page = image.convert("L")
    cols, rows = grid
    tiles: list[Tile] = []
    for index, (col, row, box) in enumerate(tile_boxes(page.width, page.height, cols, rows, overlap)):
        crop = page.crop(box)
        target = fit_size(crop.width, crop.height)
        if target != (crop.width, crop.height):
            crop = crop.resize(target, Image.Resampling.LANCZOS)
        buffer = io.BytesIO()
        crop.save(buffer, format="JPEG", quality=quality, optimize=True)
        tiles.append(
            Tile(
                index=index,
                col=col,
                row=row,
                box=box,
                width=crop.width,
                height=crop.height,
                jpeg=buffer.getvalue(),
            )
        )
    return tiles


def page_tokens_estimate(tiles: list[Tile]) -> int:
    return sum(tile.tokens_estimate for tile in tiles)


def page_bytes(tiles: list[Tile]) -> int:
    return sum(len(tile.jpeg) for tile in tiles)
