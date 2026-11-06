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
    kind: str = "detail"  # overview is orientation only
    strategy: str = "grid"

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
    strategy: str = "grid",
) -> list[Tile]:
    """Recorta a página numa grade CxR e devolve tiles JPEG em tons de cinza."""
    if not isinstance(image, Image.Image):
        image = Image.open(image)
    page = image.convert("L")
    if strategy not in ("grid", "layout"):
        raise ValueError(f"Estratégia inválida: {strategy}")
    cols, rows = grid
    if cols < 1 or rows < 1 or not 0 <= overlap < .5:
        raise ValueError("Grade deve ser positiva e sobreposição deve estar entre 0 e 0.5")
    boxes = tile_boxes(page.width, page.height, cols, rows, overlap)
    if strategy == "layout":
        from src.vision.layout import layout_boxes
        boxes, metadata = layout_boxes(page)
        pad = max(8, round((metadata["glyph_height_estimate"] or 20) * .4))
        boxes = [(col, row, (max(0, x0-pad), max(0, y0-pad), min(page.width, x1+pad), min(page.height, y1+pad)))
                 for col, row, (x0, y0, x1, y1) in boxes]
        boxes.insert(0, (-1, -1, (0, 0, page.width, page.height)))
    tiles: list[Tile] = []
    for index, (col, row, box) in enumerate(boxes):
        crop = page.crop(box)
        kind = "overview" if col == -1 else "detail"
        target = fit_size(crop.width, crop.height, max_long_edge=1000) if kind == "overview" else fit_size(crop.width, crop.height)
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
                kind=kind,
                strategy=strategy,
            )
        )
    return tiles


def page_tokens_estimate(tiles: list[Tile]) -> int:
    return sum(tile.tokens_estimate for tile in tiles)


def page_bytes(tiles: list[Tile]) -> int:
    return sum(len(tile.jpeg) for tile in tiles)


def describe_tiles(tiles: list[Tile]) -> str:
    if not tiles:
        raise ValueError("Página sem recortes")
    if tiles[0].strategy == "grid":
        cols = max(t.col for t in tiles) + 1
        rows = max(t.row for t in tiles) + 1
        return (f"Página dividida em {len(tiles)} recortes: grade de {cols} colunas por {rows} faixas, "
                "numerados da esquerda para a direita e de cima para baixo.")
    return ("Recortes adaptados ao layout. O recorte 0 é uma VISÃO GERAL apenas para orientação: "
            "não transcreva nem duplique seu texto. Leia os recortes de detalhe e use suas caixas "
            "(x0, y0, x1, y1), em pixels da página original, para reconstruir a ordem de leitura. "
            "Cabeçalhos podem atravessar colunas. Uma coluna deve ser lida de cima a baixo antes da próxima. "
            "As regiões e a ordem sugerida são heurísticas: confirme na visão geral. "
            "Sobreposições existem para recuperar palavras nas bordas, nunca duplique linhas.")


def tile_label(tile: Tile) -> str:
    if tile.kind == "overview":
        return f"Recorte {tile.index}: VISÃO GERAL (orientação apenas)"
    if tile.strategy == "layout":
        return f"Recorte {tile.index}: detalhe, caixa {tile.box}"
    return f"Recorte {tile.index} (coluna {tile.col + 1}, faixa {tile.row + 1})"


def tiles_manifest(tiles: list[Tile]) -> list[dict]:
    return [{"index": t.index, "kind": t.kind, "strategy": t.strategy, "box": list(t.box),
             "width": t.width, "height": t.height, "tokens_estimate": t.tokens_estimate,
             "scale": round(t.height / (t.box[3] - t.box[1]), 4)} for t in tiles]
