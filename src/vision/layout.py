"""Conservative whitespace segmentation; detection never changes source pixels.

XY cuts are inferred on a small copy. Rules and speckles are removed only from
that detection mask. The complete source rectangle is partitioned, so faint text
and edge annotations are retained even when the detector cannot recognize them.
"""
from __future__ import annotations

import math

import numpy as np
from PIL import Image


def _runs(values):
    edges = np.diff(np.r_[False, values, False].astype(np.int8))
    return list(zip(np.flatnonzero(edges == 1), np.flatnonzero(edges == -1)))


def detect_regions(page: Image.Image):
    import cv2

    scale = min(1.0, 1600 / max(page.size))
    small = page.resize((max(1, round(page.width * scale)), max(1, round(page.height * scale))))
    gray = np.asarray(small, dtype=np.uint8)
    if min(gray.shape) < 32 or int(gray.max()) - int(gray.min()) < 12:
        return [(0, 0, page.width, page.height)], None
    mask = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                 cv2.THRESH_BINARY_INV, 41, 12)
    h, w = mask.shape
    # Ignore scanner borders for inference; source crops still include them.
    border = max(1, round(min(w, h) * .012))
    mask[:border] = mask[-border:] = 0
    mask[:, :border] = mask[:, -border:] = 0
    # Hough also catches slightly slanted newspaper rules and scanner edges.
    lines = cv2.HoughLinesP(mask, 1, np.pi / 720, threshold=max(30, w // 8),
                            minLineLength=max(40, w // 4), maxLineGap=12)
    rule_evidence = cv2.dilate(mask, np.ones((3, 3), np.uint8))
    if lines is not None:
        for x0, y0, x1, y1 in lines[:, 0]:
            dx, dy = abs(int(x1) - int(x0)), abs(int(y1) - int(y0))
            samples = max(dx, dy) + 1
            xs = np.linspace(x0, x1, samples).astype(int)
            ys = np.linspace(y0, y1, samples).astype(int)
            continuity = np.mean(rule_evidence[ys, xs] > 0)
            if continuity > .9 and (dy <= dx * .07 or dx <= dy * .07):
                cv2.line(mask, (x0, y0), (x1, y1), 0, 5)
    for kernel in [(max(20, w // 6), 1), (1, max(20, h // 6))]:
        lines = cv2.morphologyEx(mask, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, kernel))
        mask = cv2.subtract(mask, cv2.dilate(lines, np.ones((3, 3), np.uint8)))
    count, labels, stats, _ = cv2.connectedComponentsWithStats(mask)
    keep = np.zeros(count, dtype=np.uint8)
    heights = []
    for i, (x, y, cw, ch, area) in enumerate(stats[1:], 1):
        if area >= 4 and cw < w * .2 and ch < h * .15:
            keep[i] = 1
            if 4 <= ch <= h * .04 and .15 <= cw / ch <= 2 and area >= 8:
                heights.append(ch)
    ink = keep[labels].astype(bool)
    glyph = float(np.median(heights)) if len(heights) >= 20 else None
    unit = glyph or max(6, h / 100)
    regions = []

    def partition(box, depth=0):
        x0, y0, x1, y1 = box
        crop = ink[y0:y1, x0:x1]
        bh, bw = crop.shape
        if depth >= 10 or bw < unit * 8 or bh < unit * 12:
            regions.append(box)
            return
        broad_gaps = [(a, b) for a, b in _runs(crop.mean(axis=1) < .006)
                      if b - a >= unit * 1.5 and a > unit * 2 and bh - b > unit * 2]
        if broad_gaps:
            a, b = max(broad_gaps, key=lambda ab: ab[1] - ab[0])
            cut = y0 + (a + b) // 2
            partition((x0, y0, x1, cut), depth + 1)
            partition((x0, cut, x1, y1), depth + 1)
            return
        # A gutter must be nearly empty throughout this region. Width guard
        # rejects spaces between words; edge guard prevents margin strips.
        vertical = crop.mean(axis=0) < .006
        candidates = [(a, b) for a, b in _runs(vertical)
                      if b - a >= max(4, unit * .65) and a > max(bw * .16, unit * 8) and b < min(bw * .84, bw - unit * 8)]
        if candidates:
            a, b = max(candidates, key=lambda ab: ab[1] - ab[0])
            cut = x0 + (a + b) // 2
            partition((x0, y0, cut, y1), depth + 1)
            partition((cut, y0, x1, y1), depth + 1)
            return
        horizontal = crop.mean(axis=1) < .006
        candidates = [(a, b) for a, b in _runs(horizontal)
                      if b - a >= unit * .85 and a > unit * 2 and bh - b > unit * 2]
        if candidates:
            a, b = max(candidates, key=lambda ab: ab[1] - ab[0])
            cut = y0 + (a + b) // 2
            partition((x0, y0, x1, cut), depth + 1)
            partition((x0, cut, x1, y1), depth + 1)
        else:
            regions.append(box)

    partition((0, 0, w, h))
    # Join adjacent stacked regions with identical horizontal boundaries. This
    # keeps mastheads together but never joins a spanning header to a column.
    merged = []
    for box in regions:
        if merged and merged[-1][0] == box[0] and merged[-1][2] == box[2] and merged[-1][3] == box[1]:
            prev = merged.pop()
            merged.append((prev[0], prev[1], box[2], box[3]))
        else:
            merged.append(box)
    # Map shared boundaries with the same rounding; preserve every source pixel.
    mapped = [(round(x0 * page.width / w), round(y0 * page.height / h),
               round(x1 * page.width / w), round(y1 * page.height / h)) for x0, y0, x1, y1 in merged]
    return mapped, glyph / scale if glyph else None


def layout_boxes(page: Image.Image, *, target_glyph_px=22, max_tiles=48):
    from src.vision.tiles import MAX_LONG_EDGE, MAX_PIXELS, tile_boxes

    regions, glyph = detect_regions(page)
    # Without reliable glyphs preserve more detail, rather than guessing that
    # a whole dense page will remain readable after reduction.
    scale = min(1.0, target_glyph_px / glyph) if glyph else .5
    boxes = []
    for region in regions:
        x0, y0, x1, y1 = region
        width, height = x1 - x0, y1 - y0
        cols = max(1, math.ceil(width * scale / MAX_LONG_EDGE))
        cell_w = width / cols
        max_h = min(MAX_LONG_EDGE / scale, MAX_PIXELS / (cell_w * scale * scale))
        rows = max(1, math.ceil(height / max_h))
        for col, row, (a, b, c, d) in tile_boxes(width, height, cols, rows, .04):
            boxes.append((len(boxes), 0, (x0 + a, y0 + b, x0 + c, y0 + d)))
    if len(boxes) > max_tiles:
        raise ValueError(f"Layout requer {len(boxes)} recortes (limite {max_tiles}); use --tiling grid ou revise a imagem")
    return boxes, {"regions": regions, "glyph_height_estimate": glyph, "target_glyph_px": target_glyph_px}
