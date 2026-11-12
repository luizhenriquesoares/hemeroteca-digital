"""Leitura de página via DeepSeek (API compatível com OpenAI).

Dois backends, mesmo contrato (`PageExtraction`) do leitor Claude, para que o
benchmark e a avaliação CER/WER comparem igual com igual:

- ``deepseek``: visão. Os recortes vão como imagens para o ``deepseek-flash``.
  Cada imagem custa no máximo 1024 tokens e é reduzida para ~1300x1300 px, então
  a grade precisa de mais recortes que a do Claude para manter os tipos legíveis.
- ``deepseek-text``: híbrido. O texto bruto do Tesseract (``data/text/<bib>/<id>.txt``)
  entra como texto; o modelo corrige e estrutura sem ver a imagem.

Credencial: ``DEEPSEEK_API_KEY`` no ambiente. Preços em US$/1M tokens (out/2026,
horário de pico); fora do pico custa metade e entra na coluna "batch" do benchmark.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from src.vision.schema import PageExtraction
from src.vision.tiles import Tile, describe_tiles, make_tiles, tile_label

logger = logging.getLogger(__name__)

DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_TOKENS_PER_IMAGE = 1024  # teto documentado por imagem

# US$ por 1M tokens, pico. Fora do pico (fora de 01-04h e 06-10h UTC, seg-sex): metade.
DEEPSEEK_PRICING: dict[str, dict[str, float]] = {
    "deepseek-flash": {"input": 0.30, "input_cached": 0.006, "output": 1.20},
    "deepseek-v4-pro": {"input": 1.32, "input_cached": 0.044, "output": 3.96},
}
OFF_PEAK_MULT = 0.5

TEXT_MODE_PREAMBLE = (
    "Você NÃO recebe a imagem: recebe o texto bruto de um OCR (Tesseract) da página inteira. "
    "O OCR mistura colunas, quebra palavras e troca letras. Reconstitua os segmentos, corrija apenas "
    "erros claros de OCR preservando a grafia da época, e marque [?] onde a leitura for incerta. "
    "Não invente texto que não esteja no OCR."
)


def get_deepseek_client(timeout: float = 900.0, max_retries: int = 2):
    import openai

    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("Defina DEEPSEEK_API_KEY no ambiente para usar os backends DeepSeek.")
    return openai.OpenAI(api_key=key, base_url=DEEPSEEK_BASE_URL, timeout=timeout, max_retries=max_retries)


def _schema_text() -> str:
    return json.dumps(PageExtraction.model_json_schema(), ensure_ascii=False)


BAND_NOTE = (
    "ATENÇÃO: estes recortes são só a FAIXA HORIZONTAL {k} de {n} da página (de cima para baixo). "
    "Transcreva TODO o texto visível nesta faixa, coluna por coluna, da esquerda para a direita, sem pular "
    "nenhum item nem resumir. Itens cortados no topo ou na base da faixa: transcreva apenas o que aparece, "
    "sem completar. Não modernize a grafia: copie letra por letra o que está impresso."
)


def build_messages(config, *, tiles: list[Tile] | None = None, ocr_text: str | None = None,
                   band: tuple[int, int] | None = None) -> list[dict[str, Any]]:
    from src.vision.reader import MODE_INSTRUCTIONS, SYSTEM_PROMPT

    system = (
        SYSTEM_PROMPT
        + "\n\nSaída: um único objeto JSON válido (json), sem markdown, que siga este JSON Schema:\n"
        + _schema_text()
    )
    if config.backend == "deepseek-text":
        system = system + "\n\n" + TEXT_MODE_PREAMBLE
        user: Any = (
            "Texto OCR bruto da página:\n<<<\n" + (ocr_text or "") + "\n>>>\n\n" + MODE_INSTRUCTIONS[config.mode]
        )
    else:
        intro = (f"{len(tiles)} recortes desta faixa, numerados da esquerda para a direita e de cima para baixo."
                 if band else describe_tiles(tiles))
        user = [{"type": "text", "text": intro}]
        if band:
            user.append({"type": "text", "text": BAND_NOTE.format(k=band[0], n=band[1])})
        for tile in tiles:
            user.append({"type": "text", "text": tile_label(tile)})
            user.append({
                "type": "image_url",
                "image_url": {"url": "data:image/jpeg;base64," + tile.b64(), "detail": "high"},
            })
        user.append({"type": "text", "text": MODE_INSTRUCTIONS[config.mode] + " Responda em json."})
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def usage_to_dict(usage: Any) -> dict[str, int]:
    if usage is None:
        return {}
    data = usage.model_dump() if hasattr(usage, "model_dump") else dict(usage)
    prompt = int(data.get("prompt_tokens") or 0)
    hit = int(data.get("prompt_cache_hit_tokens") or 0)
    miss = int(data.get("prompt_cache_miss_tokens") or (prompt - hit))
    details = data.get("completion_tokens_details") or {}
    return {
        "input_tokens": miss,  # cobrado a preço cheio
        "cache_read_input_tokens": hit,
        "output_tokens": int(data.get("completion_tokens") or 0),
        "thinking_tokens": int((details or {}).get("reasoning_tokens") or 0),
        "prompt_tokens_total": prompt,
    }


def estimate_cost(usage: dict[str, int], model: str, *, off_peak: bool = False) -> float:
    price = DEEPSEEK_PRICING.get(model)
    if not price:
        return 0.0
    cost = (
        usage.get("input_tokens", 0) * price["input"]
        + usage.get("cache_read_input_tokens", 0) * price["input_cached"]
        + usage.get("output_tokens", 0) * price["output"]
    ) / 1_000_000
    return cost * OFF_PEAK_MULT if off_peak else cost


def _load_ocr_text(image_path: Path) -> str | None:
    from src.config import TEXT_DIR

    page_id = image_path.stem
    bib = image_path.parent.name
    path = TEXT_DIR / bib / f"{page_id}.txt"
    return path.read_text(encoding="utf-8", errors="ignore") if path.exists() else None


def _parse_json(content: str) -> dict[str, Any]:
    content = (content or "").strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", content, re.S)
    if fenced:
        content = fenced.group(1)
    return json.loads(content)


def read_page_deepseek(image_path: Path, config, *, client=None, tiles: list[Tile] | None = None):
    from src.vision.reader import _base_result

    image_path = Path(image_path)
    text_mode = config.backend == "deepseek-text"
    if text_mode:
        tiles = []
    elif tiles is None:
        extra = {"overlap": config.overlap} if config.overlap is not None else {}
        tiles = make_tiles(image_path, grid=config.grid, strategy=config.tiling, **extra)
    result = _base_result(image_path.stem, config, tiles)
    result.backend = config.backend
    result.tiles_tokens_estimate = 0 if text_mode else len(tiles) * DEEPSEEK_TOKENS_PER_IMAGE

    ocr_text = None
    if text_mode:
        ocr_text = _load_ocr_text(image_path)
        if not ocr_text:
            result.error = "sem texto Tesseract (.txt) local para o modo híbrido"
            return result
        result.details["ocr_chars"] = len(ocr_text)

    try:
        client = client or get_deepseek_client()
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"
        return result

    if not text_mode and config.bands > 1:
        rows = sorted({t.row for t in tiles})
        per = -(-len(rows) // config.bands)
        groups = [[t for t in tiles if t.row in rows[i:i + per]] for i in range(0, len(rows), per)]
        parts = [(g, (i + 1, len(groups))) for i, g in enumerate(groups)]
    else:
        parts = [(tiles, None)]

    started = time.perf_counter()
    total: dict[str, int] = {}
    segments: list[dict[str, Any]] = []
    header: dict[str, Any] = {}
    calls = []
    for part_tiles, band in parts:
        payload, usage, info = _call_with_retry(client, config, part_tiles, ocr_text, band)
        for key, value in usage.items():
            total[key] = total.get(key, 0) + value
        calls.append(info)
        if payload is None:
            result.error = info.get("error")
            continue
        try:
            page = PageExtraction.model_validate(payload).model_dump(mode="json")
        except ValidationError as exc:
            page = {"segments": [
                {"type": "outro", "text": str(s.get("text", "")), "people": [], "relations": []}
                for s in (payload.get("segments") or []) if isinstance(s, dict)
            ]}
            info["schema_errors"] = str(exc)[:400]
        for key in ("newspaper", "issue_date", "issue_number", "page_number", "columns"):
            if page.get(key) and not header.get(key):
                header[key] = page[key]
        segments.extend(page.get("segments") or [])

    result.elapsed_s = round(time.perf_counter() - started, 2)
    result.usage = total
    result.cost_usd = round(estimate_cost(total, config.model), 6)
    result.cost_usd_batch = round(estimate_cost(total, config.model, off_peak=True), 6)
    result.stop_reason = ",".join(str(c.get("finish_reason")) for c in calls)
    result.details["calls"] = calls
    # Custo realista em produção: páginas únicas não reaproveitam cache de imagem.
    price = DEEPSEEK_PRICING.get(config.model, {"input": 0, "output": 0})
    result.details["cost_usd_no_cache"] = round(
        (total.get("prompt_tokens_total", 0) * price["input"] + total.get("output_tokens", 0) * price["output"]) / 1e6, 6)
    if segments:
        result.error = None if all(not c.get("error") for c in calls) else result.error
        result.extraction = {**header, "segments": segments}
        if result.error:
            result.details["partial_error"] = result.error
            result.error = None
    return result


def _call_with_retry(client, config, tiles, ocr_text, band, attempts: int = 2):
    kwargs: dict[str, Any] = {
        "model": config.model,
        "messages": build_messages(config, tiles=tiles, ocr_text=ocr_text, band=band),
        "max_tokens": config.max_tokens,
        "response_format": {"type": "json_object"},
    }
    if config.effort:
        kwargs["reasoning_effort"] = config.effort
        kwargs["extra_body"] = {"thinking": {"type": "enabled"}}
    else:
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
    usage: dict[str, int] = {}
    info: dict[str, Any] = {"band": band}
    for attempt in range(attempts):
        try:
            response = client.chat.completions.create(**kwargs)
        except Exception as exc:  # noqa: BLE001
            info["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            continue
        for key, value in usage_to_dict(response.usage).items():
            usage[key] = usage.get(key, 0) + value
        choice = response.choices[0]
        content = choice.message.content or ""
        info.update(finish_reason=choice.finish_reason, served_model=getattr(response, "model", None),
                    response_chars=len(content), attempts=attempt + 1)
        try:
            payload = _parse_json(content)
            info.pop("error", None)
            return payload, usage, info
        except json.JSONDecodeError as exc:
            info["error"] = f"JSON inválido (finish_reason={choice.finish_reason}): {exc}"
            info["response_tail"] = content[-300:]
    return None, usage, info
