"""Leitura de página pelo Claude Code em modo headless (``claude -p``).

Usa a sessão/assinatura do Claude Code em vez de créditos da API: os tiles são
gravados em disco, o prompt pede que o modelo os leia com a ferramenta Read e
devolva o JSON validado pelo schema (``--json-schema``). A saída ``--output-format
json`` traz ``structured_output`` e ``usage``.

O custo informado pelo Claude Code inclui o overhead do harness (system prompt
e ferramentas, ~35k tokens em cache). Para extrapolar o custo de produção via
API, o resultado também traz uma estimativa "API equivalente": tokens dos tiles
pela fórmula de pixels + prompt, mais os tokens de saída reais.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import tempfile
from pathlib import Path
from typing import Any

from src.config import DATA_DIR
from src.vision.reader import (
    MODE_INSTRUCTIONS,
    MODES,
    SYSTEM_PROMPT,
    ReadConfig,
    ReadResult,
    _base_result,
    estimate_cost,
)
from src.vision.schema import PageExtraction
from src.vision.tiles import Tile, make_tiles, describe_tiles, tile_label

logger = logging.getLogger(__name__)

CLAUDE_CODE_MODEL_ALIAS = {
    "claude-opus-5": "opus",
    "claude-sonnet-5": "sonnet",
    "claude-haiku-4-5": "haiku",
}
PROMPT_TOKENS_ESTIMATE = 1_500  # system prompt + instruções + listagem dos tiles
DEFAULT_TIMEOUT_S = 1_500
TILES_DIR = DATA_DIR / "benchmarks" / "visao" / "_tiles"


def write_tiles(tiles: list[Tile], page_id: str, work_dir: Path | None = None) -> list[Path]:
    """Grava os tiles como JPEG para o Claude Code ler pelo caminho."""
    out_dir = (work_dir or TILES_DIR) / page_id
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for tile in tiles:
        path = out_dir / f"tile_{tile.index:02d}.jpg"
        path.write_bytes(tile.jpeg)
        paths.append(path)
    return paths


def build_prompt(tiles: list[Tile], tile_paths: list[Path], config: ReadConfig) -> str:
    if config.mode not in MODES:
        raise ValueError(f"Modo inválido: {config.mode!r} (use um de {MODES})")
    listing = "\n".join(f"- {tile_label(tile)}: {path}" for tile, path in zip(tiles, tile_paths))
    return (
        f"{SYSTEM_PROMPT}\n\n"
        f"{describe_tiles(tiles)}\n"
        "Leia TODOS os arquivos abaixo com a ferramenta Read, na ordem, antes de responder:\n"
        f"{listing}\n\n"
        f"{MODE_INSTRUCTIONS[config.mode]}\n\n"
        "Não escreva nada além do JSON final."
    )


def inline_schema_refs(schema: dict[str, Any]) -> dict[str, Any]:
    """Substitui ``$ref`` por suas definições: o ``--json-schema`` do Claude Code com ``$defs``
    levou o modelo a devolver os nomes das definições como chaves em vez do documento."""
    defs = schema.get("$defs") or schema.get("definitions") or {}

    def resolve(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/"):
                name = ref.split("/")[-1]
                target = defs.get(name)
                if target is None:
                    raise ValueError(f"$ref não resolvido no schema: {ref}")
                merged = {key: value for key, value in node.items() if key != "$ref"}
                merged.update(resolve(target))
                return merged
            return {key: resolve(value) for key, value in node.items() if key not in ("$defs", "definitions")}
        if isinstance(node, list):
            return [resolve(item) for item in node]
        return node

    return resolve(schema)


def unwrap_structured(structured: Any) -> Any:
    """Desfaz embrulhos vistos na prática: ``{"response": "<json>"}`` ou string JSON."""
    if isinstance(structured, str):
        try:
            return unwrap_structured(json.loads(structured))
        except json.JSONDecodeError:
            return None
    if isinstance(structured, dict) and "segments" not in structured and len(structured) == 1:
        (inner,) = structured.values()
        return unwrap_structured(inner)
    return structured


def build_command(prompt: str, config: ReadConfig, schema: dict[str, Any], max_turns: int = 60) -> list[str]:
    alias = CLAUDE_CODE_MODEL_ALIAS.get(config.model, config.model)
    schema = inline_schema_refs(schema)
    cmd = [
        "claude",
        "-p",
        prompt,
        "--model",
        alias,
        "--output-format",
        "json",
        "--allowedTools",
        "Read",
        "--max-turns",
        str(max_turns),
        "--json-schema",
        json.dumps(schema, ensure_ascii=False),
    ]
    if config.effort:
        # Sem isso o Sonnet gastou ~60k tokens de raciocínio numa página (11 min).
        cmd += ["--effort", config.effort]
    return cmd


def run_claude_code(cmd: list[str], timeout: int = DEFAULT_TIMEOUT_S) -> subprocess.CompletedProcess:
    env = {key: value for key, value in os.environ.items() if key != "CLAUDECODE"}  # permite rodar aninhado
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env, check=False)


def _usage_from_payload(payload: dict[str, Any]) -> dict[str, int]:
    usage = payload.get("usage") or {}
    keys = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    parsed = {key: int(usage.get(key) or 0) for key in keys}
    details = usage.get("output_tokens_details") or {}
    parsed["thinking_tokens"] = int(details.get("thinking_tokens") or 0)
    return parsed


def _details_from_payload(payload: dict[str, Any], raw_limit: int = 6000) -> dict[str, Any]:
    """Diagnóstico do run do Claude Code (turnos, subtype, negações, trecho da resposta)."""
    raw = payload.get("result")
    return {
        "subtype": payload.get("subtype"),
        "num_turns": payload.get("num_turns"),
        "terminal_reason": payload.get("terminal_reason"),
        "permission_denials": payload.get("permission_denials") or [],
        "api_error_status": payload.get("api_error_status"),
        "session_id": payload.get("session_id"),
        "model_usage": payload.get("modelUsage") or {},
        "result_text": raw[:raw_limit] if isinstance(raw, str) else raw,
        "result_text_len": len(raw) if isinstance(raw, str) else None,
        "has_structured_output": payload.get("structured_output") is not None,
    }


def read_page_claude_code(
    image_path: Path | str,
    config: ReadConfig,
    *,
    tiles: list[Tile] | None = None,
    work_dir: Path | None = None,
    runner=None,
    timeout: int = DEFAULT_TIMEOUT_S,
) -> ReadResult:
    image_path = Path(image_path)
    runner = runner or run_claude_code  # resolvido na chamada, para testes trocarem o executor
    tiles = tiles if tiles is not None else make_tiles(image_path, grid=config.grid, strategy=config.tiling)
    result = _base_result(image_path.stem, config, tiles)
    result.backend = "claude-code"

    started = time.perf_counter()
    try:
        # Each call owns its tiles until Read finishes: concurrent grids/configs
        # must never overwrite the files of an active CLI session.
        root = work_dir or TILES_DIR
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="read-", dir=root) as run_dir:
            tile_paths = write_tiles(tiles, image_path.stem, Path(run_dir))
            prompt = build_prompt(tiles, tile_paths, config)
            cmd = build_command(prompt, config, PageExtraction.model_json_schema(), max_turns=max(60, 2 * len(tiles) + 8))
            proc = runner(cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        result.elapsed_s = round(time.perf_counter() - started, 2)
        result.error = f"timeout ({timeout}s)"
        return result
    except Exception as exc:  # noqa: BLE001
        result.elapsed_s = round(time.perf_counter() - started, 2)
        result.error = f"{type(exc).__name__}: {exc}"
        return result
    result.elapsed_s = round(time.perf_counter() - started, 2)

    stdout = (proc.stdout or "").strip()
    if not stdout:
        result.error = (proc.stderr or stdout or f"returncode={proc.returncode}").strip()[:500]
        logger.error("Claude Code falhou (%s, %s): %s", config.name, image_path.name, result.error)
        return result

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        result.error = (proc.stderr or stdout)[:500] if proc.returncode else f"saída não é JSON: {exc}"
        return result

    cli_usage = _usage_from_payload(payload)
    result.stop_reason = payload.get("subtype") or payload.get("stop_reason")
    result.cost_usd_reported = payload.get("total_cost_usd")
    result.details = _details_from_payload(payload)

    # Custo "API equivalente": tiles pela fórmula de pixels + prompt na entrada, saída real do CLI.
    # `usage` fica nesse formato (comparável ao backend api); o bruto do CLI vai em `usage_cli`.
    result.usage = {
        "input_tokens": result.tiles_tokens_estimate + PROMPT_TOKENS_ESTIMATE,
        "output_tokens": cli_usage.get("output_tokens", 0),
        "thinking_tokens": cli_usage.get("thinking_tokens", 0),
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
    }
    result.usage_cli = cli_usage
    result.cost_usd = round(estimate_cost(result.usage, config.model), 6)
    result.cost_usd_batch = round(estimate_cost(result.usage, config.model, batch=True), 6)

    if payload.get("is_error") or proc.returncode:
        # Nonzero exit can still contain valid diagnostic JSON (e.g. revoked
        # OAuth). Parse it before handling failure; never charge fictitious
        # image input when the request failed before consuming any tokens.
        if not any(cli_usage.get(key, 0) for key in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")):
            result.usage["input_tokens"] = 0
            result.cost_usd = result.cost_usd_batch = 0.0
        result.error = str(payload.get("result") or proc.stderr or payload.get("subtype") or "erro")[:500]
        return result

    candidates = [unwrap_structured(payload.get("structured_output")), unwrap_structured(payload.get("result"))]
    candidates = [candidate for candidate in candidates if isinstance(candidate, dict) and "segments" in candidate]
    if not candidates:
        result.error = f"sem JSON com 'segments' na resposta (subtype={payload.get('subtype')})"
        return result

    last_error = ""
    for candidate in candidates:
        try:
            result.extraction = PageExtraction.model_validate(candidate).model_dump(mode="json")
            result.error = None
            return result
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)[:300]
    result.error = f"JSON fora do schema: {last_error}"
    return result
