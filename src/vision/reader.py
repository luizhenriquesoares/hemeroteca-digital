"""Leitura de uma página de jornal via Claude com saída estruturada.

Uma chamada por página: os tiles entram como imagens, o modelo devolve um
`PageExtraction` (transcrição por segmento + estrutura). Duas variantes:

- ``full``: transcrição integral de cada segmento (custo dominado pela saída);
- ``structure``: só estrutura e âncora textual curta (saída barata; o texto
  integral vem de um OCR barato à parte).
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.vision.schema import PageExtraction
from src.vision.tiles import DEFAULT_GRID, Tile, make_tiles, page_bytes, page_tokens_estimate, describe_tiles, tile_label, tiles_manifest

load_dotenv()
logger = logging.getLogger(__name__)

# US$ por milhão de tokens (API Anthropic, 2026-06). Cache: escrita 1,25x, leitura 0,1x.
PRICING_USD_PER_MTOK: dict[str, dict[str, float]] = {
    "claude-opus-5": {"input": 5.0, "output": 25.0},
    "claude-opus-5-5": {"input": 4.0, "output": 20.0},
    "claude-sonnet-5-5": {"input": 2.0, "output": 10.0},
    "claude-haiku-5-5": {"input": 0.10, "output": 0.50},
    "claude-sonnet-5": {"input": 2.0, "output": 10.0},
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0},
}
CACHE_WRITE_MULT = 1.25
CACHE_READ_MULT = 0.10
BATCH_MULT = 0.5

MODES = ("full", "structure")


BACKENDS = ("api", "claude-code", "deepseek", "deepseek-text")
DEEPSEEK_BACKENDS = ("deepseek", "deepseek-text")


@dataclass(frozen=True)
class ReadConfig:
    name: str
    model: str
    mode: str = "full"
    grid: tuple[int, int] = DEFAULT_GRID
    effort: str | None = "medium"  # None para modelos sem `output_config.effort` (Haiku)
    max_tokens: int = 24000
    backend: str = "api"  # "api" (SDK, créditos) ou "claude-code" (CLI headless, assinatura)

    tiling: str = "grid"
    overlap: float | None = None  # sobreposição dos recortes; None = padrão de tiles.py (3%)
    bands: int = 1  # DeepSeek: >1 lê a página em faixas horizontais, uma chamada por faixa

    def with_grid(self, grid: tuple[int, int]) -> "ReadConfig":
        return replace(self, grid=grid)

    def with_backend(self, backend: str) -> "ReadConfig":
        if backend not in BACKENDS:
            raise ValueError(f"Backend inválido: {backend!r} (use um de {BACKENDS})")
        return replace(self, backend=backend)


CONFIGS: dict[str, ReadConfig] = {
    "opus-full": ReadConfig("opus-full", "claude-opus-5", "full", effort="low"),
    "opus-structure": ReadConfig("opus-structure", "claude-opus-5", "structure", effort="medium", max_tokens=12000),
    "sonnet-full": ReadConfig("sonnet-full", "claude-sonnet-5", "full", effort="low"),
    "sonnet-structure": ReadConfig("sonnet-structure", "claude-sonnet-5", "structure", effort="medium", max_tokens=12000),
    "haiku-full": ReadConfig("haiku-full", "claude-haiku-4-5", "full", effort=None),
    "haiku-structure": ReadConfig("haiku-structure", "claude-haiku-4-5", "structure", effort=None, max_tokens=12000),
    "sonnet55-full": ReadConfig("sonnet55-full", "claude-sonnet-5-5", "full", effort="low", max_tokens=64000),
    "haiku55-full": ReadConfig("haiku55-full", "claude-haiku-5-5", "full", effort="low", max_tokens=64000),
    "opus55-full": ReadConfig("opus55-full", "claude-opus-5-5", "full", effort="low", max_tokens=64000),
    # DeepSeek (rodada de 2026-10): imagem limitada a 1024 tokens/~1300 px, por isso grade mais fina.
    "dsflash-full": ReadConfig("dsflash-full", "deepseek-flash", "full", grid=(4, 6), effort=None, max_tokens=32000, backend="deepseek"),
    "dsflash-full-think": ReadConfig("dsflash-full-think", "deepseek-flash", "full", grid=(4, 6), effort="low", max_tokens=48000, backend="deepseek"),
    "dsflash-bands3": ReadConfig("dsflash-bands3", "deepseek-flash", "full", grid=(4, 6), effort=None, max_tokens=32000, backend="deepseek", bands=3),
    "dsflash-bands6": ReadConfig("dsflash-bands6", "deepseek-flash", "full", grid=(4, 6), effort=None, max_tokens=32000, backend="deepseek", bands=6),
    "dsflash-bands6-ov": ReadConfig("dsflash-bands6-ov", "deepseek-flash", "full", grid=(4, 6), effort=None, max_tokens=32000, backend="deepseek", bands=6, overlap=0.12),
    "dsflash-ocrtext": ReadConfig("dsflash-ocrtext", "deepseek-flash", "full", effort=None, max_tokens=32000, backend="deepseek-text"),
    "dspro-ocrtext": ReadConfig("dspro-ocrtext", "deepseek-v4-pro", "full", effort=None, max_tokens=32000, backend="deepseek-text"),
}

SYSTEM_PROMPT = """Você é historiador e paleógrafo especialista em jornais brasileiros do século XIX (Pernambuco, 1825-1889).

Você recebe UMA página de jornal dividida em recortes (tiles) numerados, acompanhados de sua posição na página e, quando disponível, de uma visão geral para orientação. Recortes vizinhos se sobrepõem levemente: um mesmo trecho pode aparecer no fim de um recorte e no início do seguinte. Nunca duplique esse trecho.

Sua tarefa é reconstituir a página como um leitor da época a leria:
1. Identifique o layout (número de colunas do jornal) e o cabeçalho (título, data, número da edição, página).
2. Siga a ordem de leitura: cada coluna do jornal de cima a baixo, depois a coluna seguinte. Um item que continua na coluna ou no recorte seguinte é UM só segmento.
3. Divida o conteúdo em segmentos (itens editoriais): editais, atos do governo, nomeações, sentenças judiciais, anúncios de fuga ou venda de escravizados, movimento do porto (entradas e saídas de embarcações), óbitos, leilões, anúncios comerciais, correspondências, notícias.
4. Para cada segmento, extraia pessoas (nome completo como impresso, cargo ou condição, papel no item), instituições, lugares, embarcações, data mencionada e relações explícitas com trecho literal.

Regras de transcrição:
- Preserve exatamente a grafia impressa. Não modernize nem arcaíze: mantenha um se estiver impresso um, e hum apenas se estiver impresso hum. Exemplos de grafia não são instruções de substituição.
- Preserve maiúsculas, abreviaturas e pontuação originais. Desfaça apenas a hifenização de fim de linha.
- Trecho ilegível: escreva [ilegível]. Leitura incerta: melhor leitura seguida de [?].
- Não invente nomes, datas ou palavras. Se não está na imagem, não está na saída.
- Relações só quando explícitas na fonte (viúva de, filho de, escravo de, réu, nomeado para, morador em, passageiro do brigue X).

Responda somente com o JSON do schema fornecido."""

MODE_INSTRUCTIONS = {
    "full": (
        "Modo TRANSCRIÇÃO COMPLETA: o campo `text` de cada segmento deve conter a "
        "transcrição integral do segmento, sem resumir nem omitir linhas."
    ),
    "structure": (
        "Modo ESTRUTURA: o campo `text` de cada segmento deve conter apenas as primeiras "
        "linhas (até ~300 caracteres) como âncora. Concentre-se em identificar TODOS os "
        "segmentos, pessoas, cargos, lugares, embarcações e relações da página."
    ),
}


@dataclass
class ReadResult:
    config: str
    model: str
    mode: str
    page: str
    grid: tuple[int, int]
    tiles: int
    tiles_tokens_estimate: int
    tiles_bytes: int
    usage: dict[str, int] = field(default_factory=dict)
    cost_usd: float = 0.0
    cost_usd_batch: float = 0.0
    elapsed_s: float = 0.0
    stop_reason: str | None = None
    extraction: dict[str, Any] | None = None
    error: str | None = None
    backend: str = "api"
    cost_usd_reported: float | None = None  # custo informado pelo Claude Code (inclui overhead do harness)
    usage_cli: dict[str, int] = field(default_factory=dict)  # uso bruto do Claude Code, quando backend=claude-code
    tiling: str = "grid"
    tile_manifest: list[dict[str, Any]] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)  # diagnóstico do run (turnos, subtype, trecho da resposta)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["grid"] = list(self.grid)
        return payload


OAUTH_BETA_HEADER = "oauth-2025-04-20"


def _ant_access_token() -> str | None:
    """Token do perfil criado por `ant auth login` (CLI da Anthropic), se houver."""
    import shutil
    import subprocess

    if not shutil.which("ant"):
        return None
    try:
        proc = subprocess.run(
            ["ant", "auth", "print-credentials", "--access-token"],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    token = proc.stdout.strip()
    return token if proc.returncode == 0 and token.startswith("sk-ant-oat") else None


def get_client(timeout: float = 900.0, max_retries: int = 2):
    """Cliente Anthropic. Timeout explícito libera max_tokens grande sem streaming.

    Credencial: ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN do ambiente (o SDK resolve);
    sem elas, cai no perfil do `ant auth login` (token OAuth + cabeçalho beta).
    """
    import os

    import anthropic

    if os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN"):
        return anthropic.Anthropic(timeout=timeout, max_retries=max_retries)

    token = _ant_access_token()
    if token:
        return anthropic.Anthropic(
            auth_token=token,
            default_headers={"anthropic-beta": OAUTH_BETA_HEADER},
            timeout=timeout,
            max_retries=max_retries,
        )
    raise RuntimeError(
        "Sem credencial Anthropic: defina ANTHROPIC_API_KEY em .env ou faça `ant auth login`."
    )


def build_user_content(tiles: list[Tile], config: ReadConfig) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = [{"type": "text", "text": describe_tiles(tiles)}]
    for tile in tiles:
        content.append(
            {
                "type": "text",
                "text": tile_label(tile),
            }
        )
        content.append(
            {
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": tile.b64()},
            }
        )
    content.append({"type": "text", "text": MODE_INSTRUCTIONS[config.mode]})
    return content


def build_request(tiles: list[Tile], config: ReadConfig) -> dict[str, Any]:
    if config.mode not in MODES:
        raise ValueError(f"Modo inválido: {config.mode!r} (use um de {MODES})")
    request: dict[str, Any] = {
        "model": config.model,
        "max_tokens": config.max_tokens,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": build_user_content(tiles, config)}],
    }
    if config.effort:
        request["output_config"] = {"effort": config.effort}
    return request


def usage_to_dict(usage: Any) -> dict[str, int]:
    keys = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    return {key: int(getattr(usage, key, 0) or 0) for key in keys}


def estimate_cost(usage: dict[str, int], model: str, *, batch: bool = False) -> float:
    price = PRICING_USD_PER_MTOK.get(model)
    if not price:
        return 0.0
    cost = (
        usage.get("input_tokens", 0) * price["input"]
        + usage.get("cache_creation_input_tokens", 0) * price["input"] * CACHE_WRITE_MULT
        + usage.get("cache_read_input_tokens", 0) * price["input"] * CACHE_READ_MULT
        + usage.get("output_tokens", 0) * price["output"]
    ) / 1_000_000
    return cost * BATCH_MULT if batch else cost


def count_input_tokens(client, request: dict[str, Any]) -> int:
    """Contagem oficial (gratuita) dos tokens de entrada da requisição."""
    response = client.messages.count_tokens(
        model=request["model"],
        system=request["system"],
        messages=request["messages"],
    )
    return int(response.input_tokens)


def _base_result(page: str, config: ReadConfig, tiles: list[Tile]) -> ReadResult:
    return ReadResult(
        config=config.name,
        model=config.model,
        mode=config.mode,
        page=page,
        grid=config.grid,
        tiles=len(tiles),
        tiles_tokens_estimate=page_tokens_estimate(tiles),
        tiles_bytes=page_bytes(tiles),
        tiling=config.tiling,
        tile_manifest=tiles_manifest(tiles),
    )


def read_page(
    image_path: Path | str,
    config: ReadConfig,
    *,
    client=None,
    tiles: list[Tile] | None = None,
) -> ReadResult:
    """Lê uma página com a configuração dada e devolve resultado + custo."""
    image_path = Path(image_path)
    if config.backend in DEEPSEEK_BACKENDS:
        from src.vision.deepseek_reader import read_page_deepseek

        return read_page_deepseek(image_path, config, client=client, tiles=tiles)
    if config.backend == "claude-code":
        from src.vision.claude_code_reader import read_page_claude_code

        return read_page_claude_code(image_path, config, tiles=tiles)

    client = client or get_client()
    tiles = tiles if tiles is not None else make_tiles(image_path, grid=config.grid, strategy=config.tiling)
    result = _base_result(image_path.stem, config, tiles)
    request = build_request(tiles, config)

    started = time.perf_counter()
    try:
        response = client.messages.parse(output_format=PageExtraction, **request)
    except Exception as exc:  # noqa: BLE001 - benchmark registra a falha e segue
        result.elapsed_s = round(time.perf_counter() - started, 2)
        result.error = f"{type(exc).__name__}: {exc}"
        logger.error("Leitura falhou (%s, %s): %s", config.name, image_path.name, result.error)
        return result

    result.elapsed_s = round(time.perf_counter() - started, 2)
    result.usage = usage_to_dict(response.usage)
    result.cost_usd = round(estimate_cost(result.usage, config.model), 6)
    result.cost_usd_batch = round(estimate_cost(result.usage, config.model, batch=True), 6)
    result.stop_reason = getattr(response, "stop_reason", None)

    parsed = getattr(response, "parsed_output", None)
    if parsed is None:
        result.error = f"sem JSON parseável (stop_reason={result.stop_reason})"
    else:
        result.extraction = parsed.model_dump(mode="json")
    return result
