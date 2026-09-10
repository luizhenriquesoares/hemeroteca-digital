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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.vision.schema import PageExtraction
from src.vision.tiles import DEFAULT_GRID, Tile, make_tiles, page_bytes, page_tokens_estimate

load_dotenv()
logger = logging.getLogger(__name__)

# US$ por milhão de tokens (API Anthropic, 2026-06). Cache: escrita 1,25x, leitura 0,1x.
PRICING_USD_PER_MTOK: dict[str, dict[str, float]] = {
    "claude-opus-5": {"input": 5.0, "output": 25.0},
    "claude-sonnet-5": {"input": 2.0, "output": 10.0},
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0},
}
CACHE_WRITE_MULT = 1.25
CACHE_READ_MULT = 0.10
BATCH_MULT = 0.5

MODES = ("full", "structure")


@dataclass(frozen=True)
class ReadConfig:
    name: str
    model: str
    mode: str = "full"
    grid: tuple[int, int] = DEFAULT_GRID
    effort: str | None = "medium"  # None para modelos sem `output_config.effort` (Haiku)
    max_tokens: int = 24000

    def with_grid(self, grid: tuple[int, int]) -> "ReadConfig":
        return ReadConfig(self.name, self.model, self.mode, grid, self.effort, self.max_tokens)


CONFIGS: dict[str, ReadConfig] = {
    "opus-full": ReadConfig("opus-full", "claude-opus-5", "full", effort="low"),
    "opus-structure": ReadConfig("opus-structure", "claude-opus-5", "structure", effort="medium", max_tokens=12000),
    "sonnet-full": ReadConfig("sonnet-full", "claude-sonnet-5", "full", effort="low"),
    "sonnet-structure": ReadConfig("sonnet-structure", "claude-sonnet-5", "structure", effort="medium", max_tokens=12000),
    "haiku-full": ReadConfig("haiku-full", "claude-haiku-4-5", "full", effort=None),
    "haiku-structure": ReadConfig("haiku-structure", "claude-haiku-4-5", "structure", effort=None, max_tokens=12000),
}

SYSTEM_PROMPT = """Você é historiador e paleógrafo especialista em jornais brasileiros do século XIX (Pernambuco, 1825-1889).

Você recebe UMA página de jornal dividida em recortes (tiles) numerados, dispostos em uma grade de colunas por faixas, apresentados da esquerda para a direita e de cima para baixo. Recortes vizinhos se sobrepõem levemente: um mesmo trecho pode aparecer no fim de um recorte e no início do seguinte. Nunca duplique esse trecho.

Sua tarefa é reconstituir a página como um leitor da época a leria:
1. Identifique o layout (número de colunas do jornal) e o cabeçalho (título, data, número da edição, página).
2. Siga a ordem de leitura: cada coluna do jornal de cima a baixo, depois a coluna seguinte. Um item que continua na coluna ou no recorte seguinte é UM só segmento.
3. Divida o conteúdo em segmentos (itens editoriais): editais, atos do governo, nomeações, sentenças judiciais, anúncios de fuga ou venda de escravizados, movimento do porto (entradas e saídas de embarcações), óbitos, leilões, anúncios comerciais, correspondências, notícias.
4. Para cada segmento, extraia pessoas (nome completo como impresso, cargo ou condição, papel no item), instituições, lugares, embarcações, data mencionada e relações explícitas com trecho literal.

Regras de transcrição:
- Preserve a ortografia da época (Commandante, Escripturario, Affonso, hum, mesmo). Não modernize.
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

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["grid"] = list(self.grid)
        return payload


def get_client(timeout: float = 900.0, max_retries: int = 2):
    """Cliente Anthropic. Timeout explícito libera max_tokens grande sem streaming."""
    import anthropic

    return anthropic.Anthropic(timeout=timeout, max_retries=max_retries)


def build_user_content(tiles: list[Tile], config: ReadConfig) -> list[dict[str, Any]]:
    # A grade descrita ao modelo vem dos tiles reais, não da config (podem divergir).
    cols = max(tile.col for tile in tiles) + 1
    rows = max(tile.row for tile in tiles) + 1
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Página dividida em {len(tiles)} recortes: grade de {cols} colunas por {rows} faixas, "
                f"numerados de 0 a {len(tiles) - 1}, da esquerda para a direita e de cima para baixo."
            ),
        }
    ]
    for tile in tiles:
        content.append(
            {
                "type": "text",
                "text": f"Recorte {tile.index} (coluna {tile.col + 1}/{cols}, faixa {tile.row + 1}/{rows}):",
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
    client = client or get_client()
    tiles = tiles if tiles is not None else make_tiles(image_path, grid=config.grid)
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
