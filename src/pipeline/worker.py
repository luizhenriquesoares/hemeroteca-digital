"""Processador em segundo plano: baixa imagens hi-res à frente e lê páginas com o modelo.

Um fio de captura mantém ``prefetch`` imagens prontas (Selenium, um navegador por
vez para não sobrecarregar a Hemeroteca) e N fios de leitura consomem as páginas
prontas. Tudo respeita a pausa global e a pausa por trabalho, guardadas no banco,
então o painel controla o processo sem reiniciá-lo.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable

from src.config import DATA_DIR, IMAGES_DIR
from src.pipeline.store import Store

logger = logging.getLogger(__name__)

OUTPUT_DIR = DATA_DIR / "extractions" / "visao"
MAX_WORKERS = 8

# Erros que não são da página: pausar tudo e esperar o usuário.
FATAL_PATTERNS: list[tuple[re.Pattern, str]] = [
    # antes do padrão de login: sem o binário cada página gastaria as tentativas e terminaria em erro
    (re.compile(r"No such file or directory: '?claude'?", re.I),
     "O Claude Code não está instalado neste computador. Instale (curl -fsSL https://claude.ai/install.sh | bash), "
     "faça `claude login` e aperte Continuar."),
    (re.compile(r"login expired|/login|not logged in|authenticat|oauth", re.I),
     "O login do Claude expirou. Rode `claude login` no terminal e aperte Continuar."),
    (re.compile(r"usage limit|limit reached|hit your limit|weekly limit|5-hour limit|out of extra usage|rate.?limit", re.I),
     "Limite da assinatura atingido. Espere a cota renovar e aperte Continuar."),
    (re.compile(r"credit balance", re.I), "A conta da API está sem créditos."),
    (re.compile(r"DEEPSEEK_API_KEY", re.I),
     "Falta DEEPSEEK_API_KEY: defina no ambiente do painel (na VPS, em /etc/hemeroteca/painel.env) e reinicie."),
    (re.compile(r"ANTHROPIC_API_KEY|api_key client option must be set|Could not resolve authentication", re.I),
     "Falta ANTHROPIC_API_KEY para o backend da API: defina no ambiente do painel ou use a assinatura (claude-code)."),
]


def image_path(bib: str, page_id: str) -> Path:
    return IMAGES_DIR / bib / f"{page_id}.jpg"


def image_exists(bib: str, page_id: str) -> bool:
    return image_path(bib, page_id).exists()


def default_capturer(page_ids: list[str]) -> None:
    from src.scraping.hires_pipeline import processar_paginas_hires

    processar_paginas_hires(page_ids, headless=True, keep_images=True)


def default_reader(image: Path, config_name: str, backend: str):
    from src.vision.reader import CONFIGS, DEEPSEEK_BACKENDS, read_page

    config = CONFIGS[config_name]
    if config.backend not in DEEPSEEK_BACKENDS:
        config = config.with_backend(backend)
    return read_page(image, config)


def fatal_reason(error: str) -> str | None:
    for pattern, reason in FATAL_PATTERNS:
        if pattern.search(error or ""):
            return reason
    return None


# Página de jornal tem milhares de caracteres: bem menos que isso costuma ser leitura truncada
# (modelo parou cedo, recorte errado) e não pode entrar na base como "ok" sem alguém olhar.
MIN_CHARS_OK = 300
MIN_CONFIDENCE_OK = 0.5


def needs_review(metrics: dict[str, Any]) -> bool:
    confidence = metrics.get("confidence")
    return (metrics.get("chars") or 0) < MIN_CHARS_OK or (confidence is not None and confidence < MIN_CONFIDENCE_OK)


# Marcas de leitura incerta que o modelo deixa no texto: [da], [?], [ilegível]...
UNCERTAIN_RE = re.compile(r"\[[^\[\]\n]{0,30}\]")


def summarize(result: Any) -> dict[str, Any]:
    """Métricas de um ReadResult para o banco."""
    usage = result.usage or {}
    cli = getattr(result, "usage_cli", None) or {}
    segments = ((result.extraction or {}).get("segments") or []) if result.extraction else []
    confidences = [float(s["confidence"]) for s in segments if isinstance(s.get("confidence"), (int, float))]
    return {
        "tokens_in": int(usage.get("input_tokens", 0) or 0) + int(usage.get("cache_read_input_tokens", 0) or 0),
        # Para a assinatura conta a saída real do CLI; nos outros backends, a saída da API.
        "tokens_out": int(cli.get("output_tokens") or usage.get("output_tokens", 0) or 0),
        "thinking": int(cli.get("thinking_tokens") or usage.get("thinking_tokens", 0) or 0),
        "cost_usd": float((result.details or {}).get("cost_usd_no_cache") or result.cost_usd or 0),
        "cost_batch": float(result.cost_usd_batch or 0),
        "seconds": float(result.elapsed_s or 0),
        "chars": sum(len(s.get("text") or "") for s in segments),
        "segments": len(segments),
        "people": sum(len(s.get("people") or []) for s in segments),
        "uncertain": sum(len(UNCERTAIN_RE.findall(s.get("text") or "")) for s in segments),
        "confidence": round(sum(confidences) / len(confidences), 3) if confidences else None,
    }


class Runner:
    def __init__(
        self,
        store: Store,
        *,
        reader: Callable[[Path, str, str], Any] = default_reader,
        capturer: Callable[[list[str]], None] = default_capturer,
        output_dir: Path = OUTPUT_DIR,
        idle_sleep: float = 2.0,
    ):
        self.store = store
        self.reader = reader
        self.capturer = capturer
        self.output_dir = output_dir
        self.idle_sleep = idle_sleep
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    # ---------- ciclo de vida ----------
    def start(self) -> None:
        self.store.recover()
        self.threads = [threading.Thread(target=self._capture_loop, name="captura", daemon=True)]
        self.threads += [threading.Thread(target=self._read_loop, args=(i,), name=f"leitor-{i + 1}", daemon=True)
                         for i in range(MAX_WORKERS)]
        for thread in self.threads:
            thread.start()
        self.store.log("info", "Processador iniciado")

    def stop(self) -> None:
        self.stop_event.set()

    def _sleep(self) -> None:
        self.stop_event.wait(self.idle_sleep)

    def _over_budget(self) -> bool:
        settings = self.store.settings()
        estimate = self.store.usage_estimate()["pct"]
        if estimate is not None and estimate >= float(settings["limite_pct"]):
            self.store.pause(f"A cota estimada chegou a {estimate:.0f}% (limite {settings['limite_pct']:.0f}%).")
            return True
        return False

    # ---------- captura ----------
    def _capture_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if not self.capture_once():
                    self._sleep()
            except Exception as exc:  # noqa: BLE001
                logger.exception("captura")
                self.store.log("erro", f"Captura: {type(exc).__name__}: {exc}")
                self._sleep()

    def capture_once(self) -> bool:
        settings = self.store.settings()
        if settings["paused"]:
            return False
        batch = self.store.next_to_capture(int(settings["prefetch"]))
        if not batch:
            return False
        missing = []
        for item in batch:
            if image_exists(item["bib"], item["page_id"]):
                self.store.mark(item["job_id"], item["page"], "pronta")
            else:
                self.store.mark(item["job_id"], item["page"], "baixando", worker="captura", started_at=time.time())
                missing.append(item)
        if missing:
            try:
                self.capturer([item["page_id"] for item in missing])
            except Exception as exc:  # noqa: BLE001
                self.store.log("erro", f"Captura falhou: {type(exc).__name__}: {exc}")
            max_attempts = int(settings["max_tentativas"])
            for item in missing:
                if image_exists(item["bib"], item["page_id"]):
                    self.store.mark(item["job_id"], item["page"], "pronta", captured=1, worker=None, attempts=0)
                else:
                    row = self.store.page_row(item["job_id"], item["page_id"]) or {}
                    attempts = int(row.get("attempts") or 0) + 1
                    status = "erro" if attempts >= max_attempts else "pendente"
                    self.store.mark(item["job_id"], item["page"], status, attempts=attempts, worker=None,
                                    error="não foi possível baixar a imagem" if status == "erro" else None,
                                    finished_at=time.time() if status == "erro" else None)
        return True

    # ---------- leitura ----------
    def _read_loop(self, index: int) -> None:
        name = f"leitor-{index + 1}"
        while not self.stop_event.is_set():
            try:
                settings = self.store.settings()
                if settings["paused"] or index >= int(settings["workers"]) or self._over_budget():
                    self._sleep()
                    continue
                if not self.read_once(name):
                    self._sleep()
            except Exception as exc:  # noqa: BLE001
                logger.exception(name)
                self.store.log("erro", f"{name}: {type(exc).__name__}: {exc}")
                self._sleep()

    def read_once(self, worker: str) -> bool:
        item = self.store.claim_ready(worker)
        if not item:
            return False
        job_id, page, bib, page_id = item["job_id"], item["page"], item["bib"], item["page_id"]
        image = image_path(bib, page_id)
        if not image.exists():
            self.store.mark(job_id, page, "pendente", worker=None)
            return True
        try:
            result = self.reader(image, item["config"], item["backend"])
        except Exception as exc:  # noqa: BLE001
            result = None
            error = f"{type(exc).__name__}: {exc}"
        else:
            error = result.error

        metrics = summarize(result) if result is not None else {}
        if error:
            reason = fatal_reason(error)
            if reason:
                # não é culpa da página: devolve para a fila sem gastar tentativa
                self.store.mark(job_id, page, "pronta", worker=None, attempts=max(0, item["attempts"]))
                self.store.pause(reason)
                return True
            max_attempts = int(self.store.get("max_tentativas"))
            final = item["attempts"] + 1 >= max_attempts
            self.store.mark(job_id, page, "erro" if final else "pronta", worker=None, error=str(error)[:500],
                            finished_at=time.time() if final else None, **metrics)
            self.store.log("aviso", f"{page_id}: {str(error)[:160]}" + ("" if final else " (vai tentar de novo)"))
            return True

        out = self.output_dir / item["config"] / bib / f"{page_id}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = result.to_dict()
        # guarda só as caixas dos recortes: o inspetor usa para mostrar onde cada trecho está no scan
        manifest = payload.pop("tile_manifest", None) or []
        payload["tile_boxes"] = [m.get("box") for m in manifest if isinstance(m, dict) and m.get("box")]
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        review = {"review": "revisar"} if needs_review(metrics) else {}
        self.store.mark(job_id, page, "ok", worker=None, error=None, finished_at=time.time(), out_path=str(out),
                        **review, **metrics)
        if item["captured"] and self.store.get("apagar_imagens"):
            image.unlink(missing_ok=True)
        return True
