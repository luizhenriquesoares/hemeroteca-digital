"""Fila e métricas do processamento em SQLite local (data/pipeline/painel.db).

Uma linha por página em ``pages`` guarda o estado (pendente → baixando → pronta →
lendo → ok/erro) e o consumo medido (tokens, custo, tempo). ``usage_marks`` guarda
as porcentagens da cota semanal digitadas no painel, usadas para calibrar quantos
tokens de saída custam 1% da assinatura.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import CACHE_DIR, DATA_DIR

DB_PATH = DATA_DIR / "pipeline" / "painel.db"

# Calibração de 2026-10-31: 11 páginas do Opus 5.5 (186.445 tokens de saída) = +1% da semana.
DEFAULT_TOKENS_POR_PCT = 186_000

DEFAULT_SETTINGS: dict[str, Any] = {
    "paused": True,  # começa pausado: o usuário aperta "continuar" no painel
    "pause_reason": "aguardando início",
    "workers": 1,
    "limite_pct": 95.0,  # pausa sozinho quando a % estimada da semana chegar aqui
    "tokens_por_pct": DEFAULT_TOKENS_POR_PCT,
    "apagar_imagens": False,  # apaga só imagens baixadas pelo painel, depois da leitura ok
    "prefetch": 4,  # imagens baixadas à frente da leitura
    "max_tentativas": 2,
    "revisar_marcas": 5,  # páginas com tantas marcas [..] de leitura incerta vão para a revisão
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id INTEGER PRIMARY KEY,
  bib TEXT NOT NULL,
  nome TEXT,
  config TEXT NOT NULL,
  backend TEXT NOT NULL DEFAULT 'claude-code',
  first_page INTEGER NOT NULL,
  last_page INTEGER NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'ativo',  -- ativo | pausado | cancelado | concluido
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pages(
  job_id INTEGER NOT NULL,
  bib TEXT NOT NULL,
  page INTEGER NOT NULL,
  page_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pendente',  -- pendente | baixando | pronta | lendo | ok | erro
  worker TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  captured INTEGER NOT NULL DEFAULT 0,  -- 1 = imagem baixada pelo painel (pode ser apagada)
  tokens_in INTEGER, tokens_out INTEGER, thinking INTEGER,
  cost_usd REAL, cost_batch REAL, seconds REAL, chars INTEGER, segments INTEGER,
  error TEXT, started_at REAL, finished_at REAL, out_path TEXT,
  PRIMARY KEY(job_id, page)
);
CREATE INDEX IF NOT EXISTS pages_status ON pages(status);
CREATE INDEX IF NOT EXISTS pages_finished ON pages(finished_at);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_marks(
  id INTEGER PRIMARY KEY, ts REAL NOT NULL, pct REAL NOT NULL, tokens_out_total INTEGER NOT NULL, note TEXT
);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, ts REAL NOT NULL, level TEXT NOT NULL, msg TEXT NOT NULL);
"""

# Colunas acrescentadas depois da primeira versão (o banco existente é migrado ao abrir).
EXTRA_PAGE_COLUMNS = {
    "people": "INTEGER",  # pessoas mencionadas na página
    "uncertain": "INTEGER",  # marcas [..] de leitura incerta ou ilegível
    "confidence": "REAL",  # confiança média dos segmentos (0 a 1)
    "review": "TEXT",  # NULL | revisar | aprovada
}

PAGE_CODES = {"pendente": "q", "baixando": "d", "pronta": "p", "lendo": "l", "ok": "o", "erro": "e", "cancelada": "c"}


def page_id_for(bib: str, page: int) -> str:
    return f"{bib}_{page:05d}"


def load_acervos() -> list[dict[str, Any]]:
    path = CACHE_DIR / "acervos_pe.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []


class Store:
    """Acesso thread-safe ao banco (uma conexão, um lock)."""

    def __init__(self, path: Path | str = DB_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)
            have = {r[1] for r in self._conn.execute("PRAGMA table_info(pages)")}
            for col, kind in EXTRA_PAGE_COLUMNS.items():
                if col not in have:
                    self._conn.execute(f"ALTER TABLE pages ADD COLUMN {col} {kind}")

    # ---------- utilitários ----------
    def _q(self, sql: str, params: tuple | list = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _x(self, sql: str, params: tuple | list = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return cur.lastrowid if sql.lstrip().upper().startswith("INSERT") else cur.rowcount

    # ---------- configurações ----------
    def settings(self) -> dict[str, Any]:
        values = dict(DEFAULT_SETTINGS)
        for row in self._q("SELECT key, value FROM settings"):
            values[row["key"]] = json.loads(row["value"])
        return values

    def get(self, key: str) -> Any:
        return self.settings()[key]

    def set(self, **values: Any) -> None:
        for key, value in values.items():
            if key not in DEFAULT_SETTINGS:
                raise KeyError(key)
            self._x("INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json.dumps(value)))

    def pause(self, reason: str) -> None:
        self.set(paused=True, pause_reason=reason)
        self.log("aviso", f"Pausado: {reason}")

    def resume(self) -> None:
        self.set(paused=False, pause_reason="")
        self.log("info", "Processamento retomado")

    # ---------- eventos ----------
    def log(self, level: str, msg: str) -> None:
        self._x("INSERT INTO events(ts, level, msg) VALUES(?, ?, ?)", (time.time(), level, msg))

    def events(self, limit: int = 40) -> list[dict[str, Any]]:
        return [dict(r) for r in self._q("SELECT ts, level, msg FROM events ORDER BY id DESC LIMIT ?", (limit,))]

    # ---------- trabalhos ----------
    def add_job(self, bib: str, config: str, first_page: int, last_page: int, *, priority: int = 0,
                backend: str = "claude-code", nome: str | None = None, page_exists=None) -> int:
        if first_page < 1 or last_page < first_page:
            raise ValueError("Intervalo de páginas inválido")
        if nome is None:
            nome = next((a.get("nome") for a in load_acervos() if a.get("bib") == bib), f"Acervo {bib}")
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                cur = self._conn.execute(
                    "INSERT INTO jobs(bib, nome, config, backend, first_page, last_page, priority, created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (bib, nome, config, backend, first_page, last_page, priority, datetime.now(timezone.utc).isoformat()),
                )
                job_id = cur.lastrowid
                rows = []
                for page in range(first_page, last_page + 1):
                    pid = page_id_for(bib, page)
                    ready = bool(page_exists and page_exists(bib, pid))
                    rows.append((job_id, bib, page, pid, "pronta" if ready else "pendente"))
                self._conn.executemany("INSERT INTO pages(job_id, bib, page, page_id, status) VALUES(?,?,?,?,?)", rows)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self.log("info", f"Novo trabalho #{job_id}: {bib} págs {first_page}–{last_page} com {config}")
        return job_id

    def set_job(self, job_id: int, *, status: str | None = None, priority: int | None = None) -> None:
        if status is not None:
            if status not in ("ativo", "pausado", "cancelado"):
                raise ValueError(status)
            self._x("UPDATE jobs SET status=? WHERE id=?", (status, job_id))
            if status == "cancelado":
                self._x("UPDATE pages SET status='cancelada' WHERE job_id=? AND status IN ('pendente','pronta')", (job_id,))
            elif status == "ativo":
                self._x("UPDATE pages SET status='pendente' WHERE job_id=? AND status='cancelada'", (job_id,))
            self.log("info", f"Trabalho #{job_id}: {status}")
        if priority is not None:
            self._x("UPDATE jobs SET priority=? WHERE id=?", (priority, job_id))

    def retry_errors(self, job_id: int) -> int:
        return self._x("UPDATE pages SET status='pendente', attempts=0, error=NULL WHERE job_id=? AND status='erro'", (job_id,))

    def job(self, job_id: int) -> dict[str, Any] | None:
        rows = self._q("SELECT * FROM jobs WHERE id=?", (job_id,))
        return dict(rows[0]) if rows else None

    def jobs(self) -> list[dict[str, Any]]:
        sql = """
        SELECT j.*,
          COUNT(p.page) AS total,
          SUM(p.status='ok') AS ok,
          SUM(p.status='erro') AS erro,
          SUM(p.status IN ('baixando','lendo')) AS andamento,
          SUM(p.status='pronta') AS prontas,
          SUM(CASE WHEN p.status='ok' THEN p.tokens_out ELSE 0 END) AS tokens_out,
          SUM(CASE WHEN p.status='ok' THEN p.cost_usd ELSE 0 END) AS cost_usd,
          AVG(CASE WHEN p.status='ok' THEN p.seconds END) AS avg_seconds,
          AVG(CASE WHEN p.status='ok' THEN p.tokens_out END) AS avg_tokens_out
        FROM jobs j LEFT JOIN pages p ON p.job_id=j.id
        GROUP BY j.id ORDER BY (j.status='cancelado'), j.priority DESC, j.id
        """
        out = []
        for row in self._q(sql):
            d = dict(row)
            for k in ("ok", "erro", "andamento", "prontas", "tokens_out"):
                d[k] = int(d[k] or 0)
            d["cost_usd"] = float(d["cost_usd"] or 0)
            if d["status"] == "ativo" and d["total"] and d["ok"] + d["erro"] == d["total"]:
                self._x("UPDATE jobs SET status='concluido' WHERE id=?", (d["id"],))
                d["status"] = "concluido"
            out.append(d)
        return out

    # ---------- fila ----------
    def recover(self) -> None:
        """Ao iniciar: páginas que estavam em andamento voltam para a fila."""
        self._x("UPDATE pages SET status='pendente', worker=NULL WHERE status='baixando'")
        self._x("UPDATE pages SET status='pronta', worker=NULL WHERE status='lendo'")

    _ACTIVE = "SELECT id FROM jobs WHERE status='ativo'"

    def next_to_capture(self, limit: int) -> list[dict[str, Any]]:
        """Páginas pendentes dos trabalhos ativos, respeitando quantas já estão prontas (prefetch)."""
        ready = self._q(f"SELECT COUNT(*) AS n FROM pages WHERE status IN ('pronta','baixando') AND job_id IN ({self._ACTIVE})")[0]["n"]
        free = max(0, limit - ready)
        if not free:
            return []
        rows = self._q(f"""
            SELECT p.job_id, p.bib, p.page, p.page_id FROM pages p JOIN jobs j ON j.id=p.job_id
            WHERE p.status='pendente' AND j.status='ativo'
            ORDER BY j.priority DESC, j.id, p.page LIMIT ?""", (free,))
        return [dict(r) for r in rows]

    def mark(self, job_id: int, page: int, status: str, **fields: Any) -> None:
        cols = ["status=?"] + [f"{k}=?" for k in fields]
        self._x(f"UPDATE pages SET {', '.join(cols)} WHERE job_id=? AND page=?", [status, *fields.values(), job_id, page])

    def claim_ready(self, worker: str) -> dict[str, Any] | None:
        with self._lock:
            rows = self._conn.execute("""
                SELECT p.job_id, p.page, p.bib, p.page_id, p.attempts, p.captured, j.config, j.backend FROM pages p JOIN jobs j ON j.id=p.job_id
                WHERE p.status='pronta' AND j.status='ativo'
                ORDER BY j.priority DESC, j.id, p.page LIMIT 1""").fetchall()
            if not rows:
                return None
            row = dict(rows[0])
            self._conn.execute(
                "UPDATE pages SET status='lendo', worker=?, started_at=?, attempts=attempts+1 WHERE job_id=? AND page=?",
                (worker, time.time(), row["job_id"], row["page"]),
            )
            return row

    def in_progress(self) -> list[dict[str, Any]]:
        rows = self._q("""SELECT p.job_id, p.page_id, p.page, p.status, p.worker, p.started_at, j.config, j.nome, j.bib
                          FROM pages p JOIN jobs j ON j.id=p.job_id WHERE p.status IN ('baixando','lendo') ORDER BY p.started_at""")
        now = time.time()
        return [dict(r, elapsed=round(now - r["started_at"], 1) if r["started_at"] else None) for r in rows]

    def recent(self, limit: int = 30) -> list[dict[str, Any]]:
        rows = self._q("""SELECT p.job_id, p.page_id, p.status, p.tokens_in, p.tokens_out, p.thinking, p.cost_usd, p.cost_batch,
                                 p.seconds, p.chars, p.segments, p.error, p.finished_at, p.people, p.uncertain,
                                 p.confidence, p.review, j.config, j.nome
                          FROM pages p JOIN jobs j ON j.id=p.job_id WHERE p.finished_at IS NOT NULL
                          ORDER BY p.finished_at DESC LIMIT ?""", (limit,))
        return [dict(r) for r in rows]

    def page_row(self, job_id: int, page_id: str) -> dict[str, Any] | None:
        rows = self._q("SELECT p.*, j.config FROM pages p JOIN jobs j ON j.id=p.job_id WHERE p.job_id=? AND p.page_id=?", (job_id, page_id))
        return dict(rows[0]) if rows else None

    def job_pages(self, job_id: int) -> dict[str, Any]:
        """Situação de todas as páginas de um trabalho, compacta (um caractere por página) para a fita."""
        limit = int(self.get("revisar_marcas"))
        rows = self._q("SELECT page, status, uncertain, review FROM pages WHERE job_id=? ORDER BY page", (job_id,))
        codes = []
        for r in rows:
            code = PAGE_CODES.get(r["status"], "q")
            if code == "o" and r["review"] != "aprovada" and (r["review"] == "revisar" or (r["uncertain"] or 0) >= limit):
                code = "r"
            codes.append(code)
        return {"first": rows[0]["page"] if rows else 0, "codes": "".join(codes)}

    def job_rows(self, job_id: int, *, status: str | None = None, limit: int = 60, offset: int = 0) -> list[dict[str, Any]]:
        where, params = "job_id=?", [job_id]
        if status:
            where += " AND status=?"
            params.append(status)
        rows = self._q(f"""SELECT page, page_id, status, tokens_out, cost_usd, seconds, segments, people, uncertain,
                                  confidence, review, error, finished_at
                           FROM pages WHERE {where} ORDER BY (finished_at IS NULL), finished_at DESC, page LIMIT ? OFFSET ?""",
                       [*params, limit, offset])
        return [dict(r) for r in rows]

    def job_stats(self, job_id: int) -> dict[str, Any]:
        row = self._q("""SELECT MIN(seconds) AS min_s, MAX(seconds) AS max_s, AVG(seconds) AS avg_s,
                                AVG(tokens_out) AS avg_out, SUM(people) AS people, SUM(segments) AS segments,
                                AVG(confidence) AS confidence, SUM(cost_batch) AS cost_batch
                         FROM pages WHERE job_id=? AND status='ok'""", (job_id,))[0]
        hist = self._q("SELECT CAST(tokens_out / 2500 AS INTEGER) AS b, COUNT(*) AS n FROM pages WHERE job_id=? AND status='ok' GROUP BY b",
                       (job_id,))
        errors = self._q("SELECT error, COUNT(*) AS n, GROUP_CONCAT(page) AS pages FROM pages WHERE job_id=? AND status='erro' GROUP BY error",
                         (job_id,))
        return {**{k: row[k] for k in row.keys()}, "histogram": {r["b"]: r["n"] for r in hist},
                "errors": [dict(r) for r in errors]}

    def neighbors(self, job_id: int, page: int) -> dict[str, str | None]:
        prev = self._q("SELECT page_id FROM pages WHERE job_id=? AND page<? AND status='ok' ORDER BY page DESC LIMIT 1", (job_id, page))
        nxt = self._q("SELECT page_id FROM pages WHERE job_id=? AND page>? AND status='ok' ORDER BY page LIMIT 1", (job_id, page))
        return {"prev": prev[0]["page_id"] if prev else None, "next": nxt[0]["page_id"] if nxt else None}

    # ---------- revisão ----------
    def review_queue(self, limit: int = 200) -> list[dict[str, Any]]:
        threshold = int(self.get("revisar_marcas"))
        rows = self._q("""SELECT p.job_id, p.page_id, p.page, p.uncertain, p.confidence, p.people, p.segments, p.review,
                                 p.finished_at, j.nome, j.config
                          FROM pages p JOIN jobs j ON j.id=p.job_id
                          WHERE p.status='ok' AND COALESCE(p.review,'') != 'aprovada'
                            AND (p.review='revisar' OR COALESCE(p.uncertain,0) >= ?)
                          ORDER BY COALESCE(p.uncertain,0) DESC, p.finished_at DESC LIMIT ?""", (threshold, limit))
        return [dict(r) for r in rows]

    def set_review(self, job_id: int, page_id: str, value: str | None) -> None:
        if value not in (None, "revisar", "aprovada"):
            raise ValueError(value)
        self._x("UPDATE pages SET review=? WHERE job_id=? AND page_id=?", (value, job_id, page_id))

    def quality(self) -> dict[str, Any]:
        threshold = int(self.get("revisar_marcas"))
        row = self._q("""SELECT COUNT(*) AS lidas, AVG(confidence) AS confidence, COALESCE(SUM(people),0) AS people,
                                COALESCE(SUM(segments),0) AS segments, COALESCE(SUM(uncertain),0) AS uncertain,
                                SUM(review='aprovada') AS aprovadas,
                                SUM(COALESCE(review,'')!='aprovada' AND (review='revisar' OR COALESCE(uncertain,0)>=?)) AS revisar
                         FROM pages WHERE status='ok'""", (threshold,))[0]
        return {k: (row[k] or 0) for k in row.keys()}

    # ---------- métricas ----------
    def tokens_out_total(self) -> int:
        return int(self._q("SELECT COALESCE(SUM(tokens_out),0) AS t FROM pages WHERE finished_at IS NOT NULL")[0]["t"])

    def totals(self) -> dict[str, Any]:
        row = self._q("""SELECT COUNT(*) AS total, SUM(p.status='ok') AS ok, SUM(p.status='erro') AS erro,
                                SUM(p.status IN ('pendente','pronta','baixando','lendo')) AS restantes,
                                COALESCE(SUM(p.tokens_out),0) AS tokens_out, COALESCE(SUM(p.tokens_in),0) AS tokens_in,
                                COALESCE(SUM(p.cost_usd),0) AS cost_usd, COALESCE(SUM(p.cost_batch),0) AS cost_batch,
                                AVG(CASE WHEN p.status='ok' THEN p.seconds END) AS avg_seconds,
                                AVG(CASE WHEN p.status='ok' THEN p.tokens_out END) AS avg_tokens_out,
                                AVG(CASE WHEN p.status='ok' THEN p.cost_usd END) AS avg_cost
                         FROM pages p JOIN jobs j ON j.id=p.job_id WHERE j.status!='cancelado'""")[0]
        d = {k: (row[k] or 0) for k in row.keys()}
        hour = self._q("""SELECT COUNT(*) AS n, COALESCE(SUM(tokens_out),0) AS t FROM pages
                          WHERE status='ok' AND finished_at > ?""", (time.time() - 3600,))[0]
        d["pages_last_hour"] = hour["n"]
        d["tokens_last_hour"] = hour["t"]
        return d

    def timeline(self, hours: int = 24, bucket_min: int = 60) -> list[dict[str, Any]]:
        start = time.time() - hours * 3600
        size = bucket_min * 60
        rows = self._q("""SELECT CAST((finished_at - ?) / ? AS INTEGER) AS b, COUNT(*) AS pages, COALESCE(SUM(tokens_out),0) AS tokens
                          FROM pages WHERE status='ok' AND finished_at > ? GROUP BY b""", (start, size, start))
        by = {r["b"]: r for r in rows}
        return [{"ts": start + i * size, "pages": by[i]["pages"] if i in by else 0, "tokens": by[i]["tokens"] if i in by else 0}
                for i in range(int(hours * 60 / bucket_min))]

    def processed_by_bib(self) -> dict[str, int]:
        return {r["bib"]: r["n"] for r in self._q("SELECT bib, COUNT(DISTINCT page) AS n FROM pages WHERE status='ok' GROUP BY bib")}

    # ---------- % da assinatura ----------
    def add_usage_mark(self, pct: float, note: str = "") -> None:
        self._x("INSERT INTO usage_marks(ts, pct, tokens_out_total, note) VALUES(?,?,?,?)",
                (time.time(), float(pct), self.tokens_out_total(), note))
        self.log("info", f"Cota da semana informada: {pct:.0f}%")

    def usage_marks(self, limit: int = 20) -> list[dict[str, Any]]:
        return [dict(r) for r in self._q("SELECT * FROM usage_marks ORDER BY id DESC LIMIT ?", (limit,))]

    def usage_estimate(self) -> dict[str, Any]:
        """% da semana estimada = última % digitada + tokens de saída desde então / tokens por 1%."""
        settings = self.settings()
        marks = self.usage_marks(50)
        calibrated = None
        # calibra pelo par mais recente com subida de pelo menos 1 ponto e tokens gastos no meio
        for newer, older in zip(marks, marks[1:]):
            dp = newer["pct"] - older["pct"]
            dt = newer["tokens_out_total"] - older["tokens_out_total"]
            if dp >= 1 and dt > 0:
                calibrated = dt / dp
                break
        per_pct = calibrated or float(settings["tokens_por_pct"])
        if not marks:
            return {"pct": None, "per_pct": per_pct, "calibrated": calibrated, "last_mark": None}
        last = marks[0]
        since = self.tokens_out_total() - last["tokens_out_total"]
        return {
            "pct": round(last["pct"] + since / per_pct, 1),
            "per_pct": round(per_pct),
            "calibrated": round(calibrated) if calibrated else None,
            "last_mark": last,
            "tokens_since_mark": since,
        }
