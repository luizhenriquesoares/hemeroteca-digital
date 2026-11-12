"""Painel web do processamento: API JSON + fluxo em tempo real (SSE) + página estática.

Roda só em 127.0.0.1 (``python main.py painel``). O processador sobe junto, no
mesmo processo, e é controlado pelo banco: pausar/continuar não reinicia nada.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from src.pipeline.store import Store, load_acervos
from src.pipeline.worker import Runner, image_exists, image_path

STATIC = Path(__file__).parent / "static"
PAGE_ID_RE = re.compile(r"^[0-9A-Za-z]+(?:_[0-9A-Za-z]+)*_\d{5}$")
EDITABLE = {"workers": int, "limite_pct": float, "tokens_por_pct": float, "apagar_imagens": bool, "prefetch": int, "max_tentativas": int,
            "revisar_marcas": int}


# ---------- acesso ----------
# Sem PAINEL_SENHA o painel é só local (127.0.0.1). Num servidor, defina PAINEL_SENHA: toda rota
# passa a exigir login, com cookie assinado e limite de tentativas por IP.
COOKIE = "painel_sessao"
SESSION_DAYS = 14
LOGIN_HTML = """<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Entrar · Scriptorium</title><link rel="stylesheet" href="/static/fonts/fonts.css"><style>
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#F4F0E6;font:14px Inter,system-ui,sans-serif;color:#1B1814}
form{width:340px;max-width:calc(100vw - 32px);background:#FBF9F3;border:1px solid #DDD5C5;border-radius:14px;padding:28px;display:flex;flex-direction:column;gap:14px}
.m{width:38px;height:38px;border-radius:9px;background:#8E2F14;color:#FBEFE6;font:italic 24px Newsreader,Georgia,serif;display:flex;align-items:center;justify-content:center}
h1{font:500 26px Newsreader,Georgia,serif;margin:4px 0 0}p{margin:0;color:#776F62;font-size:13px}
input{height:40px;border:1px solid #CABFAB;border-radius:9px;padding:0 12px;font:inherit;background:#fff}
button{height:40px;border:0;border-radius:9px;background:#1B1814;color:#FBF6EC;font:500 14px Inter,sans-serif;cursor:pointer}
.e{color:#A12E28;font-size:13px}</style></head><body><form method="post" action="/login"><div class="m">S</div><h1>Scriptorium</h1>
<p>Painel de transcrição da Hemeroteca PE</p>__ERR__<input type="password" name="senha" placeholder="Senha" autofocus required><button>Entrar</button></form></body></html>"""


def _password() -> str:
    return os.environ.get("PAINEL_SENHA", "")


def _sign(expires: int, secret: str) -> str:
    mac = hmac.new(secret.encode(), str(expires).encode(), hashlib.sha256).hexdigest()
    return f"{expires}.{mac}"


def _valid(token: str | None, secret: str) -> bool:
    if not token or "." not in token:
        return False
    expires, _ = token.split(".", 1)
    if not expires.isdigit() or int(expires) < time.time():
        return False
    return hmac.compare_digest(token, _sign(int(expires), secret))


def install_auth(app: FastAPI, password: str) -> None:
    # a chave do cookie deriva da senha: trocar a senha derruba as sessões abertas
    secret = hashlib.sha256(("scriptorium:" + password).encode()).hexdigest()
    failures: dict[str, list[float]] = {}

    def client_ip(request: Request) -> str:
        fwd = request.headers.get("x-forwarded-for", "")
        return (fwd.split(",")[0].strip() if fwd else "") or (request.client.host if request.client else "?")

    @app.middleware("http")
    async def guard(request: Request, call_next):
        path = request.url.path
        if path == "/login" or path.startswith("/static/fonts/") or _valid(request.cookies.get(COOKIE), secret):
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse({"detail": "faça login"}, status_code=401)
        return RedirectResponse("/login", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    def login_page() -> str:
        return LOGIN_HTML.replace("__ERR__", "")

    @app.post("/login")
    def login(request: Request, senha: str = Form("")):
        ip = client_ip(request)
        now = time.time()
        recent = [t for t in failures.get(ip, []) if now - t < 900]
        if len(recent) >= 8:
            return HTMLResponse(LOGIN_HTML.replace("__ERR__", '<p class="e">Muitas tentativas. Espere 15 minutos.</p>'), status_code=429)
        if not hmac.compare_digest(senha.encode(), password.encode()):
            failures[ip] = recent + [now]
            time.sleep(1)
            return HTMLResponse(LOGIN_HTML.replace("__ERR__", '<p class="e">Senha incorreta.</p>'), status_code=401)
        failures.pop(ip, None)
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie(COOKIE, _sign(int(now + SESSION_DAYS * 86400), secret), max_age=SESSION_DAYS * 86400,
                        httponly=True, samesite="strict", secure=request.headers.get("x-forwarded-proto") == "https")
        return resp

    @app.post("/logout")
    def logout():
        resp = RedirectResponse("/login", status_code=303)
        resp.delete_cookie(COOKIE)
        return resp


def config_catalog() -> list[dict[str, Any]]:
    from src.vision.reader import CONFIGS, DEEPSEEK_BACKENDS, PRICING_USD_PER_MTOK

    out = []
    for name, cfg in CONFIGS.items():
        out.append({
            "name": name,
            "model": cfg.model,
            "deepseek": cfg.backend in DEEPSEEK_BACKENDS,
            "price": PRICING_USD_PER_MTOK.get(cfg.model),
        })
    return out


def build_state(store: Store) -> dict[str, Any]:
    settings = store.settings()
    totals = store.totals()
    usage = store.usage_estimate()
    workers = max(1, int(settings["workers"]))
    eta_s = None
    if totals["avg_seconds"] and totals["restantes"]:
        eta_s = totals["restantes"] * totals["avg_seconds"] / workers
    pages_per_week = None
    if totals["avg_tokens_out"] and usage["per_pct"]:
        pages_per_week = round(usage["per_pct"] * 100 / totals["avg_tokens_out"])
    return {
        "settings": settings,
        "totals": totals,
        "usage": usage,
        "eta_s": eta_s,
        "pages_per_week": pages_per_week,
        "jobs": store.jobs(),
        "in_progress": store.in_progress(),
        "recent": store.recent(30),
        "timeline": store.timeline(),
        "events": store.events(30),
        "quality": store.quality(),
        "usage_marks": store.usage_marks(30),
        "acervo_total": sum(int(a.get("paginas") or 0) for a in load_acervos()),
        "now": time.time(),
    }


def create_app(store: Store | None = None, runner: Runner | None = None, *, start_runner: bool = True,
               password: str | None = None) -> FastAPI:
    store = store or Store()
    runner = runner or Runner(store)

    @asynccontextmanager
    async def lifespan(_app):
        if start_runner:
            runner.start()
        yield
        runner.stop()

    app = FastAPI(title="Painel de processamento", lifespan=lifespan)
    app.state.store = store
    app.state.runner = runner
    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    if password is None:
        password = _password()
    if password:
        install_auth(app, password)

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return (STATIC / "painel.html").read_text(encoding="utf-8")

    @app.get("/api/state")
    def state() -> dict[str, Any]:
        return build_state(store)

    @app.get("/api/stream")
    async def stream() -> StreamingResponse:
        async def events():
            while True:
                data = await asyncio.to_thread(build_state, store)
                yield f"data: {json.dumps(data, default=str)}\n\n"
                await asyncio.sleep(2)
        return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.post("/api/control")
    def control(action: str = Body(..., embed=True)) -> dict[str, Any]:
        if action == "pause":
            store.pause("pausado no painel")
        elif action == "resume":
            store.resume()
        else:
            raise HTTPException(400, "ação inválida")
        return {"ok": True}

    @app.post("/api/settings")
    def settings(values: dict[str, Any] = Body(...)) -> dict[str, Any]:
        clean = {}
        for key, value in values.items():
            if key not in EDITABLE:
                raise HTTPException(400, f"campo não editável: {key}")
            clean[key] = EDITABLE[key](value)
        if "workers" in clean and not 1 <= clean["workers"] <= 8:
            raise HTTPException(400, "leitores em paralelo: de 1 a 8")
        store.set(**clean)
        return store.settings()

    @app.post("/api/jobs")
    def add_job(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        from src.vision.reader import CONFIGS

        bib = str(payload.get("bib", "")).strip()
        config = str(payload.get("config", "")).strip()
        if not re.fullmatch(r"[0-9A-Za-z_]+", bib):
            raise HTTPException(400, "acervo inválido")
        if config not in CONFIGS:
            raise HTTPException(400, "configuração inválida")
        backend = str(payload.get("backend") or "claude-code")
        if backend not in ("claude-code", "api"):
            raise HTTPException(400, "backend inválido")
        first = int(payload.get("first_page") or 1)
        last = int(payload.get("last_page") or first)
        total = next((a.get("paginas", 0) for a in load_acervos() if a.get("bib") == bib), 0)
        if total and last > total:
            raise HTTPException(400, f"o acervo tem {total} páginas")
        try:
            job_id = store.add_job(bib, config, first, last, priority=int(payload.get("priority") or 0),
                                   backend=backend, page_exists=image_exists)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"id": job_id}

    @app.get("/api/jobs/{job_id}")
    def job_detail(job_id: int, status: str | None = None, offset: int = 0, limit: int = 25) -> dict[str, Any]:
        job = next((j for j in store.jobs() if j["id"] == job_id), None)
        if not job:
            raise HTTPException(404, "trabalho não existe")
        return {"job": job, "stats": store.job_stats(job_id), "ribbon": store.job_pages(job_id),
                "rows": store.job_rows(job_id, status=status or None, offset=max(0, offset),
                                               limit=min(max(limit, 1), 500))}

    @app.get("/api/timeline")
    def timeline(hours: int = 24) -> list[dict[str, Any]]:
        hours = min(max(hours, 1), 24 * 7)
        bucket = 10 if hours <= 6 else 60 if hours <= 24 else 360
        return store.timeline(hours, bucket)

    @app.get("/api/review")
    def review() -> list[dict[str, Any]]:
        return store.review_queue()

    @app.post("/api/page/{job_id}/{page_id}/review")
    def set_review(job_id: int, page_id: str, value: str | None = Body(None, embed=True)) -> dict[str, Any]:
        if not store.page_row(job_id, page_id):
            raise HTTPException(404, "página não encontrada")
        try:
            store.set_review(job_id, page_id, value)
        except ValueError as exc:
            raise HTTPException(400, "valor inválido") from exc
        return {"ok": True}

    @app.post("/api/jobs/{job_id}")
    def update_job(job_id: int, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        if not store.job(job_id):
            raise HTTPException(404, "trabalho não existe")
        if payload.get("retry_errors"):
            store.retry_errors(job_id)
        try:
            store.set_job(job_id, status=payload.get("status"), priority=payload.get("priority"))
        except ValueError as exc:
            raise HTTPException(400, f"status inválido: {exc}") from exc
        return store.job(job_id) or {}

    @app.post("/api/usage")
    def usage(pct: float = Body(..., embed=True)) -> dict[str, Any]:
        if not 0 <= pct <= 100:
            raise HTTPException(400, "porcentagem de 0 a 100")
        store.add_usage_mark(pct)
        return store.usage_estimate()

    @app.get("/api/acervos")
    def acervos() -> list[dict[str, Any]]:
        done = store.processed_by_bib()
        return [dict(a, processadas=done.get(a.get("bib"), 0)) for a in load_acervos()]

    @app.get("/api/configs")
    def configs() -> list[dict[str, Any]]:
        return config_catalog()

    @app.get("/api/page/{job_id}/{page_id}")
    def page(job_id: int, page_id: str) -> dict[str, Any]:
        row = store.page_row(job_id, page_id)
        if not row:
            raise HTTPException(404, "página não encontrada")
        extraction, meta = None, {}
        if row.get("out_path") and Path(row["out_path"]).exists():
            data = json.loads(Path(row["out_path"]).read_text(encoding="utf-8"))
            extraction = data.get("extraction")
            meta = {k: data.get(k) for k in ("model", "grid", "tile_boxes", "elapsed_s", "effort", "backend")}
        job = store.job(job_id) or {}
        return {"row": row, "extraction": extraction, "meta": meta, "nome": job.get("nome"),
                "neighbors": store.neighbors(job_id, row["page"]), "has_image": image_exists(row["bib"], page_id)}

    @app.get("/api/image/{page_id}")
    def image(page_id: str) -> FileResponse:
        if not PAGE_ID_RE.fullmatch(page_id):
            raise HTTPException(400, "id inválido")
        bib = page_id.rsplit("_", 1)[0]
        path = image_path(bib, page_id)
        if not path.exists():
            raise HTTPException(404, "imagem não está no disco")
        return FileResponse(path, media_type="image/jpeg")

    return app
