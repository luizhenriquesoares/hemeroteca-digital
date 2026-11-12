"""Painel de processamento: fila, captura, leitura, pausa e calibração da cota (sem rede)."""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.pipeline import worker as worker_mod
from src.pipeline.store import Store
from src.pipeline.worker import Runner, fatal_reason


class FakeResult(SimpleNamespace):
    def to_dict(self):
        return {"page": self.page, "extraction": self.extraction, "tile_manifest": []}


def ok_result(page="p", out_tokens=1000):
    return FakeResult(page=page, error=None, usage={"input_tokens": 10, "output_tokens": out_tokens},
                      usage_cli={"output_tokens": out_tokens, "thinking_tokens": 5}, details={},
                      cost_usd=0.5, cost_usd_batch=0.25, elapsed_s=3.0,
                      extraction={"segments": [{"type": "anuncio", "text": "Vende-se hum escravo"}]})


@pytest.fixture()
def env(tmp_path, monkeypatch):
    images = tmp_path / "images"
    monkeypatch.setattr(worker_mod, "IMAGES_DIR", images)
    store = Store(tmp_path / "db.sqlite")
    store.set(paused=False)

    def capturer(ids):
        for pid in ids:
            bib = pid.rsplit("_", 1)[0]
            (images / bib).mkdir(parents=True, exist_ok=True)
            (images / bib / f"{pid}.jpg").write_bytes(b"jpg")

    calls = []

    def reader(image, config, backend):
        calls.append((image.stem, config, backend))
        return ok_result(image.stem)

    runner = Runner(store, reader=reader, capturer=capturer, output_dir=tmp_path / "out", idle_sleep=0.01)
    return SimpleNamespace(store=store, runner=runner, calls=calls, images=images, tmp=tmp_path)


def test_fluxo_completo_captura_le_e_salva(env):
    job = env.store.add_job("029033_04", "opus55-full", 1, 3, nome="Diario")
    env.store.set(prefetch=10)
    assert env.runner.capture_once()
    assert all(env.runner.read_once("t") for _ in range(3))
    assert not env.runner.read_once("t")
    jobs = env.store.jobs()
    assert jobs[0]["ok"] == 3 and jobs[0]["status"] == "concluido"
    assert env.calls[0] == ("029033_04_00001", "opus55-full", "claude-code")
    out = env.tmp / "out" / "opus55-full" / "029033_04" / "029033_04_00001.json"
    assert out.exists() and "tile_manifest" not in out.read_text()
    totals = env.store.totals()
    assert totals["tokens_out"] == 3000 and totals["pages_last_hour"] == 3
    row = env.store.page_row(job, "029033_04_00001")
    assert row["chars"] == len("Vende-se hum escravo") and row["segments"] == 1


def test_pausa_global_e_por_trabalho(env):
    a = env.store.add_job("A", "opus55-full", 1, 2, nome="A")
    env.store.set(prefetch=10)
    env.store.set_job(a, status="pausado")
    assert not env.runner.capture_once()
    env.store.set_job(a, status="ativo")
    env.store.pause("teste")
    assert not env.runner.capture_once()
    env.store.resume()
    assert env.runner.capture_once()


def test_prioridade_define_a_ordem(env):
    env.store.set(prefetch=1)
    env.store.add_job("A", "opus55-full", 1, 1, nome="A")
    b = env.store.add_job("B", "opus55-full", 1, 1, nome="B", priority=5)
    env.runner.capture_once()
    env.runner.read_once("t")
    assert env.calls[0][0] == "B_00001"
    assert env.store.page_row(b, "B_00001")["status"] == "ok"


def test_limite_de_uso_pausa_e_devolve_a_pagina(env):
    job = env.store.add_job("A", "opus55-full", 1, 1, nome="A")
    env.runner.reader = lambda *a: FakeResult(error="Claude AI usage limit reached|1700000000", usage={}, usage_cli={}, details={},
                                               cost_usd=0, cost_usd_batch=0, elapsed_s=1, extraction=None, page="x")
    env.runner.capture_once()
    env.runner.read_once("t")
    s = env.store.settings()
    assert s["paused"] and "Limite" in s["pause_reason"]
    row = env.store.page_row(job, "A_00001")
    assert row["status"] == "pronta" and row["attempts"] == 0


def test_erro_da_pagina_tenta_de_novo_e_depois_marca_erro(env):
    job = env.store.add_job("A", "opus55-full", 1, 1, nome="A")
    env.runner.reader = lambda *a: FakeResult(error="JSON fora do schema", usage={}, usage_cli={}, details={},
                                               cost_usd=0, cost_usd_batch=0, elapsed_s=1, extraction=None, page="x")
    env.runner.capture_once()
    env.runner.read_once("t")
    assert env.store.page_row(job, "A_00001")["status"] == "pronta"
    env.runner.read_once("t")
    assert env.store.page_row(job, "A_00001")["status"] == "erro"
    assert env.store.retry_errors(job) == 1


def test_captura_que_falha_vira_erro(env):
    job = env.store.add_job("A", "opus55-full", 1, 1, nome="A")
    env.runner.capturer = lambda ids: None
    env.runner.capture_once()
    assert env.store.page_row(job, "A_00001")["status"] == "pendente"
    env.runner.capture_once()
    assert env.store.page_row(job, "A_00001")["status"] == "erro"


def test_estimativa_e_calibracao_da_cota(env):
    env.store.add_job("A", "opus55-full", 1, 4, nome="A")
    env.store.set(prefetch=10, tokens_por_pct=1000)
    env.store.add_usage_mark(70)
    env.runner.capture_once()
    env.runner.read_once("t"); env.runner.read_once("t")  # 2000 tokens
    est = env.store.usage_estimate()
    assert est["pct"] == 72.0 and est["calibrated"] is None
    env.store.add_usage_mark(74)  # subiu 4 com 2000 tokens -> 500 por 1%
    est = env.store.usage_estimate()
    assert est["calibrated"] == 500 and est["pct"] == 74.0


def test_orcamento_pausa_antes_de_ler(env):
    env.store.add_job("A", "opus55-full", 1, 2, nome="A")
    env.store.set(limite_pct=80)
    env.store.add_usage_mark(81)
    assert env.runner._over_budget()
    assert env.store.settings()["paused"]


def test_recover_devolve_paginas_presas(env):
    job = env.store.add_job("A", "opus55-full", 1, 2, nome="A")
    env.store.mark(job, 1, "lendo")
    env.store.mark(job, 2, "baixando")
    env.store.recover()
    assert env.store.page_row(job, "A_00001")["status"] == "pronta"
    assert env.store.page_row(job, "A_00002")["status"] == "pendente"


def test_apaga_so_imagem_baixada_pelo_painel(env):
    (env.images / "A").mkdir(parents=True)
    (env.images / "A" / "A_00001.jpg").write_bytes(b"golden")
    env.store.add_job("A", "opus55-full", 1, 2, nome="A", page_exists=worker_mod.image_exists)
    env.store.set(apagar_imagens=True, prefetch=10)
    env.runner.capture_once()
    env.runner.read_once("t"); env.runner.read_once("t")
    assert (env.images / "A" / "A_00001.jpg").exists()
    assert not (env.images / "A" / "A_00002.jpg").exists()


def test_fatal_reason():
    assert "login" in fatal_reason("Login expired · Run /login").lower()
    assert fatal_reason("JSON fora do schema") is None
    # sem o CLI instalado (VPS nova) pausa em vez de queimar as tentativas de cada página
    assert "não está instalado" in fatal_reason("FileNotFoundError: [Errno 2] No such file or directory: 'claude'")
    assert "DEEPSEEK_API_KEY" in fatal_reason("RuntimeError: Defina DEEPSEEK_API_KEY no ambiente")


def test_api_do_painel(env):
    from fastapi.testclient import TestClient
    from src.pipeline.server import create_app

    app = create_app(env.store, env.runner, start_runner=False)
    c = TestClient(app)
    assert "Scriptorium" in c.get("/").text
    assert c.get("/static/app.js").status_code == 200
    r = c.post("/api/jobs", json={"bib": "029033_04", "config": "opus55-full", "first_page": 1, "last_page": 2})
    assert r.status_code == 200, r.text
    assert c.post("/api/jobs", json={"bib": "029033_04", "config": "nao-existe", "first_page": 1, "last_page": 2}).status_code == 400
    assert c.post("/api/jobs", json={"bib": "029033_04", "config": "opus55-full", "first_page": 1, "last_page": 99_999_999}).status_code == 400
    assert c.post("/api/control", json={"action": "pause"}).json()["ok"]
    assert c.post("/api/settings", json={"workers": 3}).json()["workers"] == 3
    assert c.post("/api/settings", json={"paused": False}).status_code == 400
    assert c.post("/api/usage", json={"pct": 74}).status_code == 200
    state = c.get("/api/state").json()
    assert state["settings"]["paused"] and state["jobs"][0]["total"] == 2 and state["usage"]["pct"] == 74
    acervos = c.get("/api/acervos").json()
    assert any(a["bib"] == "029033_04" for a in acervos)
    assert c.get("/api/image/..%2Fx").status_code in (400, 404)
    assert any(cfg["name"] == "dsflash-bands6" for cfg in c.get("/api/configs").json())


def test_qualidade_revisao_e_fita(env):
    def reader(image, config, backend):
        r = ok_result(image.stem)
        r.extraction = {"segments": [
            {"type": "obito", "text": "Fallecido [Joa]o, [?] [ilegível] [da] [as]", "confidence": 0.5,
             "people": [{"name": "João"}], "tiles": [0]},
            {"type": "anuncio", "text": "Vende-se", "confidence": 0.9, "people": []},
        ]}
        r.to_dict = lambda: {"page": image.stem, "extraction": r.extraction,
                             "tile_manifest": [{"box": [0, 0, 10, 10]}, {"box": [10, 0, 20, 10]}]}
        return r

    env.runner.reader = reader
    job = env.store.add_job("A", "opus55-full", 1, 3, nome="A")
    env.store.set(prefetch=10)
    env.runner.capture_once()
    env.runner.read_once("t")
    row = env.store.page_row(job, "A_00001")
    assert row["uncertain"] == 5 and row["people"] == 1 and abs(row["confidence"] - 0.7) < 1e-9
    import json as _json
    saved = _json.loads(Path(row["out_path"]).read_text())
    assert saved["tile_boxes"] == [[0, 0, 10, 10], [10, 0, 20, 10]] and "tile_manifest" not in saved
    assert env.store.job_pages(job)["codes"] == "rpp"
    assert [r["page_id"] for r in env.store.review_queue()] == ["A_00001"]
    env.store.set_review(job, "A_00001", "aprovada")
    assert env.store.review_queue() == [] and env.store.job_pages(job)["codes"] == "opp"
    assert env.store.quality()["aprovadas"] == 1

    from fastapi.testclient import TestClient
    from src.pipeline.server import create_app

    c = TestClient(create_app(env.store, env.runner, start_runner=False))
    d = c.get(f"/api/jobs/{job}").json()
    assert d["ribbon"]["first"] == 1 and d["rows"][0]["page_id"] == "A_00001"
    p = c.get(f"/api/page/{job}/A_00001").json()
    assert p["meta"]["tile_boxes"] and p["neighbors"] == {"prev": None, "next": None}
    assert c.post(f"/api/page/{job}/A_00001/review", json={"value": "revisar"}).status_code == 200
    assert c.post(f"/api/page/{job}/A_00001/review", json={"value": "x"}).status_code == 400
    assert len(c.get("/api/review").json()) == 1
    assert len(c.get("/api/timeline?hours=6").json()) == 36


def test_migra_banco_antigo(tmp_path):
    import sqlite3
    db = tmp_path / "old.sqlite"
    con = sqlite3.connect(db)
    from src.pipeline.store import SCHEMA

    con.executescript(SCHEMA)  # formato da primeira versão, sem as colunas novas
    con.close()
    Store(db)
    cols = {r[1] for r in sqlite3.connect(db).execute("PRAGMA table_info(pages)")}
    assert {"people", "uncertain", "confidence", "review"} <= cols


def test_login_protege_o_painel(env):
    from fastapi.testclient import TestClient
    from src.pipeline.server import create_app

    c = TestClient(create_app(env.store, env.runner, start_runner=False, password="segredo-forte"))
    assert c.get("/api/state").status_code == 401
    assert c.post("/api/control", json={"action": "resume"}).status_code == 401
    assert c.get("/", follow_redirects=False).status_code == 303
    assert c.get("/static/app.js", follow_redirects=False).status_code == 303
    assert c.post("/login", data={"senha": "errada"}).status_code == 401
    r = c.post("/login", data={"senha": "segredo-forte"}, follow_redirects=False)
    assert r.status_code == 303 and "painel_sessao" in r.cookies
    assert c.get("/api/state").status_code == 200
    c.cookies.set("painel_sessao", "9999999999.falso")
    assert c.get("/api/state").status_code == 401


def test_pagina_curta_ou_incerta_vai_para_revisao():
    from src.pipeline.worker import needs_review

    assert needs_review({"chars": 40, "confidence": 0.9})  # leitura truncada
    assert needs_review({"chars": 5000, "confidence": 0.3})
    assert not needs_review({"chars": 5000, "confidence": 0.9})
    assert not needs_review({"chars": 5000, "confidence": None})
