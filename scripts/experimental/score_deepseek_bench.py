"""Pontua uma rodada do benchmark-visao contra os trechos de referência (CER/WER).

Uso: python scripts/experimental/score_deepseek_bench.py <dir_da_rodada> [<outra_rodada> ...]
Normaliza apóstrofos tipográficos além do NFC/espaços do src.vision.evaluation.
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.vision import evaluation as ev

_orig = ev.normalize
ev.normalize = lambda t: _orig(t.replace("’", "'").replace("‘", "'"))
ref = json.loads(Path("data/benchmarks/deepseek_ref/referencia_trechos.json").read_text())
rows = {}
for run in sys.argv[1:]:
    for f in sorted(Path(run).glob("*__*.json")):
        r = json.loads(f.read_text())
        s = ev.evaluate_result(ref, r)
        if "skipped" in s:
            continue
        agg = rows.setdefault(r["config"], {"ce": 0, "cn": 0, "we": 0, "wn": 0, "pages": 0, "cost": 0.0, "cost_op": 0.0})
        agg["pages"] += 1
        u = r.get("usage") or {}
        nocache = (r.get("details") or {}).get("cost_usd_no_cache")
        if nocache is None and u.get("prompt_tokens_total") is not None:
            pin, pout = {"deepseek-flash": (0.30, 1.20), "deepseek-v4-pro": (1.32, 3.96)}.get(r.get("model"), (0, 0))
            nocache = (u.get("prompt_tokens_total", 0) * pin + u.get("output_tokens", 0) * pout) / 1e6
        agg["cost"] += nocache if nocache is not None else (r.get("cost_usd") or 0)
        agg["cost_op"] += (nocache / 2) if nocache is not None else (r.get("cost_usd_batch") or 0)
        for c in s["checks"]:
            agg["ce"] += c["char_edits"]; agg["cn"] += c["reference_chars"]
            agg["we"] += c["word_edits"]; agg["wn"] += c["reference_words"]
# Linhas de base do pipeline atual: Tesseract bruto e correção LLM já salva (_corrigido.txt).
for page in sorted({e["page"] for e in ref["excerpts"]}):
    bib = page.rsplit("_", 1)[0]
    for label, name in (("tesseract (atual)", f"{page}.txt"), ("tesseract+correção salva", f"{page}_corrigido.txt")):
        f = Path("data/text") / bib / name
        if not f.exists():
            continue
        r = {"page": page, "config": label, "mode": "full", "extraction": {"segments": [{"text": f.read_text(errors="ignore")}]}}
        s = ev.evaluate_result(ref, r)
        agg = rows.setdefault(label, {"ce": 0, "cn": 0, "we": 0, "wn": 0, "pages": 0, "cost": 0.0, "cost_op": 0.0})
        agg["pages"] += 1
        for c in s["checks"]:
            agg["ce"] += c["char_edits"]; agg["cn"] += c["reference_chars"]
            agg["we"] += c["word_edits"]; agg["wn"] += c["reference_words"]
print("| config | págs c/ trecho | CER | WER | US$/pág (sem cache) | US$/pág (batch/fora do pico) |")
print("|---|---:|---:|---:|---:|---:|")
for k, a in sorted(rows.items(), key=lambda kv: kv[1]["ce"] / max(1, kv[1]["cn"])):
    print(f"| {k} | {a['pages']} | {a['ce']/a['cn']:.1%} | {a['we']/a['wn']:.1%} | {a['cost']/a['pages']:.4f} | {a['cost_op']/a['pages']:.4f} |")
