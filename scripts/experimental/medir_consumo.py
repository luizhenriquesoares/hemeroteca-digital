"""Medidor de consumo por página e projeção para o acervo.

Lê os JSONs por página de uma ou mais rodadas do benchmark-visao e mostra, por página:
caracteres transcritos, tokens (entrada, saída, raciocínio), tokens reais gastos no Claude Code
(cache incluso, quando backend=claude-code), custo API equivalente, tempo e densidade.
Grava tudo em data/benchmarks/consumo.csv (acumula entre rodadas, sem duplicar).

Projeção para o acervo:
  --paginas 150000      páginas a processar (padrão 150 mil, século XIX)
  --paralelo 4          leituras simultâneas (só afeta o tempo de relógio)
  --pct-semana 3.5      % do limite semanal da assinatura que ESTAS páginas consumiram
                        (veja /usage no Claude Code antes e depois da rodada)

Uso: python scripts/experimental/medir_consumo.py data/benchmarks/visao/opus55_ref3 [--pct-semana 3.5]
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path

LEDGER = Path("data/benchmarks/consumo.csv")
FIELDS = ["rodada", "pagina", "config", "modelo", "backend", "chars", "segmentos", "tok_in", "tok_out",
          "tok_raciocinio", "tok_cli_total", "usd_api", "usd_api_batch", "usd_cli_lista", "segundos",
          "tok_out_por_1k_chars", "erro"]


def row_from(path: Path) -> dict:
    r = json.loads(path.read_text())
    u = r.get("usage") or {}
    cli = r.get("usage_cli") or {}
    q = r.get("quality") or {}
    chars = q.get("chars") or sum(len(s.get("text", "")) for s in (r.get("extraction") or {}).get("segments", []))
    cli_total = sum(cli.get(k, 0) for k in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"))
    details = r.get("details") or {}
    usd = details.get("cost_usd_no_cache", r.get("cost_usd"))
    return {
        "rodada": path.parent.name, "pagina": r.get("page"), "config": r.get("config"), "modelo": r.get("model"),
        "backend": r.get("backend", "api"), "chars": chars, "segmentos": q.get("segments") or len((r.get("extraction") or {}).get("segments", [])),
        "tok_in": u.get("prompt_tokens_total") or u.get("input_tokens", 0), "tok_out": u.get("output_tokens", 0),
        "tok_raciocinio": u.get("thinking_tokens", 0), "tok_cli_total": cli_total or "",
        "usd_api": round(usd or 0, 4), "usd_api_batch": round((usd or 0) / 2, 4),
        "usd_cli_lista": round(r.get("cost_usd_reported") or 0, 4) or "", "segundos": r.get("elapsed_s", 0),
        "tok_out_por_1k_chars": round(u.get("output_tokens", 0) / max(1, chars) * 1000, 1),
        "erro": (r.get("error") or "")[:80],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("rodadas", nargs="+", type=Path)
    ap.add_argument("--paginas", type=int, default=150_000)
    ap.add_argument("--paralelo", type=int, default=4)
    ap.add_argument("--pct-semana", type=float, default=None)
    a = ap.parse_args()

    rows = [row_from(p) for d in a.rodadas for p in sorted(d.glob("*__*.json"))]
    ok = [r for r in rows if not r["erro"]]
    print(f"{'página':18} {'config':16} {'chars':>7} {'tok in':>7} {'tok out':>8} {'US$ API':>8} {'seg':>5}")
    for r in rows:
        flag = f"  ERRO {r['erro']}" if r["erro"] else ""
        print(f"{r['pagina']:18} {r['config']:16} {r['chars']:>7} {r['tok_in']:>7} {r['tok_out']:>8} {r['usd_api']:>8.4f} {r['segundos']:>5.0f}{flag}")

    # Livro-razão acumulado (substitui linhas da mesma rodada/página/config).
    existing = []
    if LEDGER.exists():
        with LEDGER.open() as f:
            existing = list(csv.DictReader(f))
    keys = {(r["rodada"], r["pagina"], r["config"]) for r in rows}
    merged = [r for r in existing if (r["rodada"], r["pagina"], r["config"]) not in keys] + rows
    with LEDGER.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(merged)

    for config in sorted({r["config"] for r in ok}):
        rs = [r for r in ok if r["config"] == config]
        n = len(rs)
        mean = lambda k: statistics.fmean(float(r[k] or 0) for r in rs)
        secs = mean("segundos")
        dias = a.paginas * secs / a.paralelo / 86400
        print(f"\n== {config} ({n} páginas)")
        print(f"   média por página: {mean('chars'):,.0f} chars, {mean('tok_out'):,.0f} tokens de saída, US$ {mean('usd_api'):.4f} API (US$ {mean('usd_api_batch'):.4f} batch), {secs:.0f} s")
        print(f"   {a.paginas:,} páginas pela API: US$ {mean('usd_api') * a.paginas:,.0f} (batch US$ {mean('usd_api_batch') * a.paginas:,.0f})")
        print(f"   tempo de relógio com {a.paralelo} em paralelo, sem limite de uso: {dias:,.0f} dias")
        if a.pct_semana and all(r['backend'] == 'claude-code' for r in rs):
            por_semana = n / (a.pct_semana / 100)
            semanas = a.paginas / por_semana
            print(f"   assinatura: {a.pct_semana}% da semana para {n} páginas → ~{por_semana:,.0f} páginas/semana → {semanas:,.0f} semanas ({semanas * 7:,.0f} dias)")
    print(f"\nRegistro acumulado em {LEDGER}")


if __name__ == "__main__":
    main()
