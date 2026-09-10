"""Comandos da leitura visão-primeiro (Claude)."""

from __future__ import annotations

from pathlib import Path

import click

from src.cli.context import console

CONFIG_NAMES = "opus-full, opus-structure, sonnet-full, sonnet-structure, haiku-full, haiku-structure"


def register_vision_commands(cli) -> None:
    @cli.command("benchmark-visao")
    @click.option("--page", "pages", multiple=True, help="ID (029033_01_00044) ou caminho de imagem; repetível")
    @click.option("--golden", is_flag=True, help="Inclui as páginas douradas de src/vision/golden.json")
    @click.option("--config", "configs", multiple=True, help=f"Configurações ({CONFIG_NAMES}); repetível; padrão: todas")
    @click.option("--grid", default=None, help="Grade de recortes CxR (padrão 4x3)")
    @click.option(
        "--backend",
        type=click.Choice(["api", "claude-code"]),
        default="api",
        show_default=True,
        help="api = SDK com créditos da API; claude-code = `claude -p` headless pela assinatura",
    )
    @click.option("--dry-run", is_flag=True, help="Só conta tokens de entrada (grátis), sem chamar o modelo")
    @click.option("--extrapolar", default=150000, show_default=True, help="Páginas para extrapolar o custo")
    @click.option("--out", default=None, help="Diretório de saída (padrão data/benchmarks/visao/<timestamp>)")
    def benchmark_visao(pages, golden, configs, grid, backend, dry_run, extrapolar, out):
        """Benchmark de leitura de páginas por modelo de visão: tokens, custo e qualidade."""
        from src.vision.benchmark import load_golden, run_benchmark
        from src.vision.tiles import parse_grid

        page_list = list(pages)
        if golden:
            page_list += [page for page in load_golden() if page not in page_list]
        if not page_list:
            raise click.UsageError("Informe --page e/ou --golden")

        grid_tuple = parse_grid(grid) if grid else None
        if dry_run and backend == "claude-code":
            raise click.UsageError("--dry-run só faz sentido com --backend api (contagem oficial de tokens)")
        modo = "dry-run (só tokens de entrada)" if dry_run else "chamadas reais (gera custo)"
        if backend == "claude-code":
            modo = "Claude Code headless pela assinatura (US$/pág = estimativa API equivalente)"
        console.print(f"\n[bold]Benchmark visão-primeiro[/bold] — {len(page_list)} páginas, {modo}\n")

        summary = run_benchmark(
            page_list,
            list(configs) or None,
            grid=grid_tuple,
            backend=backend,
            out_dir=Path(out) if out else None,
            dry_run=dry_run,
            extrapolate_pages=extrapolar,
            progress=lambda msg: console.print(f"  {msg}"),
        )

        if summary["missing_images"]:
            console.print(
                f"\n[yellow]{len(summary['missing_images'])} páginas sem imagem hi-res local:[/yellow] "
                + ", ".join(summary["missing_images"])
            )
            console.print(
                "  Recapture com: python main.py ocr-hires --bib <bib> --keep-images "
                "(mantém o JPG em data/images/<bib>/)"
            )
        errors = [str(r.get("error") or "") for r in summary["results"]]
        if any("AuthenticationError" in err for err in errors):
            console.print(
                "\n[red]Credencial Anthropic inválida ou ausente.[/red] "
                "Defina ANTHROPIC_API_KEY em .env ou faça `ant auth login`, e rode de novo."
            )
        if any("credit balance" in err for err in errors):
            console.print(
                "\n[red]A organização está sem créditos de API.[/red] "
                "Compre créditos em https://console.anthropic.com/settings/billing "
                "(Plans & Billing). Assinatura do Claude não vale para a API."
            )
        if summary["by_config"]:
            console.print("\n" + summary["table"])
        console.print(f"\nArtefatos em {summary['out_dir']}")
