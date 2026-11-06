"""Comandos da leitura visão-primeiro (Claude)."""

from __future__ import annotations

from pathlib import Path

import click

from src.cli.context import console

CONFIG_NAMES = "opus-full, opus-structure, sonnet-full, sonnet-structure, haiku-full, haiku-structure"


def register_vision_commands(cli) -> None:
    @cli.command("ocr-local")
    @click.option("--image", required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @click.option("--scale", default=0.5, type=click.FloatRange(0.1, 1), show_default=True)
    @click.option("--review", is_flag=True, help="Compara três escalas e gera HTML com divergências e coordenadas")
    @click.option("--correct", is_flag=True, help="Correção experimental por consenso de releituras das linhas; guarda texto original")
    @click.option("--refine", is_flag=True, help="Com --correct: segunda votação com segmentação alternativa e suavização de traços")
    @click.option("--out", required=True, type=click.Path(path_type=Path))
    def ocr_local(image, scale, review, correct, refine, out):
        """Transcreve imagem local em português, sem API; preserva o OCR existente."""
        import json
        from src.vision.local_ocr import transcribe
        if refine and not correct:
            raise click.UsageError("--refine exige --correct.")
        if review and correct:
            raise click.UsageError("Use --review ou --correct; --correct já inclui relatório visual.")
        out.mkdir(parents=True, exist_ok=True)
        targets = [out / f"{image.stem}.txt", out / f"{image.stem}.json"]
        if review or correct:
            targets.append(out / f"{image.stem}.html")
        if correct:
            targets.append(out / f"{image.stem}.raw.txt")
        if any(p.exists() for p in targets):
            raise click.ClickException("Saída já existe; escolha outro diretório para preservar o resultado.")
        if correct:
            from src.vision.ocr_repair import transcribe_repaired, repair_review
            def progress(done, total):
                if done % 20 == 0 or done == total:
                    console.print(f"Releitura de linhas: {done}/{total}")
            result = transcribe_repaired(image, scale=scale, progress=progress, refine=refine)
            targets[2].write_text(repair_review(result, image), encoding="utf-8")
            targets[3].write_text(result["raw_text"], encoding="utf-8")
        elif review:
            from src.vision.ocr_consensus import transcribe_review, review_html
            result = transcribe_review(image, scale=scale)
            targets[2].write_text(review_html(result, image), encoding="utf-8")
        else:
            result = transcribe(image, scale=scale)
        targets[0].write_text(result["extraction"]["segments"][0]["text"], encoding="utf-8")
        targets[1].write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        console.print(f"OCR local salvo em {out} · escala efetiva {result['scale']}")

    @cli.command("benchmark-visao")
    @click.option("--page", "pages", multiple=True, help="ID (029033_01_00044) ou caminho de imagem; repetível")
    @click.option("--golden", is_flag=True, help="Inclui as páginas douradas de src/vision/golden.json")
    @click.option("--config", "configs", multiple=True, help=f"Configurações ({CONFIG_NAMES}); repetível; padrão: todas")
    @click.option("--grid", default=None, help="Grade de recortes CxR (padrão 4x3)")
    @click.option(
        "--backend",
        type=click.Choice(["api", "claude-code"]),  # configs DeepSeek (ds*) usam o próprio backend
        default="api",
        show_default=True,
        help="api = SDK com créditos da API; claude-code = `claude -p` headless pela assinatura",
    )
    @click.option("--tiling", type=click.Choice(["grid", "layout"]), default="grid", show_default=True)
    @click.option("--effort", type=click.Choice(["low", "medium", "high"]), default=None, help="Mesmo esforço para todas as configurações selecionadas (exceto Haiku)")
    @click.option("--preview", is_flag=True, help="Prévia local dos recortes; não chama API nem Claude Code")
    @click.option("--dry-run", is_flag=True, help="Só conta tokens de entrada (grátis), sem chamar o modelo")
    @click.option("--extrapolar", default=150000, show_default=True, help="Páginas para extrapolar o custo")
    @click.option("--out", default=None, help="Diretório de saída (padrão data/benchmarks/visao/<timestamp>)")
    def benchmark_visao(pages, golden, configs, grid, backend, tiling, effort, preview, dry_run, extrapolar, out):
        """Benchmark de leitura de páginas por modelo de visão: tokens, custo e qualidade."""
        from src.vision.benchmark import load_golden, run_benchmark
        from src.vision.tiles import parse_grid

        page_list = list(pages)
        if golden:
            page_list += [page for page in load_golden() if page not in page_list]
        if not page_list:
            raise click.UsageError("Informe --page e/ou --golden")

        try:
            grid_tuple = parse_grid(grid) if grid else None
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
        if grid and tiling == "layout":
            raise click.UsageError("--grid só se aplica a --tiling grid")
        if preview:
            if dry_run:
                raise click.UsageError("Use --preview ou --dry-run, não ambos")
            from datetime import datetime
            from src.vision.benchmark import resolve_page, BENCHMARK_DIR
            from src.vision.preview import preview_page
            output = Path(out) if out else BENCHMARK_DIR / ("preview_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
            missing = []
            for page in page_list:
                resolved = resolve_page(page)
                if resolved["image"] is None:
                    missing.append(page)
                    continue
                result = preview_page(resolved["image"], output / resolved["id"], grid=grid_tuple or (4, 3), strategy=tiling)
                console.print(f"{resolved['id']}: {len(result['tiles'])} imagens; ~{result['image_tokens_estimate']:,} tokens de imagem "
                              f"(grade: ~{result['grid_image_tokens_estimate']:,}). Prévia em {output / resolved['id']}")
            if missing:
                raise click.ClickException("Sem imagem local: " + ", ".join(missing))
            return
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
            tiling=tiling,
            effort=effort,
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
                "  Capture os IDs acima explicitamente. --max-pages limita a captura; não seleciona o manifesto golden."
            )
        errors = [str(r.get("error") or "") for r in summary["results"]]
        if any("AuthenticationError" in err or "authenticate" in err or "OAuth" in err for err in errors):
            console.print(
                "\n[red]Credencial Anthropic inválida ou ausente.[/red] "
                + ("Execute `claude auth login` para renovar o acesso da assinatura." if backend == "claude-code"
                   else "Defina ANTHROPIC_API_KEY em .env ou faça `ant auth login`, e rode de novo.")
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
