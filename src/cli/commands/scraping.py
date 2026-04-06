"""Comandos de scraping e pipelines."""

from __future__ import annotations

import click

from src.cli.context import console, logger
from src.cli.helpers import load_json, save_json


def register_scraping_commands(cli) -> None:
    @cli.command()
    @click.option("--headless/--no-headless", default=False,
                  help="Modo headless do Chrome (default: visual, para resolver CAPTCHA)")
    def listar(headless):
        """Lista todos os acervos (jornais) disponíveis de Pernambuco."""
        from src.scraping.acervos import buscar_acervos

        console.print("\n[bold]Buscando acervos de PE na Hemeroteca Digital...[/bold]")
        console.print("[yellow]NOTA: Se aparecer CAPTCHA, resolva manualmente no navegador.[/yellow]\n")

        acervos = buscar_acervos(headless=headless)

        from rich.table import Table

        table = Table(title=f"Acervos de PE ({len(acervos)} jornais)")
        table.add_column("#", style="dim", width=5)
        table.add_column("Código", style="cyan", width=10)
        table.add_column("Jornal", style="green")

        for index, acervo in enumerate(acervos, 1):
            table.add_row(str(index), acervo["bib"], acervo["nome"])

        console.print(table)

    @cli.command("limpar-cache")
    def limpar_cache():
        """Remove cache de acervos para forçar nova busca."""
        from src.scraping.acervos import limpar_cache as clear_cache

        clear_cache()
        console.print("[green]Cache limpo.[/green]")

    @cli.command()
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--max-pages", default=0, help="Limite de páginas por jornal (0 = sem limite)")
    @click.option("--headless/--no-headless", default=False, help="Modo headless do Chrome (default: visual para CAPTCHA)")
    def capturar(bib, max_pages, headless):
        """Captura páginas dos jornais (scraping + download de imagens)."""
        from src.scraping.acervos import buscar_acervos
        from src.scraping.driver import create_driver
        from src.scraping.scraper import scrape_jornal

        driver = create_driver(headless=headless)

        try:
            acervos = [{"bib": bib, "nome": f"Acervo {bib}"}] if bib else buscar_acervos(driver=driver, headless=headless)
            console.print(f"\n[bold]Capturando {len(acervos)} acervos...[/bold]\n")

            total_paginas = 0
            for index, acervo in enumerate(acervos, 1):
                console.print(
                    f"\n[cyan][{index}/{len(acervos)}][/cyan] "
                    f"Jornal: [bold]{acervo['nome']}[/bold] ({acervo['bib']})"
                )
                try:
                    resultados = scrape_jornal(driver, acervo["bib"], acervo["nome"], max_pages=max_pages)
                    total_paginas += len(resultados)
                    console.print(f"  -> {len(resultados)} páginas capturadas")
                except Exception as exc:
                    console.print(f"  [red]ERRO: {exc}[/red]")
                    logger.error("Erro no acervo %s: %s", acervo["bib"], exc, exc_info=True)

            console.print(f"\n[bold green]Total: {total_paginas} páginas capturadas[/bold green]")
        finally:
            driver.quit()

    @cli.command("ocr-hires")
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--force", is_flag=True, help="Reprocessar páginas já convertidas")
    @click.option("--headless/--no-headless", default=True, help="Modo headless do Chrome (default: headless)")
    @click.option("--workers", default=4, help="Número de Chrome drivers paralelos (default: 4)")
    @click.option("--max-pages", default=0, help="Limite de páginas por acervo (0 = todas)")
    @click.option("--keep-images", is_flag=True, help="Salvar imagens hi-res em data/images/ (para validação)")
    def ocr_hires(bib, force, headless, workers, max_pages, keep_images):
        """OCR em alta resolução máxima do DocReader."""
        from src.scraping.hires_pipeline import HIRES_SIZE, processar_todos_hires

        console.print(f"\n[bold]OCR Hi-Res MÁXIMO: {HIRES_SIZE}[/bold]")
        if keep_images:
            console.print("[yellow]--keep-images ativo: imagens serão salvas em data/images/[/yellow]")
        console.print(f"[dim]Workers: {workers} | max_pages: {max_pages or 'todas'}[/dim]\n")

        stats = processar_todos_hires(
            headless=headless,
            force=force,
            bib=bib,
            workers=workers,
            max_pages=max_pages,
            keep_images=keep_images,
        )
        total = sum(stats.values())
        console.print(f"\n[bold green]OCR hi-res concluído: {total} páginas em {len(stats)} acervos[/bold green]")

    @cli.command()
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--max-pages", default=0, help="Limite de páginas por jornal")
    @click.option("--headless/--no-headless", default=False, help="Modo headless do Chrome (default: visual para CAPTCHA)")
    def pipeline(bib, max_pages, headless):
        """Executa o pipeline completo: captura -> OCR -> chunking -> indexação."""
        from src.config import CACHE_DIR
        from src.processing.chunker import criar_chunks_acervo
        from src.processing.indexer import indexar_acervo
        from src.processing.ocr import processar_acervo
        from src.scraping.acervos import buscar_acervos
        from src.scraping.driver import create_driver
        from src.scraping.scraper import scrape_jornal

        progress_file = CACHE_DIR / "pipeline_progress.json"
        progress = load_json(progress_file, {"done": [], "failed": []})
        done_set = set(progress["done"])

        console.print("\n[bold]Pipeline completo (resumível)[/bold]\n")
        console.print("Etapas: Captura -> OCR -> Chunking -> Indexação\n")

        driver = create_driver(headless=headless)

        try:
            acervos = [{"bib": bib, "nome": f"Acervo {bib}"}] if bib else buscar_acervos(driver=driver, headless=headless)
            pendentes = [acervo for acervo in acervos if acervo["bib"] not in done_set]
            console.print(
                f"Total: {len(acervos)} acervos | "
                f"Concluídos: {len(done_set)} | "
                f"Pendentes: {len(pendentes)}\n"
            )

            for index, acervo in enumerate(acervos, 1):
                bib_code = acervo["bib"]
                nome = acervo["nome"]
                if bib_code in done_set:
                    continue

                console.print(f"\n[cyan]━━━ [{index}/{len(acervos)}] {nome} ({bib_code}) ━━━[/cyan]")

                try:
                    console.print("  [yellow]1/4[/yellow] Capturando páginas...")
                    try:
                        resultados = scrape_jornal(driver, bib_code, nome, max_pages=max_pages)
                        console.print(f"       {len(resultados)} páginas capturadas")
                    except Exception as exc:
                        console.print(f"       [red]ERRO na captura: {exc}[/red]")
                        logger.error("Erro captura %s: %s", bib_code, exc, exc_info=True)
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = create_driver(headless=headless)
                        progress["failed"].append(bib_code)
                        save_json(progress_file, progress)
                        continue

                    console.print("  [yellow]2/4[/yellow] Executando OCR...")
                    console.print(f"       {processar_acervo(bib_code)} páginas processadas")

                    console.print("  [yellow]3/4[/yellow] Criando chunks...")
                    console.print(f"       {criar_chunks_acervo(bib_code)} chunks criados")

                    console.print("  [yellow]4/4[/yellow] Indexando...")
                    console.print(f"       {indexar_acervo(bib_code)} chunks indexados")

                    progress["done"].append(bib_code)
                    done_set.add(bib_code)
                    save_json(progress_file, progress)
                    console.print(f"  [green]✓ {nome} concluído ({len(done_set)}/{len(acervos)})[/green]")
                except Exception as exc:
                    console.print(f"  [red]ERRO FATAL em {bib_code}: {exc}[/red]")
                    logger.error("Erro fatal %s: %s", bib_code, exc, exc_info=True)
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = create_driver(headless=headless)
                    progress["failed"].append(bib_code)
                    save_json(progress_file, progress)
        finally:
            try:
                driver.quit()
            except Exception:
                pass

        console.print(f"\n[bold green]Pipeline concluído! {len(done_set)} acervos processados.[/bold green]")

    @cli.command("pipeline-paralelo")
    @click.option("--workers", default=4, help="Número de Chrome drivers paralelos (default: 4)")
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--max-pages", default=0, help="Limite de páginas por jornal")
    @click.option("--headless/--no-headless", default=True, help="Modo headless (default: headless)")
    @click.option("--skip-indexing", is_flag=True, help="Pular indexação final no ChromaDB")
    @click.option("--capture-only", is_flag=True, help="Só capturar imagens (pular OCR/chunking para máxima velocidade)")
    def pipeline_paralelo(workers, bib, max_pages, headless, skip_indexing, capture_only):
        """Pipeline paralelo: N Chrome drivers processam acervos simultaneamente."""
        from src.config import CACHE_DIR
        from src.scraping.parallel import run_parallel_pipeline

        cache_file = CACHE_DIR / "acervos_pe.json"
        if bib:
            acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
        elif cache_file.exists():
            acervos = load_json(cache_file, [])
            console.print(f"[bold]{len(acervos)} acervos carregados do cache[/bold]")
        else:
            console.print("[yellow]Cache de acervos não encontrado. Execute 'listar' primeiro.[/yellow]")
            return

        run_parallel_pipeline(
            acervos,
            num_workers=workers,
            max_pages=max_pages,
            headless=headless,
            skip_indexing=skip_indexing,
            capture_only=capture_only,
        )
