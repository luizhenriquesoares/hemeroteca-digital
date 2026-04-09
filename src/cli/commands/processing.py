"""Comandos de processamento documental."""

from __future__ import annotations

from pathlib import Path

import click
from rich.table import Table

from src.cli.context import console
from src.cli.helpers import infer_saved_paths_from_image, run_parallel_file_jobs


def register_processing_commands(cli) -> None:
    @cli.command()
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--force", is_flag=True, help="Reprocessar imagens já convertidas")
    @click.option("--workers", default=0, help="Workers paralelos (0 = auto, cpu_count - 2)")
    def ocr(bib, force, workers):
        """Extrai texto das imagens usando Tesseract OCR."""
        from src.processing.ocr import processar_acervo, processar_todos_acervos

        console.print("\n[bold]Iniciando OCR...[/bold]\n")
        if bib:
            console.print(f"Acervo {bib}: {processar_acervo(bib, force=force)} páginas processadas")
            return

        stats = processar_todos_acervos(force=force, workers=workers)
        total = sum(stats.values())
        console.print(f"\n[bold green]OCR concluído: {total} páginas em {len(stats)} acervos[/bold green]")

    @cli.command("corrigir")
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option(
        "--provider",
        default="openai",
        type=click.Choice(["openai", "claude", "claude-cli", "claude-api"]),
    )
    @click.option("--model", default=None, help="Modelo do provider. Ex: gpt-4o-mini, opus, sonnet")
    @click.option("--force", is_flag=True, help="Reprocessar textos já corrigidos")
    @click.option("--workers", default=2, help="Processos paralelos (default: 2)")
    def corrigir(bib, provider, model, force, workers):
        """Corrige textos OCR usando OpenAI, Claude CLI ou Anthropic API."""
        from src.processing.correcao_provider import (
            CLAUDE_DEFAULT_MODEL,
            OPENAI_DEFAULT_MODEL,
            corrigir_arquivo,
            list_pending_files,
        )

        arquivos = list_pending_files(bib=bib, force=force)
        default_model = OPENAI_DEFAULT_MODEL if provider == "openai" else CLAUDE_DEFAULT_MODEL
        effective_model = model or default_model

        console.print(f"\n[bold]Correção {provider} / {effective_model} ({workers} workers)[/bold]")
        console.print(f"[dim]{len(arquivos)} arquivos pendentes[/dim]\n")

        if not arquivos:
            console.print("[green]Nada a fazer.[/green]")
            return

        processados, falhas = run_parallel_file_jobs(
            arquivos,
            lambda arquivo: corrigir_arquivo(arquivo, provider, effective_model, force),
            workers,
        )
        console.print(f"\n[bold green]Concluído: {processados} corrigidos, {falhas} falhas[/bold green]")

    @cli.command("corrigir-claude")
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--model", default="opus", type=click.Choice(["opus", "sonnet", "haiku"]))
    @click.option("--force", is_flag=True, help="Reprocessar textos já corrigidos")
    @click.option("--workers", default=2, help="Processos paralelos (default: 2)")
    def corrigir_claude_legacy(bib, model, force, workers):
        """Compatibilidade legada: encaminha para correção via Claude."""
        from src.processing.correcao_provider import corrigir_arquivo, list_pending_files

        arquivos = list_pending_files(bib=bib, force=force)
        console.print(f"\n[bold]Correção claude / {model} ({workers} workers)[/bold]")
        console.print(f"[dim]{len(arquivos)} arquivos pendentes[/dim]\n")

        if not arquivos:
            console.print("[green]Nada a fazer.[/green]")
            return

        processados, falhas = run_parallel_file_jobs(
            arquivos,
            lambda arquivo: corrigir_arquivo(arquivo, "claude-cli", model, force),
            workers,
        )
        console.print(f"\n[bold green]Concluído: {processados} corrigidos, {falhas} falhas[/bold green]")

    @cli.command("benchmark-correcao")
    @click.argument("txt_path", type=click.Path(exists=True, path_type=str))
    @click.option("--sample-chars", default=2000, help="Limite de caracteres para benchmark rápido (0 = arquivo inteiro)")
    @click.option("--openai-mini-model", default="gpt-4o-mini", help="Modelo OpenAI mini")
    @click.option("--openai-max-model", default="gpt-5", help="Modelo OpenAI mais forte")
    @click.option("--claude-model", default="opus", help="Modelo Claude")
    def benchmark_correcao(txt_path, sample_chars, openai_mini_model, openai_max_model, claude_model):
        """Compara correção do mesmo OCR entre OpenAI mini, OpenAI max e Claude."""
        from src.benchmarks.correcao import run_benchmark

        runs = [
            ("openai-mini", "openai", openai_mini_model),
            ("openai-max", "openai", openai_max_model),
            ("claude", "claude", claude_model),
        ]
        summary = run_benchmark(Path(txt_path), runs=runs, sample_chars=sample_chars)

        table = Table(title="Benchmark de Correção OCR")
        table.add_column("Label", style="cyan")
        table.add_column("Modelo", style="green")
        table.add_column("OK", style="yellow")
        table.add_column("Tempo(s)", justify="right")
        table.add_column("Mudança", justify="right")
        table.add_column("Palavras válidas", justify="right")

        for result in summary["results"]:
            table.add_row(
                result["label"],
                result["model"],
                "sim" if result["ok"] else "não",
                str(result["elapsed_sec"]),
                f'{result["changed_ratio"]:.2%}',
                f'{result["real_word_ratio"]:.2%}',
            )

        console.print()
        console.print(table)
        console.print(f"\n[dim]Saídas salvas em: {summary['out_dir']}[/dim]")

    @cli.command("benchmark-ocr")
    @click.argument("image_path", type=click.Path(exists=True, path_type=str))
    @click.option("--saved-ocr-path", default=None, type=click.Path(path_type=str), help="TXT OCR já salvo para comparar")
    @click.option("--corrected-path", default=None, type=click.Path(path_type=str), help="TXT corrigido para referência")
    def benchmark_ocr(image_path, saved_ocr_path, corrected_path):
        """Compara OCR salvo, OCR adaptativo novo e texto corrigido."""
        from src.benchmarks.ocr import run_benchmark

        image = Path(image_path)
        saved = Path(saved_ocr_path) if saved_ocr_path else None
        corrected = Path(corrected_path) if corrected_path else None

        if saved is None and corrected is None:
            saved, corrected = infer_saved_paths_from_image(image)
        elif saved is None:
            saved, _ = infer_saved_paths_from_image(image)
        elif corrected is None:
            _, corrected = infer_saved_paths_from_image(image)

        summary = run_benchmark(image, saved_ocr_path=saved, corrected_path=corrected)

        table = Table(title="Benchmark de OCR Histórico")
        table.add_column("Label", style="cyan")
        table.add_column("OK", style="yellow")
        table.add_column("Tempo(s)", justify="right")
        table.add_column("Score", justify="right")
        table.add_column("Palavras válidas", justify="right")
        table.add_column("Ruído", justify="right")
        table.add_column("Similar ao corrigido", justify="right")
        table.add_column("Variante", style="green")

        for result in summary["results"]:
            similarity = result["similarity_to_corrected"]
            table.add_row(
                result["label"],
                "sim" if result["ok"] else "não",
                str(result["elapsed_sec"]),
                f'{result["score"]:.4f}',
                f'{result["valid_word_ratio"]:.2%}',
                f'{result["odd_char_ratio"]:.2%}',
                f'{similarity:.2%}' if similarity is not None else "-",
                result.get("variant", ""),
            )

        console.print()
        console.print(table)
        console.print(f"\n[dim]Saídas salvas em: {summary['out_dir']}[/dim]")

    @cli.command("benchmark-search")
    @click.argument("cases_path", required=False, type=click.Path(exists=True, path_type=str))
    @click.option(
        "--write-template",
        default=None,
        type=click.Path(path_type=str),
        help="Escrever um template JSON de casos de benchmark e sair",
    )
    @click.option(
        "--mode",
        "modes",
        multiple=True,
        type=click.Choice(["textual", "semantica", "hibrida"]),
        help="Modo(s) de busca a comparar. Sem informar, roda todos.",
    )
    @click.option("--n-results", default=10, show_default=True, help="Top-k avaliado por query")
    @click.option("--bib", default=None, help="Filtrar benchmark para um acervo específico")
    def benchmark_search(cases_path, write_template, modes, n_results, bib):
        """Avalia recuperação textual, semântica e híbrida com um gabarito de queries."""
        from src.benchmarks.search import run_benchmark, select_runs, write_template as write_search_template

        if write_template:
            target = write_search_template(Path(write_template))
            console.print(f"\n[bold green]Template salvo em:[/bold green] {target}")
            if not cases_path:
                return

        if not cases_path:
            raise click.UsageError("Informe CASES_PATH ou use --write-template para gerar um modelo")

        selected_runs = select_runs(list(modes)) if modes else None
        summary = run_benchmark(
            Path(cases_path),
            runs=selected_runs,
            n_results=n_results,
            filtro_bib=bib,
        )

        table = Table(title="Benchmark de Busca/RAG")
        table.add_column("Run", style="cyan")
        table.add_column("Modo", style="green")
        table.add_column("Queries", justify="right")
        table.add_column(f"Hit@{n_results}", justify="right")
        table.add_column(f"Recall@{n_results}", justify="right")
        table.add_column("MRR", justify="right")
        table.add_column(f"nDCG@{n_results}", justify="right")
        table.add_column("Tempo(ms)", justify="right")
        table.add_column("Erros", justify="right")

        for run in summary["runs"]:
            table.add_row(
                run["label"],
                run["mode"],
                str(run["queries"]),
                f'{run[f"hit_rate_at_{n_results}"]:.2%}',
                f'{run[f"mean_recall_at_{n_results}"]:.2%}',
                f'{run["mrr"]:.4f}',
                f'{run[f"ndcg_at_{n_results}"]:.4f}',
                f'{run["avg_elapsed_ms"]:.2f}',
                str(run["errors"]),
            )

        console.print()
        console.print(table)
        console.print(f"\n[dim]Saídas salvas em: {summary['out_dir']}[/dim]")

    @cli.command()
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--force", is_flag=True, help="Recriar chunks existentes")
    @click.option("--reset", is_flag=True, help="Apagar os chunks atuais do acervo antes de recriar")
    def chunkar(bib, force, reset):
        """Divide textos em chunks para indexação RAG."""
        from src.processing.chunker import (
            criar_chunks_acervo,
            criar_chunks_todos,
            limpar_chunks_acervo,
        )

        console.print("\n[bold]Criando chunks...[/bold]\n")
        if reset and not bib:
            raise click.UsageError("--reset exige --bib para evitar limpeza global acidental")

        if bib:
            if reset:
                removed = limpar_chunks_acervo(bib)
                console.print(
                    f"Acervo {bib}: {removed['chunks_removed']} chunks antigos removidos "
                    f"em {removed['files_removed']} arquivo(s)"
                )
            total = criar_chunks_acervo(bib, force=force or reset)
            console.print(f"Acervo {bib}: {total} chunks criados")
            return

        stats = criar_chunks_todos(force=force)
        total = sum(stats.values())
        console.print(f"\n[bold green]Chunking concluído: {total} chunks em {len(stats)} acervos[/bold green]")

    @cli.command("enriquecer-metadados")
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--headless/--no-headless", default=True, help="Modo headless do Chrome")
    def enriquecer_metadados(bib, headless):
        """Atualiza apenas os arquivos .json das páginas que já têm .txt."""
        from src.processing.metadata_enrichment import enrich_metadata

        console.print("\n[bold]Enriquecendo metadados sem refazer OCR...[/bold]\n")
        results = enrich_metadata(headless=headless, bib=bib)

        total_updated = sum(item["updated"] for item in results.values())
        total_pages = sum(item["total"] for item in results.values())

        if bib and bib in results:
            stats = results[bib]
            console.print(f"Acervo {bib}: {stats['updated']} .json atualizados de {stats['total']} páginas com TXT")
            return

        console.print(
            f"\n[bold green]Metadados enriquecidos:[/bold green] "
            f"{total_updated} .json atualizados em {total_pages} páginas"
        )

    @cli.command()
    @click.option("--bib", default=None, help="Código do acervo específico")
    @click.option("--reset", is_flag=True, help="Remover embeddings antigos do acervo antes de reindexar")
    @click.option("--batch-size", default=100, show_default=True, help="Tamanho do lote no ChromaDB")
    def indexar(bib, reset, batch_size):
        """Indexa chunks no ChromaDB para busca semântica."""
        from src.processing.indexer import indexar_acervo, indexar_todos, reindexar_acervo

        console.print("\n[bold]Indexando no ChromaDB...[/bold]\n")
        if reset and not bib:
            raise click.UsageError("--reset exige --bib para evitar limpeza global acidental")

        if bib:
            if reset:
                result = reindexar_acervo(bib, batch_size=batch_size)
                console.print(
                    f"Acervo {bib}: {result['deleted']} chunks removidos do índice, "
                    f"{result['indexed']} chunks reindexados"
                )
            else:
                console.print(f"Acervo {bib}: {indexar_acervo(bib, batch_size=batch_size)} chunks indexados")
            return

        stats = indexar_todos(batch_size=batch_size)
        total = sum(stats.values())
        console.print(f"\n[bold green]Indexação concluída: {total} novos chunks[/bold green]")
