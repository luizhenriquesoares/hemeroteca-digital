"""Comandos de consulta e estatísticas."""

from __future__ import annotations

import click
from rich.table import Table

from src.cli.context import console
from src.config import CHUNKS_DIR, IMAGES_DIR, TEXT_DIR


def register_query_commands(cli) -> None:
    @cli.command()
    @click.argument("query")
    @click.option("--n", default=10, help="Número de resultados")
    @click.option("--bib", default=None, help="Filtrar por acervo")
    def buscar(query, n, bib):
        """Busca semântica nos jornais indexados."""
        from src.processing.indexer import buscar as search_index

        console.print(f"\n[bold]Buscando: '{query}'[/bold]\n")
        resultados = search_index(query, n_results=n, filtro_bib=bib)

        if not resultados:
            console.print("[yellow]Nenhum resultado encontrado.[/yellow]")
            return

        for index, result in enumerate(resultados, 1):
            meta = result["metadata"]
            score = 1 - result["distancia"]
            console.print(f"[cyan]━━━ Resultado {index} (score: {score:.3f}) ━━━[/cyan]")
            console.print(
                f"Acervo: [bold]{meta.get('bib', '?')}[/bold] | "
                f"Página: {meta.get('pagina', '?')} | "
                f"Ano: {meta.get('ano', '?')}"
            )
            console.print(f"\n{result['texto'][:500]}...")
            console.print()

    @cli.command()
    def stats():
        """Mostra estatísticas do projeto."""
        table = Table(title="Estatísticas do Projeto")
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="green")

        img_count = sum(1 for _ in IMAGES_DIR.rglob("*.jpg")) if IMAGES_DIR.exists() else 0
        txt_count = sum(1 for _ in TEXT_DIR.rglob("*.txt")) if TEXT_DIR.exists() else 0
        chunk_count = sum(1 for _ in CHUNKS_DIR.rglob("*.jsonl")) if CHUNKS_DIR.exists() else 0
        acervos_img = len(list(IMAGES_DIR.iterdir())) if IMAGES_DIR.exists() else 0

        table.add_row("Acervos com imagens", str(acervos_img))
        table.add_row("Total de imagens", str(img_count))
        table.add_row("Total de textos (OCR)", str(txt_count))
        table.add_row("Arquivos de chunks", str(chunk_count))

        if chunk_count > 0:
            try:
                from src.processing.indexer import stats as index_stats

                stats_payload = index_stats()
                table.add_row("Chunks indexados", str(stats_payload["total_chunks"]))
                table.add_row("Modelo de embedding", stats_payload["embedding_model"])
            except Exception:
                table.add_row("Chunks indexados", "N/A")
        else:
            table.add_row("Chunks indexados", "0 (execute 'chunkar' e 'indexar' primeiro)")

        console.print(table)
