"""Comandos da camada estruturada."""

from __future__ import annotations

import click

from src.cli.context import console


def register_structured_commands(cli) -> None:
    @cli.command("estruturar")
    @click.option("--bib", default=None, help="Código do acervo específico")
    def estruturar(bib):
        """Extrai entidades e relações com evidência para a camada estruturada."""
        from src.structured.service import process_all, process_bib

        console.print("\n[bold]Extraindo entidades e relações com evidência...[/bold]\n")

        if bib:
            stats = process_bib(bib)
            console.print(
                f"Acervo {bib}: {stats['chunks']} chunks, {stats['entities']} entidades, "
                f"{stats['relations']} relações"
            )
            return

        all_stats = process_all()
        total_chunks = sum(item["chunks"] for item in all_stats.values())
        total_entities = sum(item["entities"] for item in all_stats.values())
        total_relations = sum(item["relations"] for item in all_stats.values())
        console.print(
            f"\n[bold green]Camada estruturada concluída:[/bold green] "
            f"{total_chunks} chunks, {total_entities} entidades, {total_relations} relações"
        )
