"""CLI principal do projeto Hemeroteca Digital."""

from __future__ import annotations

import click

from src.cli.commands import register_commands


@click.group()
def cli():
    """Hemeroteca PE - Captura de jornais de Pernambuco para RAG."""


register_commands(cli)
