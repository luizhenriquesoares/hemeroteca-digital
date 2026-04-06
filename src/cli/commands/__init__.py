"""Registro de comandos do CLI."""

from src.cli.commands.processing import register_processing_commands
from src.cli.commands.query import register_query_commands
from src.cli.commands.scraping import register_scraping_commands
from src.cli.commands.structured import register_structured_commands


def register_commands(cli) -> None:
    register_scraping_commands(cli)
    register_processing_commands(cli)
    register_structured_commands(cli)
    register_query_commands(cli)
