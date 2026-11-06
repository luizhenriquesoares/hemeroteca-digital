"""Comando do painel de processamento contínuo do acervo."""

from __future__ import annotations

import click

from src.cli.context import console


def register_pipeline_commands(cli) -> None:
    @cli.command("painel")
    @click.option("--port", default=8765, show_default=True, help="Porta local do painel")
    @click.option("--no-browser", is_flag=True, help="Não abrir o navegador automaticamente")
    @click.option("--host", default="127.0.0.1", show_default=True,
                  help="Endereço de escuta. Fora de 127.0.0.1 exige PAINEL_SENHA definida.")
    def painel(port, no_browser, host):
        """Abre o painel em tempo real e o processador do acervo (fila, métricas, pausa)."""
        import os
        import threading
        import webbrowser

        import uvicorn

        from src.pipeline.server import create_app

        if host not in ("127.0.0.1", "localhost") and not os.environ.get("PAINEL_SENHA"):
            raise click.UsageError("Para abrir o painel fora deste computador, defina PAINEL_SENHA antes.")
        url = f"http://127.0.0.1:{port}"
        console.print(f"\n[bold]Painel de processamento[/bold] em {url}  (Ctrl+C encerra; a fila fica salva)\n")
        if not no_browser:
            threading.Timer(1.5, lambda: webbrowser.open(url)).start()
        uvicorn.run(create_app(), host=host, port=port, log_level="warning", proxy_headers=True)
