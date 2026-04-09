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

    @cli.command("gerar-biografias")
    @click.option("--top", default=50, help="Número de entidades mais mencionadas")
    @click.option("--force", is_flag=True, help="Regerar biografias existentes")
    def gerar_biografias(top, force):
        """Gera mini-biografias por LLM para as entidades mais relevantes."""
        import json as _json
        from pathlib import Path
        from src.config import DATA_DIR
        from src.structured.repository import StructuredRepository
        from src.web.rag_service import generate_entity_bio

        repo = StructuredRepository()
        bio_dir = DATA_DIR / "structured" / "bios"
        bio_dir.mkdir(parents=True, exist_ok=True)

        # Buscar entidades elegíveis via featured entity pool
        candidates = []
        seen_ids = set()
        for seed in range(0, top * 20):
            entity = repo.get_featured_entity(seed)
            if entity and entity["id"] not in seen_ids:
                seen_ids.add(entity["id"])
                candidates.append(entity)
            if len(candidates) >= top:
                break

        console.print(f"\n[bold]Gerando biografias para {len(candidates)} entidades...[/bold]\n")

        generated = 0
        skipped = 0
        errors = 0
        for entity in candidates:
            entity_id = entity["id"]
            bio_file = bio_dir / f"{entity_id}.json"
            if bio_file.exists() and not force:
                skipped += 1
                continue

            try:
                full_entity = repo.get_entity(entity_id)
                if not full_entity:
                    continue
                bio_text = generate_entity_bio(full_entity)
                bio_file.write_text(
                    _json.dumps({"entity_id": entity_id, "bio": bio_text}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                generated += 1
                console.print(f"  [green]✓[/green] {entity['canonical_name']} ({entity['mentions']} menções)")
            except Exception as exc:
                errors += 1
                console.print(f"  [red]✗[/red] {entity['canonical_name']}: {exc}")

        console.print(
            f"\n[bold green]Biografias concluídas:[/bold green] "
            f"{generated} geradas, {skipped} já existentes, {errors} erros"
        )

    @cli.command("gerar-resumo")
    def gerar_resumo():
        """Gera resumo narrativo do acervo por LLM (cacheado em disco)."""
        import json as _json
        from src.config import DATA_DIR
        from src.structured.repository import StructuredRepository
        from src.web.rag_service import generate_corpus_summary

        repo = StructuredRepository()
        console.print("\n[bold]Gerando resumo narrativo do acervo...[/bold]\n")

        overview = repo.get_discovery_overview(limit=8)
        text = generate_corpus_summary(overview)

        summary_file = DATA_DIR / "cache" / "corpus_summary.json"
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        summary_file.write_text(
            _json.dumps({"summary": text}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        console.print(f"\n{text}\n")
        console.print(f"[dim]Salvo em: {summary_file}[/dim]")

    @cli.command("exportar-grafo")
    @click.option("--include-mentioned-with", is_flag=True, help="Incluir arestas mentioned_with (muitas)")
    @click.option("--min-confidence", default=0.3, help="Confiança mínima para relações")
    @click.option("--min-mentions", default=1, help="Mínimo de menções para incluir entidade")
    @click.option("--format", "fmt", default="all", type=click.Choice(["json", "graphml", "all"]))
    def exportar_grafo(include_mentioned_with, min_confidence, min_mentions, fmt):
        """Exporta camada estruturada para grafo (GraphML + JSON).

        Gera arquivos em data/graph/ para uso com Neo4j, Gephi ou frontend D3.js.

        Exemplos:
            python main.py exportar-grafo
            python main.py exportar-grafo --min-mentions 2 --format json
            python main.py exportar-grafo --include-mentioned-with
        """
        from src.structured.graph_store import build_graph, export_graphml, export_json

        console.print("\n[bold]Construindo grafo...[/bold]\n")

        G = build_graph(
            include_mentioned_with=include_mentioned_with,
            min_confidence=min_confidence,
            min_mentions=min_mentions,
        )

        console.print(f"Grafo: [bold]{G.number_of_nodes()}[/bold] nós, [bold]{G.number_of_edges()}[/bold] arestas\n")

        if fmt in ("graphml", "all"):
            path = export_graphml(G)
            console.print(f"  GraphML: {path}")

        if fmt in ("json", "all"):
            path = export_json(G)
            console.print(f"  JSON:    {path}")

        # Resumo por tipo
        node_types = {}
        for _, data in G.nodes(data=True):
            t = data.get("node_type", "?")
            node_types[t] = node_types.get(t, 0) + 1
        edge_types = {}
        for _, _, data in G.edges(data=True):
            t = data.get("predicate", "?")
            edge_types[t] = edge_types.get(t, 0) + 1

        console.print("\n[dim]Nós por tipo:[/dim]")
        for t, c in sorted(node_types.items(), key=lambda x: -x[1]):
            console.print(f"  {t}: {c:,}")
        console.print("[dim]Arestas por tipo:[/dim]")
        for t, c in sorted(edge_types.items(), key=lambda x: -x[1]):
            console.print(f"  {t}: {c:,}")
