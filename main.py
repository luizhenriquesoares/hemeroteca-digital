"""
Hemeroteca PE - Captura de jornais de Pernambuco da Hemeroteca Digital Brasileira.

Pipeline completo: Scraping -> OCR -> Chunking -> Indexação RAG

Uso:
    python main.py listar                     # Lista acervos de PE
    python main.py capturar                   # Captura TODOS os jornais
    python main.py capturar --bib 123456      # Captura um jornal específico
    python main.py capturar --max-pages 10    # Limita páginas por jornal
    python main.py ocr                        # Extrai texto de todas as imagens
    python main.py ocr --bib 123456           # OCR de um acervo específico
    python main.py chunkar                    # Divide textos em chunks
    python main.py indexar                    # Indexa no ChromaDB
    python main.py pipeline                   # Executa tudo em sequência
    python main.py pipeline-paralelo          # Pipeline com 4 Chrome drivers
    python main.py pipeline-paralelo --workers 6  # Pipeline com 6 workers
    python main.py buscar "abolição"          # Busca semântica
    python main.py stats                      # Estatísticas do projeto
"""

import logging
import sys
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.logging import RichHandler

from src.config import LOGS_DIR, IMAGES_DIR, TEXT_DIR, CHUNKS_DIR

console = Console()

# Configurar logging
log_file = LOGS_DIR / f"hemeroteca_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, show_path=False, markup=False),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# Silenciar logs verbosos de libs externas
for _noisy in ("httpx", "httpcore", "huggingface_hub", "sentence_transformers", "chromadb"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


@click.group()
def cli():
    """Hemeroteca PE - Captura de jornais de Pernambuco para RAG."""
    pass


@cli.command()
@click.option("--headless/--no-headless", default=False,
              help="Modo headless do Chrome (default: visual, para resolver CAPTCHA)")
def listar(headless):
    """Lista todos os acervos (jornais) disponíveis de Pernambuco."""
    from src.acervos import buscar_acervos

    console.print("\n[bold]Buscando acervos de PE na Hemeroteca Digital...[/bold]")
    console.print("[yellow]NOTA: Se aparecer CAPTCHA, resolva manualmente no navegador.[/yellow]\n")

    acervos = buscar_acervos(headless=headless)

    table = Table(title=f"Acervos de PE ({len(acervos)} jornais)")
    table.add_column("#", style="dim", width=5)
    table.add_column("Código", style="cyan", width=10)
    table.add_column("Jornal", style="green")

    for i, a in enumerate(acervos, 1):
        table.add_row(str(i), a["bib"], a["nome"])

    console.print(table)


@cli.command("limpar-cache")
def limpar_cache():
    """Remove cache de acervos para forçar nova busca."""
    from src.acervos import limpar_cache as _limpar
    _limpar()
    console.print("[green]Cache limpo.[/green]")


@cli.command()
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--max-pages", default=0, help="Limite de páginas por jornal (0 = sem limite)")
@click.option("--headless/--no-headless", default=False, help="Modo headless do Chrome (default: visual para CAPTCHA)")
def capturar(bib, max_pages, headless):
    """Captura páginas dos jornais (scraping + download de imagens)."""
    from src.acervos import buscar_acervos
    from src.scraper import scrape_jornal
    from src.driver import create_driver

    driver = create_driver(headless=headless)

    try:
        if bib:
            acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
        else:
            acervos = buscar_acervos(driver=driver, headless=headless)

        console.print(f"\n[bold]Capturando {len(acervos)} acervos...[/bold]\n")

        total_paginas = 0
        for i, acervo in enumerate(acervos, 1):
            console.print(
                f"\n[cyan][{i}/{len(acervos)}][/cyan] "
                f"Jornal: [bold]{acervo['nome']}[/bold] ({acervo['bib']})"
            )
            try:
                resultados = scrape_jornal(
                    driver, acervo["bib"], acervo["nome"], max_pages=max_pages
                )
                total_paginas += len(resultados)
                console.print(f"  -> {len(resultados)} páginas capturadas")
            except Exception as e:
                console.print(f"  [red]ERRO: {e}[/red]")
                logger.error(f"Erro no acervo {acervo['bib']}: {e}", exc_info=True)

        console.print(f"\n[bold green]Total: {total_paginas} páginas capturadas[/bold green]")

    finally:
        driver.quit()


@cli.command()
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--force", is_flag=True, help="Reprocessar imagens já convertidas")
@click.option("--workers", default=0, help="Workers paralelos (0 = auto, cpu_count - 2)")
def ocr(bib, force, workers):
    """Extrai texto das imagens usando Tesseract OCR."""
    from src.ocr import processar_acervo, processar_todos_acervos

    console.print("\n[bold]Iniciando OCR...[/bold]\n")

    if bib:
        count = processar_acervo(bib, force=force)
        console.print(f"Acervo {bib}: {count} páginas processadas")
    else:
        stats = processar_todos_acervos(force=force, workers=workers)
        total = sum(stats.values())
        console.print(f"\n[bold green]OCR concluído: {total} páginas em {len(stats)} acervos[/bold green]")


@cli.command("ocr-hires")
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--force", is_flag=True, help="Reprocessar páginas já convertidas")
@click.option("--headless/--no-headless", default=True,
              help="Modo headless do Chrome (default: headless)")
@click.option("--workers", default=4, help="Número de Chrome drivers paralelos (default: 4)")
@click.option("--max-pages", default=0, help="Limite de páginas por acervo (0 = todas)")
@click.option("--keep-images", is_flag=True, help="Salvar imagens hi-res em data/images/ (para validação)")
def ocr_hires(bib, force, headless, workers, max_pages, keep_images):
    """OCR em alta resolução MÁXIMA (6464x8940): baixa imagens hi-res do DocReader, faz OCR e deleta.

    Resolução máxima do site: 6464x8940. Uso mínimo de disco (~4MB temporário por imagem).
    Se --keep-images, as imagens são mantidas em data/images/ para validação.

    Exemplos:
        python main.py ocr-hires --max-pages 2 --keep-images --bib 029033_06   # Teste 2 pgs
        python main.py ocr-hires --bib 029033_06                                # Um acervo
        python main.py ocr-hires --no-headless                                  # Visual (CAPTCHA)
        python main.py ocr-hires --workers 8                                    # 8 workers paralelos
    """
    from src.hires_pipeline import processar_todos_hires, HIRES_SIZE

    console.print(f"\n[bold]OCR Hi-Res MÁXIMO: {HIRES_SIZE}[/bold]")
    if keep_images:
        console.print("[yellow]--keep-images ativo: imagens serão salvas em data/images/[/yellow]")
    console.print(f"[dim]Workers: {workers} | max_pages: {max_pages or 'todas'}[/dim]\n")

    stats = processar_todos_hires(headless=headless, force=force, bib=bib, workers=workers,
                                   max_pages=max_pages, keep_images=keep_images)
    total = sum(stats.values())
    console.print(f"\n[bold green]OCR hi-res concluído: {total} páginas em {len(stats)} acervos[/bold green]")


@cli.command("corrigir-claude")
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--model", default="opus", type=click.Choice(["opus", "sonnet", "haiku"]))
@click.option("--force", is_flag=True, help="Reprocessar textos já corrigidos")
@click.option("--workers", default=2, help="Processos paralelos (default: 2)")
def corrigir_claude(bib, model, force, workers):
    """Corrige textos OCR usando Claude via CLI (Claude Code Max).

    Salva os arquivos corrigidos como {nome}_corrigido.txt ao lado dos originais.
    Usa sessão autenticada do Claude Code (flat-rate via Max 20x).

    Exemplos:
        python main.py corrigir-claude --bib 029033_02 --model opus
        python main.py corrigir-claude --workers 3
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.llm_correcao_claude import corrigir_arquivo
    from src.config import TEXT_DIR

    # Listar arquivos a processar
    if bib:
        txt_dir = TEXT_DIR / bib
        if not txt_dir.exists():
            console.print(f"[red]Diretório não encontrado: {txt_dir}[/red]")
            return
        arquivos = list(txt_dir.glob("*.txt"))
    else:
        arquivos = list(TEXT_DIR.rglob("*.txt"))

    # Filtrar arquivos já corrigidos
    arquivos = [a for a in arquivos if not a.name.endswith("_corrigido.txt")]
    if not force:
        arquivos = [a for a in arquivos
                    if not (a.parent / a.name.replace(".txt", "_corrigido.txt")).exists()]

    console.print(f"\n[bold]Correção Claude {model} ({workers} workers)[/bold]")
    console.print(f"[dim]{len(arquivos)} arquivos pendentes[/dim]\n")

    if not arquivos:
        console.print("[green]Nada a fazer.[/green]")
        return

    processados = 0
    falhas = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(corrigir_arquivo, a, model, force): a for a in arquivos}
        for fut in as_completed(futures):
            a = futures[fut]
            try:
                ok = fut.result()
                if ok:
                    processados += 1
                else:
                    falhas += 1
                total_done = processados + falhas
                if total_done % 10 == 0 or total_done == len(arquivos):
                    console.print(f"  [{total_done}/{len(arquivos)}] ok={processados} falhas={falhas}")
            except Exception as e:
                falhas += 1
                console.print(f"  [red]ERRO {a.name}: {e}[/red]")

    console.print(f"\n[bold green]Concluído: {processados} corrigidos, {falhas} falhas[/bold green]")


@cli.command()
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--force", is_flag=True, help="Recriar chunks existentes")
def chunkar(bib, force):
    """Divide textos em chunks para indexação RAG."""
    from src.chunker import criar_chunks_acervo, criar_chunks_todos

    console.print("\n[bold]Criando chunks...[/bold]\n")

    if bib:
        count = criar_chunks_acervo(bib, force=force)
        console.print(f"Acervo {bib}: {count} chunks criados")
    else:
        stats = criar_chunks_todos(force=force)
        total = sum(stats.values())
        console.print(f"\n[bold green]Chunking concluído: {total} chunks em {len(stats)} acervos[/bold green]")


@cli.command()
@click.option("--bib", default=None, help="Código do acervo específico")
def indexar(bib):
    """Indexa chunks no ChromaDB para busca semântica."""
    from src.indexer import indexar_acervo, indexar_todos

    console.print("\n[bold]Indexando no ChromaDB...[/bold]\n")

    if bib:
        count = indexar_acervo(bib)
        console.print(f"Acervo {bib}: {count} chunks indexados")
    else:
        stats = indexar_todos()
        total = sum(stats.values())
        console.print(f"\n[bold green]Indexação concluída: {total} novos chunks[/bold green]")


@cli.command()
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--max-pages", default=0, help="Limite de páginas por jornal")
@click.option("--headless/--no-headless", default=False, help="Modo headless do Chrome (default: visual para CAPTCHA)")
def pipeline(bib, max_pages, headless):
    """Executa o pipeline completo: captura -> OCR -> chunking -> indexação.

    Resumível: se interrompido, reinicia de onde parou.
    """
    import json
    from src.acervos import buscar_acervos
    from src.scraper import scrape_jornal
    from src.driver import create_driver
    from src.ocr import processar_acervo
    from src.chunker import criar_chunks_acervo
    from src.indexer import indexar_acervo
    from src.config import CACHE_DIR

    # Arquivo de progresso para resume
    progress_file = CACHE_DIR / "pipeline_progress.json"

    def load_progress():
        if progress_file.exists():
            with open(progress_file) as f:
                return json.load(f)
        return {"done": [], "failed": []}

    def save_progress(progress):
        with open(progress_file, "w") as f:
            json.dump(progress, f)

    console.print("\n[bold]Pipeline completo (resumível)[/bold]\n")
    console.print("Etapas: Captura -> OCR -> Chunking -> Indexação\n")

    driver = create_driver(headless=headless)
    progress = load_progress()
    done_set = set(progress["done"])

    try:
        if bib:
            acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
        else:
            acervos = buscar_acervos(driver=driver, headless=headless)

        # Contar quantos faltam
        pendentes = [a for a in acervos if a["bib"] not in done_set]
        console.print(f"Total: {len(acervos)} acervos | "
                       f"Concluídos: {len(done_set)} | "
                       f"Pendentes: {len(pendentes)}\n")

        for i, acervo in enumerate(acervos, 1):
            b = acervo["bib"]
            nome = acervo["nome"]

            # Pular acervos já concluídos
            if b in done_set:
                continue

            console.print(f"\n[cyan]━━━ [{i}/{len(acervos)}] {nome} ({b}) ━━━[/cyan]")

            try:
                # 1. Captura
                console.print("  [yellow]1/4[/yellow] Capturando páginas...")
                try:
                    resultados = scrape_jornal(driver, b, nome, max_pages=max_pages)
                    console.print(f"       {len(resultados)} páginas capturadas")
                except Exception as e:
                    console.print(f"       [red]ERRO na captura: {e}[/red]")
                    logger.error(f"Erro captura {b}: {e}", exc_info=True)
                    # Recriar driver e continuar com próximo acervo
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = create_driver(headless=headless)
                    progress["failed"].append(b)
                    save_progress(progress)
                    continue

                # 2. OCR
                console.print("  [yellow]2/4[/yellow] Executando OCR...")
                ocr_count = processar_acervo(b)
                console.print(f"       {ocr_count} páginas processadas")

                # 3. Chunking
                console.print("  [yellow]3/4[/yellow] Criando chunks...")
                chunk_count = criar_chunks_acervo(b)
                console.print(f"       {chunk_count} chunks criados")

                # 4. Indexação
                console.print("  [yellow]4/4[/yellow] Indexando...")
                index_count = indexar_acervo(b)
                console.print(f"       {index_count} chunks indexados")

                # Marcar como concluído
                progress["done"].append(b)
                done_set.add(b)
                save_progress(progress)
                console.print(f"  [green]✓ {nome} concluído ({len(done_set)}/{len(acervos)})[/green]")

            except Exception as e:
                console.print(f"  [red]ERRO FATAL em {b}: {e}[/red]")
                logger.error(f"Erro fatal {b}: {e}", exc_info=True)
                # Recriar driver e continuar
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver(headless=headless)
                progress["failed"].append(b)
                save_progress(progress)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    console.print(f"\n[bold green]Pipeline concluído! "
                   f"{len(done_set)} acervos processados.[/bold green]")


@cli.command("pipeline-paralelo")
@click.option("--workers", default=4, help="Número de Chrome drivers paralelos (default: 4)")
@click.option("--bib", default=None, help="Código do acervo específico")
@click.option("--max-pages", default=0, help="Limite de páginas por jornal")
@click.option("--headless/--no-headless", default=True, help="Modo headless (default: headless)")
@click.option("--skip-indexing", is_flag=True, help="Pular indexação final no ChromaDB")
@click.option("--capture-only", is_flag=True, help="Só capturar imagens (pular OCR/chunking para máxima velocidade)")
def pipeline_paralelo(workers, bib, max_pages, headless, skip_indexing, capture_only):
    """Pipeline paralelo: N Chrome drivers processam acervos simultaneamente.

    Captura + OCR + Chunking rodam em paralelo.
    Indexação no ChromaDB roda ao final (sequencial).

    Resumível: se interrompido (CTRL+C), reinicia de onde parou.

    Exemplos:
        python main.py pipeline-paralelo                    # 4 workers, todos acervos
        python main.py pipeline-paralelo --workers 6        # 6 workers
        python main.py pipeline-paralelo --max-pages 10     # teste rápido
    """
    import json
    from src.config import CACHE_DIR

    # Carregar acervos do cache
    cache_file = CACHE_DIR / "acervos_pe.json"
    if bib:
        acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
    elif cache_file.exists():
        with open(cache_file) as f:
            acervos = json.load(f)
        console.print(f"[bold]{len(acervos)} acervos carregados do cache[/bold]")
    else:
        console.print("[yellow]Cache de acervos não encontrado. Execute 'listar' primeiro.[/yellow]")
        return

    from src.parallel import run_parallel_pipeline
    run_parallel_pipeline(
        acervos,
        num_workers=workers,
        max_pages=max_pages,
        headless=headless,
        skip_indexing=skip_indexing,
        capture_only=capture_only,
    )


@cli.command()
@click.argument("query")
@click.option("--n", default=10, help="Número de resultados")
@click.option("--bib", default=None, help="Filtrar por acervo")
def buscar(query, n, bib):
    """Busca semântica nos jornais indexados."""
    from src.indexer import buscar as _buscar

    console.print(f"\n[bold]Buscando: '{query}'[/bold]\n")

    resultados = _buscar(query, n_results=n, filtro_bib=bib)

    if not resultados:
        console.print("[yellow]Nenhum resultado encontrado.[/yellow]")
        return

    for i, r in enumerate(resultados, 1):
        meta = r["metadata"]
        score = 1 - r["distancia"]  # cosine similarity
        console.print(f"[cyan]━━━ Resultado {i} (score: {score:.3f}) ━━━[/cyan]")
        console.print(f"Acervo: [bold]{meta.get('bib', '?')}[/bold] | "
                       f"Página: {meta.get('pagina', '?')} | "
                       f"Ano: {meta.get('ano', '?')}")
        console.print(f"\n{r['texto'][:500]}...")
        console.print()


@cli.command()
def stats():
    """Mostra estatísticas do projeto."""
    table = Table(title="Estatísticas do Projeto")
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", style="green")

    # Contagens de arquivos
    img_count = sum(1 for _ in IMAGES_DIR.rglob("*.jpg")) if IMAGES_DIR.exists() else 0
    txt_count = sum(1 for _ in TEXT_DIR.rglob("*.txt")) if TEXT_DIR.exists() else 0
    chunk_count = sum(1 for _ in CHUNKS_DIR.rglob("*.jsonl")) if CHUNKS_DIR.exists() else 0

    # Acervos
    acervos_img = len(list(IMAGES_DIR.iterdir())) if IMAGES_DIR.exists() else 0

    table.add_row("Acervos com imagens", str(acervos_img))
    table.add_row("Total de imagens", str(img_count))
    table.add_row("Total de textos (OCR)", str(txt_count))
    table.add_row("Arquivos de chunks", str(chunk_count))

    # ChromaDB stats (só carrega se já tiver chunks processados)
    if chunk_count > 0:
        try:
            from src.indexer import stats as idx_stats
            s = idx_stats()
            table.add_row("Chunks indexados", str(s["total_chunks"]))
            table.add_row("Modelo de embedding", s["embedding_model"])
        except Exception:
            table.add_row("Chunks indexados", "N/A")
    else:
        table.add_row("Chunks indexados", "0 (execute 'chunkar' e 'indexar' primeiro)")

    console.print(table)


if __name__ == "__main__":
    cli()
