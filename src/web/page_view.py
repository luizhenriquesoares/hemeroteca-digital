"""Renderização HTML para visualização de página do jornal."""

from __future__ import annotations

from html import escape
from urllib.parse import quote


def render_page_view(page: dict, *, query: str = "") -> str:
    jornal = escape(str(page.get("jornal") or "?"))
    bib = escape(str(page.get("bib") or "?"))
    pagina = escape(str(page.get("pagina") or "?"))
    ano = escape(str(page.get("ano") or "?"))
    edicao = escape(str(page.get("edicao") or "?"))
    image_url = page.get("image_url")
    api_url = f"/api/page/{bib}/{pagina}"
    query_text = str(query or "").strip()
    escaped_query = escape(query_text)
    encoded_query = quote(query_text)
    image_link = (
        f'<a href="{escape(str(image_url))}" target="_blank" rel="noreferrer" '
        'class="px-3 py-2 rounded-lg border border-stone-300 text-stone-700 hover:bg-stone-50">Abrir imagem</a>'
        if image_url
        else ""
    )
    ocr_text = escape(str(page.get("ocr_text") or "Texto não disponível."))
    citation_text = escape(f"{page.get('jornal') or '?'} · {page.get('bib') or '?'} / {page.get('pagina') or '?'} · ano {page.get('ano') or '?'} · edição {page.get('edicao') or '?'}")
    text_size = f"{len(str(page.get('ocr_text') or '')):,}".replace(",", ".")

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{jornal} · Página {pagina}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@500;700&family=Inter:wght@400;500;600&display=swap');
        :root {{
            --bg: #f5f5f4;
            --panel: #ffffff;
            --line: #e7e5e4;
            --text: #292524;
            --muted: #78716c;
            --accent: #b45309;
            --accent-soft: #fef3c7;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            background: linear-gradient(180deg, #fafaf9 0%, var(--bg) 100%);
            color: var(--text);
            font-family: 'Inter', sans-serif;
        }}
        .shell {{
            max-width: 1500px;
            margin: 0 auto;
            padding: 32px 24px 48px;
        }}
        .topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 18px;
        }}
        .header {{
            display: flex;
            gap: 16px;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 24px;
        }}
        .title {{
            font-family: 'Playfair Display', serif;
            font-size: 2rem;
            margin: 0 0 6px;
        }}
        .subtitle {{
            color: var(--muted);
            margin: 0;
            line-height: 1.5;
        }}
        .actions {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }}
        .btn {{
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 10px;
            border: 1px solid var(--line);
            background: var(--panel);
            color: var(--text);
            font-weight: 500;
        }}
        .btn.primary {{
            border-color: #d97706;
            background: #b45309;
            color: white;
        }}
        .meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 24px;
        }}
        .chip {{
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 7px 12px;
            font-size: 0.85rem;
            color: var(--muted);
        }}
        .grid {{
            display: grid;
            grid-template-columns: minmax(360px, 1.05fr) minmax(420px, 0.95fr);
            gap: 20px;
            align-items: start;
        }}
        .panel {{
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(28, 25, 23, 0.06);
        }}
        .panel-header {{
            padding: 14px 18px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(180deg, #fff 0%, #fafaf9 100%);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            font-weight: 600;
        }}
        .image-wrap {{
            min-height: 720px;
            display: block;
            overflow: auto;
            background: #fafaf9;
            padding: 18px;
        }}
        .image-wrap img {{
            display: block;
            margin: 0 auto;
            max-width: 100%;
            height: auto;
            border-radius: 12px;
            border: 1px solid #d6d3d1;
            background: white;
            transform-origin: top center;
            transition: transform 0.15s ease;
        }}
        .empty {{
            color: var(--muted);
            padding: 28px;
            text-align: center;
            line-height: 1.6;
        }}
        .panel-tools {{
            display: flex;
            gap: 10px;
            align-items: center;
            justify-content: space-between;
            padding: 12px 18px;
            border-bottom: 1px solid var(--line);
            background: #fcfcfb;
        }}
        .tool-group {{
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }}
        .tool-btn, .tool-input {{
            border: 1px solid var(--line);
            background: white;
            color: var(--text);
            border-radius: 10px;
            padding: 9px 12px;
            font: inherit;
        }}
        .tool-btn {{
            cursor: pointer;
            font-weight: 500;
        }}
        .tool-btn:hover {{
            border-color: #d6d3d1;
            background: #fafaf9;
        }}
        .tool-btn.active {{
            background: #fff7ed;
            border-color: #fdba74;
            color: #9a3412;
        }}
        .tool-input {{
            min-width: 220px;
        }}
        .text-wrap {{
            padding: 20px;
        }}
        .text-wrap pre {{
            margin: 0;
            white-space: pre-wrap;
            word-break: break-word;
            line-height: 1.8;
            font-size: 0.95rem;
            color: #1c1917;
            font-family: Georgia, 'Times New Roman', serif;
        }}
        mark {{
            background: #fde68a;
            color: inherit;
            padding: 0 2px;
            border-radius: 3px;
        }}
        .status {{
            color: var(--muted);
            font-size: 0.85rem;
        }}
        @media (max-width: 960px) {{
            .grid {{ grid-template-columns: 1fr; }}
            .header {{ flex-direction: column; }}
            .topbar {{ flex-direction: column; align-items: flex-start; }}
            .panel-tools {{ flex-direction: column; align-items: stretch; }}
        }}
    </style>
</head>
<body>
    <main class="shell">
        <div class="topbar">
            <a class="btn" href="/">Voltar à pesquisa</a>
            <div class="status">Leitura documental com referência cruzada entre imagem e transcrição.</div>
        </div>
        <div class="header">
            <div>
                <h1 class="title">{jornal}</h1>
                <p class="subtitle">Visualização documental da página {pagina}. A transcrição abaixo é exibida junto da fonte para conferência direta.</p>
            </div>
            <div class="actions">
                <a class="btn" href="{api_url}" target="_blank" rel="noreferrer">Ver JSON da API</a>
                <button class="btn primary" type="button" id="copy-citation">Copiar citação</button>
                {image_link}
            </div>
        </div>

        <div class="meta">
            <span class="chip">Acervo: {bib}</span>
            <span class="chip">Página: {pagina}</span>
            <span class="chip">Ano: {ano}</span>
            <span class="chip">Edição: {edicao}</span>
            <span class="chip">Transcrição: {text_size} caracteres</span>
            {f'<span class="chip">Destaque atual: {escaped_query}</span>' if query_text else ''}
        </div>

        <section class="grid">
            <article class="panel">
                <div class="panel-header">Página Digitalizada</div>
                <div class="panel-tools">
                    <div class="tool-group">
                        <button class="tool-btn" type="button" data-zoom="out">-</button>
                        <button class="tool-btn active" type="button" data-zoom="reset">100%</button>
                        <button class="tool-btn" type="button" data-zoom="in">+</button>
                    </div>
                    <div class="status">{'Use o zoom para conferir a página original.' if image_url else 'A visualização da imagem será habilitada quando a página estiver disponível.'}</div>
                </div>
                <div class="image-wrap">
                    {f'<img id="page-image" src="{escape(str(image_url))}" alt="Página {pagina} de {jornal}">' if image_url else '<div class="empty">Imagem da página ainda não disponível neste acervo.</div>'}
                </div>
            </article>

            <article class="panel">
                <div class="panel-header">Transcrição OCR / Texto Corrigido</div>
                <div class="panel-tools">
                    <div class="tool-group">
                        <input class="tool-input" id="text-search" type="text" value="{escaped_query}" placeholder="Destacar termo nesta página">
                        <button class="tool-btn" type="button" id="highlight-btn">Destacar</button>
                        <button class="tool-btn" type="button" id="clear-highlight-btn">Limpar</button>
                    </div>
                    <div class="status" id="highlight-status">A transcrição pode conter OCR imperfeito; confira a imagem ao lado.</div>
                </div>
                <div class="text-wrap">
                    <pre id="page-text" data-raw="{ocr_text}">{ocr_text}</pre>
                </div>
            </article>
        </section>
    </main>
    <script>
        const pageImage = document.getElementById('page-image');
        const zoomButtons = document.querySelectorAll('[data-zoom]');
        const copyCitationButton = document.getElementById('copy-citation');
        const textSearch = document.getElementById('text-search');
        const highlightButton = document.getElementById('highlight-btn');
        const clearHighlightButton = document.getElementById('clear-highlight-btn');
        const pageText = document.getElementById('page-text');
        const highlightStatus = document.getElementById('highlight-status');
        const citationText = `{citation_text}`;
        let zoomLevel = 1;

        function applyZoom(nextZoom) {{
            if (!pageImage) return;
            zoomLevel = Math.max(0.5, Math.min(3, nextZoom));
            pageImage.style.transform = `scale(${{zoomLevel}})`;
            document.querySelector('[data-zoom="reset"]').textContent = `${{Math.round(zoomLevel * 100)}}%`;
        }}

        zoomButtons.forEach(button => {{
            button.addEventListener('click', () => {{
                const kind = button.dataset.zoom;
                if (kind === 'in') applyZoom(zoomLevel + 0.15);
                if (kind === 'out') applyZoom(zoomLevel - 0.15);
                if (kind === 'reset') applyZoom(1);
            }});
        }});

        async function copyCitation() {{
            try {{
                await navigator.clipboard.writeText(citationText);
                copyCitationButton.textContent = 'Citação copiada';
                setTimeout(() => {{
                    copyCitationButton.textContent = 'Copiar citação';
                }}, 1400);
            }} catch (err) {{
                copyCitationButton.textContent = 'Falha ao copiar';
            }}
        }}

        function escapeRegExp(value) {{
            return value.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&');
        }}

        function applyHighlight(term) {{
            const raw = pageText.dataset.raw || '';
            if (!term) {{
                pageText.innerHTML = raw;
                highlightStatus.textContent = 'Destaque limpo.';
                return;
            }}
            const regex = new RegExp(`(${{escapeRegExp(term)}})`, 'gi');
            pageText.innerHTML = raw.replace(regex, '<mark>$1</mark>');
            highlightStatus.textContent = `Destaque aplicado para: "${{term}}"`;
            const url = new URL(window.location.href);
            url.searchParams.set('q', term);
            window.history.replaceState(null, '', url.toString());
        }}

        copyCitationButton.addEventListener('click', copyCitation);
        highlightButton.addEventListener('click', () => applyHighlight(textSearch.value.trim()));
        clearHighlightButton.addEventListener('click', () => {{
            textSearch.value = '';
            applyHighlight('');
            const url = new URL(window.location.href);
            url.searchParams.delete('q');
            window.history.replaceState(null, '', url.toString());
        }});
        textSearch.addEventListener('keydown', (event) => {{
            if (event.key === 'Enter') {{
                event.preventDefault();
                applyHighlight(textSearch.value.trim());
            }}
        }});

        if (`{escaped_query}`) {{
            applyHighlight(`{escaped_query}`);
        }}
    </script>
</body>
</html>"""
