"""Frontend web para busca na Hemeroteca Digital PE."""

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from src.indexer import buscar, stats

app = FastAPI(title="Hemeroteca Digital PE")


@app.get("/", response_class=HTMLResponse)
def index():
    s = stats()
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hemeroteca Digital PE</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f5f0eb; color: #2c2c2c; }}
header {{ background: #1a3a4a; color: #f5f0eb; padding: 2rem; text-align: center; }}
header h1 {{ font-size: 1.8rem; margin-bottom: 0.3rem; }}
header p {{ opacity: 0.8; font-size: 0.9rem; }}
.search-box {{ max-width: 700px; margin: 2rem auto; padding: 0 1rem; }}
.search-form {{ display: flex; gap: 0.5rem; }}
.search-form input[type=text] {{
    flex: 1; padding: 0.8rem 1rem; font-size: 1.1rem; border: 2px solid #ccc;
    border-radius: 8px; outline: none; }}
.search-form input:focus {{ border-color: #1a3a4a; }}
.search-form button {{
    padding: 0.8rem 1.5rem; background: #1a3a4a; color: white; border: none;
    border-radius: 8px; font-size: 1rem; cursor: pointer; }}
.search-form button:hover {{ background: #264e60; }}
#results {{ max-width: 700px; margin: 1rem auto; padding: 0 1rem; }}
.result {{ background: white; border-radius: 8px; padding: 1.2rem; margin-bottom: 1rem;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.result-header {{ display: flex; justify-content: space-between; margin-bottom: 0.5rem;
                  font-size: 0.85rem; color: #666; }}
.result-score {{ background: #e8f4e8; color: #2a6e2a; padding: 2px 8px;
                 border-radius: 4px; font-weight: 600; }}
.result-meta {{ font-size: 0.85rem; color: #888; margin-bottom: 0.5rem; }}
.result-text {{ font-size: 0.95rem; line-height: 1.6; white-space: pre-wrap;
                word-wrap: break-word; }}
.stats {{ text-align: center; color: #999; font-size: 0.8rem; margin: 1rem; }}
.loading {{ text-align: center; padding: 2rem; color: #666; display: none; }}
mark {{ background: #fff3b0; padding: 1px 2px; border-radius: 2px; }}
</style>
</head>
<body>
<header>
    <h1>Hemeroteca Digital PE</h1>
    <p>Busca em jornais hist&oacute;ricos de Pernambuco &mdash; {s['total_chunks']:,} trechos indexados</p>
</header>
<div class="search-box">
    <form class="search-form" onsubmit="doSearch(event)">
        <input type="text" id="q" placeholder="Pesquisar nos jornais..." autofocus>
        <button type="submit">Buscar</button>
    </form>
</div>
<div id="loading" class="loading">Buscando...</div>
<div id="results"></div>
<script>
async function doSearch(e) {{
    e.preventDefault();
    const q = document.getElementById('q').value.trim();
    if (!q) return;
    const res = document.getElementById('results');
    const loading = document.getElementById('loading');
    res.innerHTML = '';
    loading.style.display = 'block';
    try {{
        const resp = await fetch('/api/buscar?q=' + encodeURIComponent(q));
        const data = await resp.json();
        loading.style.display = 'none';
        if (!data.length) {{
            res.innerHTML = '<p style="text-align:center;color:#999;padding:2rem">Nenhum resultado encontrado.</p>';
            return;
        }}
        const words = q.toLowerCase().split(/\\s+/);
        data.forEach((r, i) => {{
            let txt = r.texto.replace(/&/g,'&amp;').replace(/</g,'&lt;');
            words.forEach(w => {{
                if (w.length >= 3) {{
                    const re = new RegExp('(' + w.replace(/[.*+?^${{}}()|[\\]\\\\]/g,'\\\\$&') + ')', 'gi');
                    txt = txt.replace(re, '<mark>$1</mark>');
                }}
            }});
            const score = (1 - r.distancia).toFixed(3);
            const meta = r.metadata;
            res.innerHTML += `
            <div class="result">
                <div class="result-header">
                    <span>#${{i+1}} &mdash; ${{meta.bib || '?'}}</span>
                    <span class="result-score">${{score}}</span>
                </div>
                <div class="result-meta">
                    P&aacute;gina: ${{meta.pagina || '?'}} | Ano: ${{meta.ano || '?'}} | Edi&ccedil;&atilde;o: ${{meta.edicao || '?'}}
                </div>
                <div class="result-text">${{txt}}</div>
            </div>`;
        }});
    }} catch(err) {{
        loading.style.display = 'none';
        res.innerHTML = '<p style="text-align:center;color:red">Erro: ' + err.message + '</p>';
    }}
}}
</script>
</body>
</html>"""


@app.get("/api/buscar")
def api_buscar(q: str = Query(..., min_length=1), n: int = Query(20, ge=1, le=100),
               bib: str = Query(None)):
    return buscar(q, n_results=n, filtro_bib=bib)
