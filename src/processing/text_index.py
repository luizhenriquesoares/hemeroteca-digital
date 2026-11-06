"""Índice local derivado para busca por palavras, sem chamadas a modelos."""
from __future__ import annotations

import json
import re
import sqlite3
import threading
from pathlib import Path

from src import config

_LOCK = threading.Lock()
_YEAR = re.compile(r"\b(1[89]\d{2}|20\d{2}|2100)\b")


def _sources():
    chunks = sorted(config.CHUNKS_DIR.glob('*/chunks.jsonl'))
    if chunks:
        return chunks, 'chunks'
    return sorted(config.TEXT_DIR.glob('*/*.txt')), 'pages'


def _documents(paths, kind):
    for path in paths:
        if kind == 'chunks':
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    if line.strip():
                        item = json.loads(line)
                        yield item['id'], item['text'], item.get('metadata', {})
        elif not path.stem.endswith('_corrigido'):
            corrected = path.with_name(path.stem + '_corrigido.txt')
            source = corrected if corrected.exists() else path
            yield f'{path.parent.name}_{path.stem}', source.read_text(encoding='utf-8', errors='replace'), {'bib': path.parent.name, 'pagina': path.stem}


def _ensure_index(db_path: Path):
    paths, kind = _sources()
    fingerprint = json.dumps([kind, [(str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in paths]])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as db:
        db.execute('CREATE TABLE IF NOT EXISTS index_state (signature TEXT)')
        row = db.execute('SELECT signature FROM index_state').fetchone()
        if row and row[0] == fingerprint:
            return
        # Uma única transação preserva o índice anterior caso a reconstrução falhe.
        db.execute('BEGIN IMMEDIATE')
        db.execute('DROP TABLE IF EXISTS text_fts')
        db.execute("CREATE VIRTUAL TABLE text_fts USING fts5(texto, doc_id UNINDEXED, metadata UNINDEXED, bib UNINDEXED, ano UNINDEXED, tokenize='unicode61 remove_diacritics 2')")
        def rows():
            for doc_id, text, meta in _documents(paths, kind):
                match = _YEAR.search(str(meta.get('ano', '')))
                yield text, doc_id, json.dumps(meta, ensure_ascii=False), str(meta.get('bib', '')), int(match[0]) if match else None
        db.executemany('INSERT INTO text_fts VALUES (?, ?, ?, ?, ?)', rows())
        db.execute('DELETE FROM index_state')
        db.execute('INSERT INTO index_state VALUES (?)', (fingerprint,))


def buscar_textual_indexada(query: str, n_results: int = 10, filtro_bib: str | None = None,
                            year_from: int | None = None, year_to: int | None = None,
                            db_path: Path | None = None) -> list[dict]:
    """Todas as palavras, sem distinção de acentos/caixa; sem inferir grafias."""
    terms = re.findall(r'[^\W_]+', query, flags=re.UNICODE)
    if not terms or (year_from is not None and year_to is not None and year_from > year_to):
        return []
    path = db_path or config.CACHE_DIR / 'text_search.sqlite'
    with _LOCK:
        _ensure_index(path)
    clauses, params = ['text_fts MATCH ?'], [' AND '.join('"' + word + '"' for word in terms)]
    if filtro_bib:
        clauses.append('bib = ?')
        params.append(filtro_bib)
    if year_from is not None:
        clauses.append('ano >= ?')
        params.append(year_from)
    if year_to is not None:
        clauses.append('ano <= ?')
        params.append(year_to)
    params.append(max(1, min(n_results, 150)))
    with sqlite3.connect(path) as db:
        rows = db.execute('SELECT doc_id, texto, metadata FROM text_fts WHERE ' + ' AND '.join(clauses) + ' ORDER BY rank LIMIT ?', params).fetchall()
    return [{'id': row[0], 'texto': row[1], 'metadata': json.loads(row[2]), 'score': 1.0,
             'modo': 'textual_indexado', 'matched_tokens': terms} for row in rows]
