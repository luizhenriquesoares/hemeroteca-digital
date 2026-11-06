import json

from src.processing import text_index


def test_index_filters_before_limit_and_refreshes(tmp_path, monkeypatch):
    chunks = tmp_path / 'chunks' / 'bib'
    chunks.mkdir(parents=True)
    source = chunks / 'chunks.jsonl'
    monkeypatch.setattr(text_index.config, 'CHUNKS_DIR', chunks.parent)
    monkeypatch.setattr(text_index.config, 'TEXT_DIR', tmp_path / 'texts')
    db = tmp_path / 'index.sqlite'
    def write(items):
        source.write_text('\n'.join(json.dumps({'id': str(i), 'text': text, 'metadata': {'ano': year, 'bib': bib}}) for i, (text, year, bib) in enumerate(items)))
    write([('José Botelho', 'Ano 1825', 'a'), ('José Botelho no Recife', 'Ano 1836', 'b'), ('José Botelho', '?', 'b')])
    result = text_index.buscar_textual_indexada('jose botelho', 1, year_from=1830, year_to=1840, db_path=db)
    assert [r['id'] for r in result] == ['1']
    assert not text_index.buscar_textual_indexada('jose botelho', filtro_bib='a', year_from=1830, db_path=db)
    assert not text_index.buscar_textual_indexada('botelho desconhecido', db_path=db)
    write([('Outro nome', 'Ano 1836', 'a')])
    assert not text_index.buscar_textual_indexada('Botelho', db_path=db)
    assert text_index.buscar_textual_indexada('Outro', db_path=db)


def test_index_treats_fts_operators_as_words(tmp_path, monkeypatch):
    chunks = tmp_path / 'chunks' / 'bib'
    chunks.mkdir(parents=True)
    (chunks / 'chunks.jsonl').write_text(json.dumps({'id': '1', 'text': 'Botelho', 'metadata': {}}))
    monkeypatch.setattr(text_index.config, 'CHUNKS_DIR', chunks.parent)
    db = tmp_path / 'index.sqlite'
    assert text_index.buscar_textual_indexada('"Botelho"*', db_path=db)
    assert not text_index.buscar_textual_indexada('Botelho OR ausente', db_path=db)
    assert not text_index.buscar_textual_indexada('***', db_path=db)
