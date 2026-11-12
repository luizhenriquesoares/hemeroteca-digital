import sqlite3
import pytest
from src.structured.schema import SCHEMA_SQL
from src.web.research_lab_service import compare_identities,connection_paths,next_clues,question_terms,investigate

@pytest.fixture
def lab_db(tmp_path):
    path=tmp_path/'lab.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA_SQL)
        for i,name in [(1,'Manoel Ribeiro'),(2,'Manuel Ribeiro'),(3,'Antonio Costa')]:
            db.execute('INSERT INTO entities(id,type,canonical_name,normalized_name) VALUES (?,\'person\',?,?)',(i,name,name.lower()))
        db.executemany('INSERT INTO pages(id,bib,pagina,ano) VALUES (?,\'a\',?,?)',[(1,'a_00001','Ano 1830'),(2,'a_00002','Ano 1831'),(3,'a_00003','?')])
        for i,e,p in [(1,1,1),(2,2,1),(3,1,2),(4,3,2)]:
            db.execute("INSERT INTO entity_mentions(id,entity_id,page_id,chunk_id,surface_form,snippet,confidence,source_text) VALUES (?,?,?,'c','Nome','Evidência textual',.95,'Fonte')",(i,e,p))
        for i,a,b,pred in [(1,1,3,'knows'),(2,3,2,'knows'),(3,1,2,'mentioned_with')]:
            db.execute("INSERT INTO relations(id,subject_entity_id,object_entity_id,predicate,confidence,status,extraction_method) VALUES (?,?,?,?,.9,'hypothesis','test')",(i,a,b,pred))
            db.execute("INSERT INTO relation_evidence(relation_id,page_id,chunk_id,quote,confidence) VALUES (?,1,'c','Texto que sustenta a extração',.9)",(i,))
    return path

def test_paths_keep_direction_and_distinguish_cooccurrence(lab_db):
    data=connection_paths(1,2,lab_db)
    assert len(data['paths'])==1
    assert data['paths'][0]['nodes']==[1,3,2]
    assert [r['id'] for r in data['paths'][0]['relations']]==[1,2]
    assert data['paths'][0]['relations'][1]['subject_entity_id']==3
    assert data['paths'][0]['relations'][0]['evidence'][0]['pagina']=='a_00001'
    assert data['shared_pages'][0]['pagina']=='a_00001'

def test_latest_review_removes_rejected_path(lab_db):
    with sqlite3.connect(lab_db) as db:
        db.execute("INSERT INTO relation_reviews(relation_id,review_status) VALUES(2,'confirmed')")
        db.execute("INSERT INTO relation_reviews(relation_id,review_status) VALUES(2,'rejected')")
    assert not connection_paths(1,2,lab_db)['paths']
    with sqlite3.connect(lab_db) as db:
        db.execute("INSERT INTO relation_reviews(relation_id,review_status) VALUES(2,'confirmed')")
    assert connection_paths(1,2,lab_db)['paths']

def test_unsubstantiated_edges_are_not_paths(lab_db):
    with sqlite3.connect(lab_db) as db:db.execute('DELETE FROM relation_evidence WHERE relation_id=2')
    assert not connection_paths(1,2,lab_db)['paths']

def test_identity_comparison_does_not_merge_records(lab_db):
    data=compare_identities([1,2],lab_db)
    assert [p['id'] for p in data['profiles']]==[1,2]
    assert data['profiles'][0]['years']==[1830,1831]
    assert data['comparisons'][0]['signals']
    with sqlite3.connect(lab_db) as db:assert db.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==3

def test_clues_resolve_numeric_page_and_exclude_source(lab_db):
    clues=next_clues('a','1',lab_db)['clues']
    assert clues[0]['entity_id']==1
    assert {s['pagina'] for c in clues for s in c['sources']}=={'a_00002'}

def test_question_terms_are_explicit_and_not_an_answer(monkeypatch):
    assert question_terms('Como a família "Botelho" chegou ao Recife?')==['Botelho']
    assert question_terms('Como a família Botelho chegou ao Recife?')==['Botelho']
    from src.processing import text_index
    monkeypatch.setattr(text_index,'buscar_textual_indexada',lambda *a,**k:[])
    data=investigate('Como a família "Botelho" chegou ao Recife?')
    assert data['sources']==[]
    assert 'Ainda não respondem' in data['finding']
