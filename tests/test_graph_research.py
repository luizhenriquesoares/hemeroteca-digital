import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.structured.graph_store import get_relation_evidence, get_layered_graph
from src.structured.repository import StructuredRepository
from src.structured.service import process_chunk


class ResearchGraphTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name)/'graph.db'
        repo = StructuredRepository(self.db)
        process_chunk({'id':'test_chunk', 'text':'João Affonso Botelho compareceu ao acto.',
                       'metadata':{'bib':'029033_01','pagina':'029033_01_00001','ano':'1834','jornal':'Diario'}}, repo)
        with sqlite3.connect(self.db) as conn:
            self.person=conn.execute('SELECT id FROM entities WHERE type = "person" LIMIT 1').fetchone()[0]
            self.page=conn.execute('SELECT id FROM pages LIMIT 1').fetchone()[0]

    def relation(self, value):
        with sqlite3.connect(self.db) as conn:
            cur=conn.execute('''INSERT INTO relations(subject_entity_id,predicate,object_literal,confidence,status,extraction_method)
                                VALUES (?, 'holds_role', ?, .8, 'hypothesis', 'test')''',(self.person,value))
            return cur.lastrowid

    def test_literal_evidence_and_latest_review_are_exact(self):
        a=self.relation('Tenente de infantaria')
        b=self.relation('Escrivão de outro juízo')
        with sqlite3.connect(self.db) as conn:
            for rid,quote in [(a,'O Tenente João Affonso Botelho'),(b,'Outra fonte independente')]:
                conn.execute('INSERT INTO relation_evidence(relation_id,page_id,chunk_id,quote,confidence) VALUES (?,?,?,?,.8)',(rid,self.page,'test_chunk',quote))
            for status in ['approved','rejected']:
                conn.execute('INSERT INTO relation_reviews(relation_id,review_status) VALUES (?,?)',(a,status))
        data=get_relation_evidence(a,self.db)
        self.assertEqual(data['object_name'],'Tenente de infantaria')
        self.assertEqual(data['effective_status'],'rejected')
        self.assertEqual([e['quote'] for e in data['evidences']],['O Tenente João Affonso Botelho'])
        self.assertTrue(data['evidences'][0]['page_view_url'].startswith('/page/029033_01/'))
        self.assertIn('error',get_relation_evidence(999999,self.db))

    def test_long_literals_do_not_merge_in_global_or_focused_graph(self):
        common='Oficial responsável pela administração da repartição '
        for ending in ['do Recife','de Olinda']:self.relation(common+ending)
        for focus in [None,self.person]:
            data=get_layered_graph(db_path=self.db,layers=['roles'],limit=20,focus_entity_id=focus,focus_depth=1)
            nodes=[n for n in data['nodes'] if n.get('literal_value','').startswith(common)]
            self.assertEqual(len(nodes),2)
            self.assertEqual(len({n['id'] for n in nodes}),2)
            self.assertEqual({n['literal_value'] for n in nodes},{common+'do Recife',common+'de Olinda'})

    def test_deselecting_all_layers_does_not_reenable_defaults(self):
        self.relation('Tenente de infantaria')
        data=get_layered_graph(db_path=self.db,layers=[])
        self.assertEqual(data['layers_active'],[])
        self.assertEqual(data['edges'],[])
        self.assertEqual(data['nodes'],[])

class GraphPresentationTests(unittest.TestCase):
    def test_grouping_keeps_all_provenance_and_distinct_predicates(self):
        from src.structured.graph_store import group_parallel_edges
        graph={'nodes':[{'id':'a'},{'id':'b'},{'id':'orphan'}], 'edges':[
            {'source':'a','target':'b','predicate':'spouse_of','layer':'family','relation_id':1},
            {'source':'a','target':'b','predicate':'spouse_of','layer':'family','relation_id':2},
            {'source':'a','target':'b','predicate':'parent_of','layer':'family','relation_id':3}]}
        output=group_parallel_edges(graph)
        self.assertEqual(len(output['edges']),2)
        self.assertEqual(output['edges'][0]['relation_ids'],[1,2])
        self.assertEqual(len(output['nodes']),2)
        self.assertEqual(len(graph['edges']),3)
