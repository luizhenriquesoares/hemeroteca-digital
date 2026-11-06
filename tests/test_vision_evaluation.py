import unittest
from src.vision.evaluation import normalize, best_span, edit_distance, evaluate_result

class EvaluationTests(unittest.TestCase):
    def test_exact_excerpt_inside_long_page(self):
        text='Antes. Diario de Pernambuco. Depois.'
        edits,start,end=best_span('Diario de Pernambuco',text)
        self.assertEqual(edits,0)
        self.assertEqual(text[start:end],'Diario de Pernambuco')
    def test_missing_text_counts_deletions(self):
        self.assertEqual(best_span('Diario','')[0],6)
        with self.assertRaises(ValueError):best_span('', 'algo')
    def test_spelling_and_accents_are_not_normalized_away(self):
        self.assertEqual(normalize('  huma\n  casa '),'huma casa')
        self.assertNotEqual(normalize('um'),normalize('hum'))
        self.assertNotEqual(normalize('casa'),normalize('Casa'))
        self.assertEqual(normalize('ac\u0327a\u0303o'),'ação')
    def test_insertion_and_substitution_in_middle(self):
        self.assertEqual(best_span('Diario de Pernambuco','prefix Diario do Pernambucu suffix')[0],2)
        self.assertEqual(edit_distance(['um','Diario'],['hum','Diario']),1)
    def test_repeated_exact_excerpt_reported(self):
        reference={'status':'draft','excerpts':[{'id':'x','page':'p','text':'huma casa'}]}
        result={'mode':'full','page':'p','extraction':{'segments':[{'text':'huma casa. huma casa.'}]}}
        score=evaluate_result(reference,result)
        self.assertEqual(score['excerpt_cer'],0)
        self.assertEqual(score['checks'][0]['exact_occurrences'],2)
    def test_structure_and_failed_reads_are_not_ranked(self):
        reference={'status':'draft','excerpts':[]}
        self.assertIn('skipped',evaluate_result(reference,{'mode':'structure'}))
        self.assertIn('Falha',evaluate_result(reference,{'error':'401'})['skipped'])
