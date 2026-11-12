import test from 'node:test';
import assert from 'node:assert/strict';
import {documentKey, chronologicalDocuments, normalizeDocument, notebookMarkdown} from '../frontend/assets/experience-model.mjs';

test('a mesma página numérica ou com prefixo não vira duas cenas',()=>{
  assert.equal(documentKey({bib:'029033_01',pagina:'029033_01_00001'}),documentKey({metadata:{bib:'029033_01',pagina:1}}));
  assert.notEqual(documentKey({bib:'a',pagina:'1'}),documentKey({bib:'b',pagina:'1'}));
});
test('sequência ordena anos conhecidos, mantém sem ano no fim e preserva fontes',()=>{
  const sources=chronologicalDocuments([
    {bib:'a',pagina:3,ano:'?',quote:'sem ano'},
    {bib:'a',pagina:2,ano:'Ano 1836',quote:'segunda'},
    {bib:'a',pagina:1,ano:'Ano 1825',quote:'primeira'},
    {bib:'a',pagina:'a_00001',ano:'Ano 1825',quote:'duplicata'},
    {quote:'sem referência'},
  ]);
  assert.deepEqual(sources.map(s=>s.pagina),['1','2','3']);
  assert.equal(sources[0].quote,'primeira');
  assert.equal(sources[2].ano,'');
});
test('caderno conserva nota e separa hipótese de transcrição na exportação',()=>{
  const doc=normalizeDocument({metadata:{bib:'a',pagina:2,ano:'1830'},evidencia:'Trecho\noriginal',note:'Conferir homônimo.'});
  const md=notebookMarkdown([doc],'http://localhost:8765');
  assert.match(md,/http:\/\/localhost:8765\/page\/a\/2/);
  assert.match(md,/> Trecho\n> original/);
  assert.match(md,/Minhas notas\n\n> Conferir homônimo/);
  assert.match(md,/hipóteses pessoais/);
});
