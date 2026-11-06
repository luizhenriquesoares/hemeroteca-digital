"""Ferramentas documentais locais: resultados rastreáveis, sem inferência de identidade."""
from __future__ import annotations

import json
import re
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path

from src.config import STRUCTURED_DB
from src.processing.search_profile import normalize_text
from src.structured.quality import assess_discovery_noise
from src.structured.graph_store import PREDICATE_LABELS

LABELS={**PREDICATE_LABELS,"presides_over":"preside","president_of":"presidente de","member_of":"membro de","responded_to":"respondeu a","role_of":"cargo de","exercised":"exerceu","knows":"conhece"}


def connect(path=STRUCTURED_DB):
    db=sqlite3.connect(path);db.row_factory=sqlite3.Row
    return db


def year(value):
    match=re.search(r'\b(1\d{3}|20\d{2})\b',str(value or ''))
    return int(match[0]) if match else None


def page_aliases(bib, page):
    raw=str(page);match=re.search(r'(\d+)$',raw)
    if not match:return [raw]
    number=int(match[0]);return list(dict.fromkeys([raw,str(number),f'{bib}_{number:05d}']))


def profile(entity_id, db_path=STRUCTURED_DB):
    with connect(db_path) as db:
        entity=db.execute('SELECT id,type,canonical_name,aliases_json FROM entities WHERE id=?',(entity_id,)).fetchone()
        if not entity:return None
        payload=dict(entity)
        try:payload['aliases']=json.loads(payload.pop('aliases_json'))
        except (ValueError,TypeError):payload['aliases']=[]
        payload['mentions']=[dict(r) for r in db.execute('''SELECT p.bib,p.pagina,p.jornal,p.ano,m.snippet AS quote FROM entity_mentions m JOIN pages p ON p.id=m.page_id WHERE m.entity_id=? GROUP BY p.id ORDER BY p.id LIMIT 40''',(entity_id,))]
        payload['years']=sorted({y for m in payload['mentions'] if (y:=year(m['ano']))})
        payload['contexts']=[dict(r) for r in db.execute('''SELECT r.id AS relation_id,r.predicate,r.subject_entity_id,r.object_entity_id,COALESCE(e.canonical_name,NULLIF(r.object_literal,''),'?') AS object_name,
            COALESCE((SELECT review_status FROM relation_reviews WHERE relation_id=r.id ORDER BY id DESC LIMIT 1),r.status) AS effective_status
            FROM relations r LEFT JOIN entities e ON e.id=r.object_entity_id WHERE r.subject_entity_id=? AND r.predicate NOT IN ('mentioned_with','co_occurs_with') ORDER BY r.confidence DESC LIMIT 40''',(entity_id,))]
        payload['contexts']=[{**r,'label':LABELS.get(r['predicate'],r['predicate'])} for r in payload['contexts'] if r['effective_status']!='rejected']
        return payload


def compare_identities(ids, db_path=STRUCTURED_DB):
    profiles=[p for i in dict.fromkeys(ids) if (p:=profile(i,db_path))]
    if len(profiles)<2:return {'error':'Escolha ao menos dois registros existentes.'}
    pairs=[]
    for i,left in enumerate(profiles):
        for right in profiles[i+1:]:
            common_years=sorted(set(left['years'])&set(right['years']))
            a={(x['predicate'],normalize_text(x['object_name'])) for x in left['contexts']}
            b={(x['predicate'],normalize_text(x['object_name'])) for x in right['contexts']}
            common=[{'predicate':k,'value':v} for k,v in sorted(a&b)]
            similarity=SequenceMatcher(None,normalize_text(left['canonical_name']),normalize_text(right['canonical_name'])).ratio()
            signals=[];checks=[]
            if similarity>=.7:signals.append('Grafias semelhantes; isso também ocorre com homônimos.')
            if common_years:signals.append('Anos de menção em comum: '+', '.join(map(str,common_years))+'.')
            if common:signals.append('Há contextos extraídos em comum. Abra as evidências para conferir.')
            if left['type']!=right['type']:checks.append('Tipos de entidade diferentes nos registros; pode haver erro de extração.')
            if left['years'] and right['years'] and not common_years:checks.append('Não há anos em comum nesta amostra. Isso não demonstra identidades diferentes.')
            if a and b and a!=b:checks.append('Os contextos extraídos diferem. Ocupações ou endereços diferentes podem refletir mudanças ao longo da vida.')
            if not a or not b:checks.append('Um dos registros não possui contextos estruturados nesta amostra; isso limita a comparação.')
            checks.append('Confira filiação, datas e contexto no original antes de reunir os registros.')
            pairs.append({'left':left['id'],'right':right['id'],'signals':signals,'checks':checks,'shared_contexts':common})
    return {'profiles':profiles,'comparisons':pairs,'scope':'Até 40 páginas e 40 relações por registro. Nenhuma identidade foi fundida.'}


def connection_paths(left, right, db_path=STRUCTURED_DB):
    if left==right:return {'error':'Escolha dois registros diferentes.'}
    with connect(db_path) as db:
        people=[dict(r) for r in db.execute('SELECT id,canonical_name,type FROM entities WHERE id IN (?,?)',(left,right))]
        if len(people)!=2:return {'error':'Registro não encontrado.'}
        def neighbors(entity):
            rows=db.execute('''SELECT r.id,r.subject_entity_id,r.object_entity_id,r.predicate,
                COALESCE((SELECT review_status FROM relation_reviews WHERE relation_id=r.id ORDER BY id DESC LIMIT 1),r.status) AS effective_status
                FROM relations r WHERE (r.subject_entity_id=? OR r.object_entity_id=?) AND r.object_entity_id IS NOT NULL
                AND r.predicate NOT IN ('mentioned_with','co_occurs_with')
                AND EXISTS(SELECT 1 FROM relation_evidence re WHERE re.relation_id=r.id AND trim(re.quote)<>'')
                ORDER BY r.confidence DESC,r.id LIMIT 251''',(entity,entity)).fetchall()
            return [dict(r) for r in rows[:250] if r['effective_status']!='rejected'],len(rows)>250
        a,cut_a=neighbors(left);b,cut_b=neighbors(right)
        other=lambda r,who:r['object_entity_id'] if r['subject_entity_id']==who else r['subject_entity_id']
        paths=[]
        for r in a:
            if other(r,left)==right:paths.append({'nodes':[left,right],'relations':[r]})
        by_neighbor={}
        for r in b:by_neighbor.setdefault(other(r,right),[]).append(r)
        for r in a:
            middle=other(r,left)
            if middle in (left,right):continue
            for last in by_neighbor.get(middle,[])[:2]:
                paths.append({'nodes':[left,middle,right],'relations':[r,last]})
                if len(paths)>=12:break
            if len(paths)>=12:break
        paths=paths[:12];ids={n for p in paths for n in p['nodes']}|{left,right}
        nodes=[dict(r) for r in db.execute('SELECT id,canonical_name,type FROM entities WHERE id IN ('+','.join('?' for _ in ids)+')',list(ids))]
        for path in paths:
            for relation in path['relations']:
                relation['label']=LABELS.get(relation['predicate'],relation['predicate'])
                relation['evidence']=[dict(r) for r in db.execute('''SELECT p.bib,p.pagina,p.ano,p.jornal,re.quote FROM relation_evidence re JOIN pages p ON p.id=re.page_id WHERE re.relation_id=? AND trim(re.quote)<>'' ORDER BY re.confidence DESC LIMIT 2''',(relation['id'],))]
        shared=[dict(r) for r in db.execute('''SELECT p.bib,p.pagina,p.ano,p.jornal,m.snippet AS quote FROM entity_mentions m JOIN pages p ON p.id=m.page_id WHERE m.entity_id=? AND EXISTS(SELECT 1 FROM entity_mentions other WHERE other.page_id=m.page_id AND other.entity_id=?) GROUP BY p.id LIMIT 8''',(left,right))]
    return {'nodes':nodes,'paths':paths,'shared_pages':shared,'truncated':cut_a or cut_b,'scope':'Até duas ligações, 250 relações por ponta e 12 caminhos. Coocorrências ficam separadas; ausência de caminho não prova ausência de vínculo.'}


def next_clues(bib,page,db_path=STRUCTURED_DB):
    aliases=page_aliases(bib,page)
    with connect(db_path) as db:
        rows=db.execute('SELECT id FROM pages WHERE bib=? AND pagina IN ('+','.join('?' for _ in aliases)+')',[bib,*aliases]).fetchall()
        page_ids=[r['id'] for r in rows]
        if not page_ids:return {'clues':[],'scope':'Esta página não tem extrações estruturadas disponíveis.'}
        clause=','.join('?' for _ in page_ids)
        entities=db.execute(f'''SELECT e.id,e.canonical_name,e.type,MAX(m.confidence) AS confidence FROM entity_mentions m JOIN entities e ON e.id=m.entity_id WHERE m.page_id IN ({clause}) AND length(e.canonical_name)>=6 AND m.confidence>=0.75 GROUP BY e.id ORDER BY confidence DESC,e.id LIMIT 40''',page_ids).fetchall()
        clues=[];seen_names=set()
        for ent in entities:
            name_key=normalize_text(ent['canonical_name'])
            if name_key.split()[:1] and name_key.split()[0] in {'pela','pelo','pelas','pelos','professor','professora','gramaticas','gramatica'}:
                continue
            if name_key in seen_names or assess_discovery_noise(entity_type=ent['type'],canonical_name=ent['canonical_name'])['is_probable_noise']:
                continue
            seen_names.add(name_key)
            other=db.execute(f'''SELECT p.bib,p.pagina,p.ano,p.jornal,m.snippet AS quote FROM entity_mentions m JOIN pages p ON p.id=m.page_id WHERE m.entity_id=? AND m.page_id NOT IN ({clause}) GROUP BY p.id ORDER BY p.id DESC LIMIT 2''',[ent['id'],*page_ids]).fetchall()
            if other:clues.append({'entity_id':ent['id'],'name':ent['canonical_name'],'type':ent['type'],'reason':'Este registro foi extraído da página salva e também aparece nas fontes abaixo. Confira possíveis homônimos.','sources':[dict(r) for r in other]})
            if len(clues)==6:break
        return {'clues':clues,'scope':'Sugestões por registro compartilhado, não comprovação de identidade ou contato.'}


def question_terms(question):
    quoted=re.findall(r'["“]([^"”]+)["”]',question)
    if quoted:return [q.strip()[:100] for q in quoted[:3] if q.strip()]
    stop=set('como quando onde porque quem qual quais essa esse esta este familia chegou chegada cidade historia historia investigar descobrir sobre para pelo pela pelos pelas uma uns umas dos das que foi eram era foram sao tem pode posso entre antes depois desde anos ano recife pernambuco'.split())
    words=[w for w in re.findall(r'[^\W\d_]+',question,flags=re.UNICODE) if len(w)>3 and normalize_text(w) not in stop]
    return list(dict.fromkeys(words))[:3]


def investigate(question, terms=None):
    from src.processing.text_index import buscar_textual_indexada
    from src.processing.search import extract_evidence_snippet
    queries=terms or question_terms(question)
    results=[];seen=set()
    for query in queries[:3]:
        for item in buscar_textual_indexada(query,n_results=8):
            meta=item['metadata'];key=(meta.get('bib'),str(meta.get('pagina')))
            if key in seen:continue
            seen.add(key);results.append({**meta,'quote':extract_evidence_snippet(query,item['texto']),'query':query})
    return {'question':question,'queries':queries,'sources':results[:18],
            'finding':f'{len(results[:18])} fontes candidatas reunidas por busca de palavras. Ainda não respondem, por si, à pergunta.',
            'gaps':['Conferir as transcrições nas imagens originais.','Identificar homônimos e verificar datas e locais.','A ausência de uma ocorrência na amostra não prova ausência no acervo.'],
            'steps':['Leia e guarde os trechos relevantes.','Compare os registros no Detetive de identidades.','Registre sua hipótese e procure uma fonte que possa contradizê-la.']}
