import {getNotebook,saveSource} from './experience.js';
import {normalizeDocument,chronologicalDocuments} from './experience-model.mjs';
import {setupAtlas} from './research-atlas.js';
import {setupStudio} from './research-studio.js';
const $=id=>document.getElementById(id);
const roster=[];let latestSources=[],lastSaved=getNotebook().at(-1),active='question';
const controllers=new Map();
const titles={question:'Uma pergunta, uma investigação',identities:'Detetive de identidades',connections:'Constelação de encontros',atlas:'Atlas através do tempo',clues:'A próxima pista',studio:'Estúdio de documentários'};
const status=message=>{$('labStatus').textContent=message;};
function element(tag,text,className=''){const e=document.createElement(tag);e.textContent=text;e.className=className;return e;}
function button(text,action,className='secondary'){const e=element('button',text,className);e.type='button';e.addEventListener('click',action);return e;}
async function api(channel,path,body){
    controllers.get(channel)?.abort();const controller=new AbortController();controllers.set(channel,controller);const timer=setTimeout(()=>controller.abort('timeout'),30000);
    try{const response=await fetch(path,{signal:controller.signal,...(body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{})});
        const data=await response.json();if(controller.signal.aborted)throw new DOMException('Cancelada','AbortError');if(!response.ok||data.error)throw new Error(typeof(data.error||data.detail)==='string'?(data.error||data.detail):'A consulta não pôde ser concluída.');return data;
    }catch(error){if(controller.signal.reason==='timeout')throw new Error('A consulta demorou mais que o esperado. Tente novamente.');throw error;}finally{clearTimeout(timer);}
}
function failure(error){if(error.name!=='AbortError')status(error.message||'Não foi possível concluir a consulta.');}
function sourceCard(raw){
    const doc=normalizeDocument(raw),card=element('article','','lab-card');
    card.append(element('p',`${doc.ano||'Ano não informado'} · ${doc.bib} · pág. ${doc.pagina}`,'eyebrow'),element('h3',doc.jornal||'Fonte do acervo'),element('blockquote',doc.quote||'Sem trecho disponível.'));
    card.append(button('Ler fonte ↗',()=>window.dispatchEvent(new CustomEvent('research:open-source',{detail:doc}))),button('＋ Guardar',()=>{saveSource(doc);status('Fonte disponível no caderno.');}));return card;
}
function listBlock(title,items,className=''){const box=element('div','',className);box.append(element('h3',title));const list=document.createElement('ul');items.forEach(item=>list.append(element('li',item)));box.append(list);return box;}
function download(name,text,type='text/markdown'){const a=document.createElement('a'),url=URL.createObjectURL(new Blob([text],{type}));a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
function readJSON(key,fallback){try{return JSON.parse(localStorage.getItem(key))||fallback;}catch{return fallback;}}
function writeJSON(key,value){try{localStorage.setItem(key,JSON.stringify(value));return true;}catch{status('Não foi possível salvar neste navegador. Exporte uma cópia.');return false;}}
const atlas=setupAtlas({$,api,status,failure,sourceCard,readJSON,writeJSON});
const studio=setupStudio({$,getNotebook,getSources:()=>latestSources,status,download});
function openLab(name){
    if(!titles[name])return;atlas.stop();studio.stop();active=name;status('');
    document.querySelectorAll('.lab-section').forEach(e=>e.hidden=e.id!==`lab-${name}`);
    document.querySelectorAll('.lab-tabs [data-lab]').forEach(e=>e.setAttribute('aria-current',e.dataset.lab===name?'page':'false'));
    $('labTitle').textContent=titles[name];if(!$('labDialog').open)$('labDialog').showModal();
    if(name==='atlas')atlas.open();if(name==='studio')studio.open();
    if(name==='clues'){document.querySelectorAll('[data-lab=clues]').forEach(e=>e.classList.remove('has-new-clues'));if(!$('clueResults').children.length)loadClues();}
}
document.querySelectorAll('[data-lab]').forEach(e=>e.addEventListener('click',()=>openLab(e.dataset.lab)));
$('closeLab').addEventListener('click',()=>$('labDialog').close());
$('labDialog').addEventListener('close',()=>{atlas.stop();studio.stop();for(const c of controllers.values())c.abort();});
document.addEventListener('visibilitychange',()=>{if(document.hidden)atlas.stop();});
window.addEventListener('research:results',event=>{latestSources=event.detail.mode==='text'?chronologicalDocuments(event.detail.items):[];});
window.addEventListener('research:entity',event=>{
    latestSources=chronologicalDocuments(event.detail.mentions||[]);
    const item=event.detail;if(roster.length<3&&!roster.some(i=>i.id===item.id)){roster.push({id:item.id,canonical_name:item.canonical_name});renderRoster();}
});
function renderRoster(){
    document.querySelectorAll('.identity-roster').forEach(host=>{host.replaceChildren();roster.forEach(item=>host.append(button(`${item.canonical_name} · #${item.id} ×`,()=>{roster.splice(roster.indexOf(item),1);renderRoster();},'identity-chip')));});
}
document.querySelectorAll('[data-picker]').forEach((host,index)=>{
    const form=element('form','','lab-form'),label=element('label','Encontre um registro pelo nome'),input=document.createElement('input');input.required=true;input.maxLength=120;input.placeholder='Bento Barroso Pereira';input.id=`labEntitySearch${index}`;label.htmlFor=input.id;label.append(input);form.append(label);const search=element('button','Procurar registros','secondary');form.append(search);
    const results=element('div','','picker-results'),selected=element('div','','identity-roster');host.append(form,results,selected);
    form.addEventListener('submit',async e=>{e.preventDefault();status('Procurando registros…');try{const data=await api(`picker${index}`,'/api/entity/search?'+new URLSearchParams({q:input.value,n:'10'}));results.replaceChildren();for(const item of data.resultados||[]){results.append(button(`${item.canonical_name} · #${item.id}`,()=>{if(roster.some(i=>i.id===item.id))return;if(roster.length===3){status('Retire um registro para escolher outro.');return;}roster.push(item);renderRoster();status('Registro incluído. Escolha outro para comparar.');},''));}status(data.total?'Escolha os registros encontrados.':'Nenhum registro encontrado. Experimente outra grafia.');}catch(error){failure(error);}});
});
$('runIdentity').addEventListener('click',async()=>{
    if(roster.length<2){status('Escolha dois ou três registros pelo nome.');return;}status('Confrontando registros e fontes…');$('identityResults').replaceChildren();
    try{const data=await api('identity','/api/lab/identities?ids='+roster.map(r=>r.id).join(','));const grid=element('div','','lab-results-grid');
        data.profiles.forEach(p=>{const card=element('article','','lab-card');card.append(element('p',`REGISTRO #${p.id} · ${{person:"Pessoa",institution:"Instituição",place:"Lugar"}[p.type]||p.type}`,'eyebrow'),element('h3',p.canonical_name),element('p','Grafias: '+(p.aliases.join(', ')||'nenhuma adicional')),element('p','Anos da amostra: '+(p.years.join(', ')||'não informados')));
            card.append(element('h4','Contextos extraídos'));
            p.contexts.forEach(ctx=>card.append(button(`${ctx.label||ctx.predicate} → ${ctx.object_name}`,async()=>{try{const evidence=await api('proof','/api/graph/relation-evidence?relation_id='+ctx.relation_id);const host=element('div','','lab-results-grid');evidence.evidences.forEach(s=>host.append(sourceCard(s)));if(!evidence.evidences.length)host.append(element('p','Sem evidência disponível para esta extração.'));card.append(host);}catch(error){failure(error);}},'identity-context')));
            const details=document.createElement('details');details.append(element('summary',`${p.mentions.length} fontes da amostra`));p.mentions.forEach(source=>details.append(sourceCard(source)));card.append(details);grid.append(card);
        });$('identityResults').append(grid);
        data.comparisons.forEach(pair=>{const names=data.profiles.filter(p=>[pair.left,pair.right].includes(p.id)).map(p=>p.canonical_name).join(' ↔ '),box=element('article','','lab-card');box.append(element('h3',names),listBlock('Sinais para investigar',pair.signals.length?pair.signals:['Não foram encontrados sinais em comum nesta amostra.'],'signals'),listBlock('Pontos de atenção',pair.checks,'checks'));$('identityResults').append(box);});status(data.scope);
    }catch(error){failure(error);}
});
$('runConnections').addEventListener('click',async()=>{
    if(roster.length!==2){status('Escolha exatamente dois registros. Retire o terceiro, se houver.');return;}status('Buscando caminhos com evidência…');$('connectionResults').replaceChildren();
    try{const data=await api('connections','/api/lab/connections?'+new URLSearchParams({left:roster[0].id,right:roster[1].id}));const names=new Map(data.nodes.map(n=>[n.id,n.canonical_name]));
        if(!data.paths.length)$('connectionResults').append(element('p','Nenhum caminho com evidência foi encontrado no recorte consultado. Isso não demonstra ausência de vínculo.','lab-empty'));
        data.paths.forEach(path=>{const chain=element('div','','path-chain');path.nodes.forEach((id,i)=>{if(i)chain.append(element('span','—','path-arrow'));chain.append(button(names.get(id),()=>{$('labDialog').close();window.dispatchEvent(new CustomEvent('research:select-entity',{detail:{id}}));}));});$('connectionResults').append(chain);const proof=element('div','','path-evidence');path.relations.forEach(r=>{const col=document.createElement('div');col.append(element('p',`${names.get(r.subject_entity_id)} → ${r.label||r.predicate} → ${names.get(r.object_entity_id)} (${{hypothesis:"hipótese",confirmed:"confirmada",approved:"aprovada",probable:"provável"}[r.effective_status]||r.effective_status})`));r.evidence.forEach(s=>col.append(sourceCard(s)));proof.append(col);});$('connectionResults').append(proof);});
        $('connectionResults').append(element('h3','Na mesma página · coocorrência'));const shared=element('div','','lab-results-grid');data.shared_pages.forEach(s=>shared.append(sourceCard(s)));if(!data.shared_pages.length)shared.append(element('p','Sem páginas compartilhadas nesta consulta.'));$('connectionResults').append(shared);status(data.scope+(data.truncated?' Limite de relações atingido.':''));
    }catch(error){failure(error);}
});
let cases=readJSON('hemeroteca.cases.v1',[]);if(!Array.isArray(cases))cases=[];
function renderCase(data){
    const host=$('caseResults');host.replaceChildren();host.append(element('h3',data.question),element('p','Consultas realizadas: '+data.queries.join(' · ')),element('p',data.finding,'lab-empty'));
    const label=element('label','Minha hipótese · ainda a verificar'),note=element('textarea','','lab-note');note.value=data.note||'';note.maxLength=10000;label.append(note);host.append(label);
    note.addEventListener('input',()=>{data.note=note.value;writeJSON('hemeroteca.cases.v1',cases);});
    host.append(listBlock('O que falta conferir',data.gaps,'checks'),listBlock('Próximos passos',data.steps,'signals'));
    const grid=element('div','','lab-results-grid');data.sources.forEach(source=>grid.append(sourceCard(source)));host.append(grid);
    host.append(button('Guardar fontes no caderno',()=>{data.sources.forEach(saveSource);status('Fontes reunidas no caderno.');}),button('Exportar investigação ↓',()=>download('investigacao-hemeroteca.md',`# ${data.question}\n\nBuscas: ${data.queries.join('; ')}\n\n${data.finding}\n\n## Hipótese do pesquisador\n\n${data.note||'Ainda não formulada.'}\n\n## Lacunas\n\n${data.gaps.map(x=>'- '+x).join('\n')}\n\n## Fontes candidatas\n\n${data.sources.map(s=>`${s.ano||'?'} · ${s.bib} · ${s.pagina}\n\n> ${s.quote.replaceAll('\n','\n> ')}\n\n${location.origin}/page/${encodeURIComponent(s.bib)}/${encodeURIComponent(s.pagina)}`).join('\n\n---\n\n')}`)));
    if(cases.length>1){const history=document.createElement('details');history.append(element('summary','Retomar outra pergunta'));cases.forEach(c=>history.append(button(c.question,()=>renderCase(c))));host.append(history);}
}
if(cases.length){$('labQuestion').value=cases[0].question;renderCase(cases[0]);}
$('questionForm').addEventListener('submit',async e=>{
    e.preventDefault();status('Reunindo fontes candidatas…');try{const data=await api('question','/api/lab/investigate',{question:$('labQuestion').value,terms:$('questionTerms').value.split(';').map(t=>t.trim()).filter(Boolean)});cases.unshift(data);cases=cases.slice(0,10);writeJSON('hemeroteca.cases.v1',cases);renderCase(data);latestSources=chronologicalDocuments(data.sources);status(data.queries.length?'Investigação organizada. A resposta exige leitura e conferência.':'Informe nomes ou palavras no campo de busca para reunir fontes.');}catch(error){failure(error);}
});
async function loadClues(){
    lastSaved=getNotebook().at(-1);if(!lastSaved){$('clueResults').replaceChildren(element('p','Guarde uma fonte no caderno para encontrar a próxima pista.','lab-empty'));return;}
    const host=$('clueResults');host.replaceChildren(element('p','Buscando registros que reaparecem em outras páginas…'));
    try{const data=await api('clues','/api/lab/clues?'+new URLSearchParams({bib:lastSaved.bib,page:lastSaved.pagina}));host.replaceChildren(element('p',`Partida: ${lastSaved.bib} · página ${lastSaved.pagina}`,'small'));
        if(!data.clues.length)host.append(element('p','Nenhuma pista estruturada disponível para esta fonte. Experimente buscar um nome do trecho.','lab-empty'));
        data.clues.forEach(clue=>{const card=element('article','','lab-card');card.append(element('h3',clue.name),element('p',clue.reason),button('Explorar este registro ↗',()=>{$('labDialog').close();window.dispatchEvent(new CustomEvent('research:select-entity',{detail:{id:clue.entity_id}}));}));const sources=element('div','','lab-results-grid');clue.sources.forEach(s=>sources.append(sourceCard(s)));card.append(sources);host.append(card);});host.append(element('p',data.scope,'small'));
    }catch(error){host.replaceChildren(element('p','Não foi possível carregar as pistas. Use o botão para tentar novamente.'));if(active==='clues')failure(error);}
}
$('refreshClues').addEventListener('click',loadClues);
window.addEventListener('research:saved',()=>{if(active==='clues'&&$('labDialog').open)loadClues();else $('clueResults').replaceChildren();document.querySelectorAll('[data-lab=clues]').forEach(e=>{e.title='Uma nova fonte foi salva. Consulte as próximas pistas.';e.classList.add('has-new-clues');});});
