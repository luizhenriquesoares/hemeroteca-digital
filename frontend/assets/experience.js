import {chronologicalDocuments, normalizeDocument, documentKey, notebookMarkdown} from './experience-model.mjs';

const el = id => document.getElementById(id);
const text = (id, value) => { el(id).textContent = value; };
const STORAGE = 'hemeroteca.notebook.v1';
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
let sources = [], sourceTitle = '', sourceQuery = '', currentSource = null, notebook = [], toastTimer;
const film = {items: [], index: 0, playing: false, timer: null, frame: null, started: 0, generation: 0, loading: false};
const pageCache = new Map();
const comparison=new Set();
let comparisonGeneration=0;
function toast(message) {
    clearTimeout(toastTimer); text('experienceToast', message); el('experienceToast').classList.add('visible');
    toastTimer = setTimeout(() => el('experienceToast').classList.remove('visible'), 4000);
}
function button(label, fn, className='secondary') {
    const result=document.createElement('button'); result.type='button'; result.className=className; result.textContent=label; result.addEventListener('click',fn); return result;
}
function readSource(item) {
    pauseFilm(); window.dispatchEvent(new CustomEvent('research:open-source',{detail:{...item,highlight:el('filmDialog').open?film.highlight:''}}));
}
function updateSources(items, title) {
    sources=chronologicalDocuments(items); sourceTitle=title;
    el('filmResults').disabled=!sources.length;
    text('filmResults', sources.length ? `▶ Percorrer ${sources.length} fontes como filme` : '▶ Ver documentos como filme');
    text('researchContext', sources.length ? `${sources.length} páginas distintas · ${title}` : title);
}
window.addEventListener('research:results', event => {
    sourceQuery=event.detail.query;
    if(event.detail.mode==='text')updateSources(event.detail.items, `Busca por “${event.detail.query}”`);
    else updateSources([], 'Selecione uma entidade para percorrer suas fontes');
});
window.addEventListener('research:entity', event => {
    const entity=event.detail; sourceQuery=entity.canonical_name; updateSources(entity.mentions||[], entity.canonical_name);
    const trail=el('researchTrail'); trail.hidden=false;
    const id=String(entity.id);
    if(![...trail.children].some(child=>child.dataset.entity===id)) {
        const step=button(entity.canonical_name, ()=>window.dispatchEvent(new CustomEvent('research:select-entity',{detail:{id:entity.id}})),'trail-step');
        step.dataset.entity=id; trail.append(step); if(trail.children.length>8)trail.firstElementChild.remove();
    }
    for(const child of trail.children)child.setAttribute('aria-current',child.dataset.entity===id?'step':'false');
});
window.addEventListener('research:source',event=>{currentSource=normalizeDocument(event.detail);updateReaderSave();});

try {
    const saved=JSON.parse(localStorage.getItem(STORAGE)||'[]');
    if(Array.isArray(saved))notebook=saved.filter(i=>i&&typeof i==='object'&&documentKey(i)).slice(0,100).map(normalizeDocument);
}catch { toast('Não foi possível recuperar o caderno deste navegador.'); }
function persist() {
    try { localStorage.setItem(STORAGE,JSON.stringify(notebook));return true; }
    catch { toast('O navegador não permitiu salvar. Exporte o caderno para não perder suas notas.');return false; }
}
function updateComparison(){text('compareSources',`Confrontar ${comparison.size}/2 fontes ↔`);el('compareSources').disabled=comparison.size!==2;}
function updateCount(){text('notebookCount', notebook.length);el('exportNotebook').disabled=!notebook.length;el('filmNotebook').disabled=!notebook.length;}
function saveSource(item) {
    const doc=normalizeDocument(item);if(!doc.key)return;
    if(notebook.some(existing=>existing.key===doc.key)){toast('Esta página já está no seu caderno.');if(el('filmDialog').open)text('filmStatus','Esta página já está no seu caderno.');return;}
    if(notebook.length>=100){toast('Seu caderno chegou a 100 fontes. Exporte uma cópia antes de continuar.');return;}
    notebook.push(doc);const saved=persist();window.dispatchEvent(new CustomEvent('research:saved',{detail:doc}));updateCount();updateReaderSave();
    if(el('notebookDialog').open)renderNotebook();
    if(saved)toast('Fonte guardada. Suas notas ficam no caderno.');
    if(el('filmDialog').open){text('filmSave','✓ Fonte no caderno');text('filmStatus',saved?'Fonte guardada no caderno.':'Fonte na memória. Exporte seu caderno para guardar.');}
}
const readerSave=button('＋ Guardar fonte',()=>{if(currentSource)saveSource(currentSource);});
readerSave.id='saveCurrentSource';el('sourceDialog').querySelector('.reader-head').insertBefore(readerSave,el('closeSource'));
function updateReaderSave(){readerSave.disabled=!currentSource;readerSave.textContent=currentSource&&notebook.some(i=>i.key===currentSource.key)?'✓ Fonte no caderno':'＋ Guardar fonte';}
window.addEventListener('research:source-loading',()=>{currentSource=null;updateReaderSave();});
function renderNotebook() {
    updateCount();updateComparison();el('notebookItems').replaceChildren();
    if(!notebook.length){const empty=document.createElement('div');empty.className='notebook-empty';empty.innerHTML='<span aria-hidden="true">✳</span><h3>Uma descoberta merece ficar.</h3><p>Abra uma fonte e escolha “Guardar fonte”, ou salve uma cena do cinema. Depois, escreva aqui o que você percebeu.</p>';el('notebookItems').append(empty);return;}
    notebook.forEach((item,index)=>{
        const card=document.createElement('article');card.className='notebook-card';
        const meta=document.createElement('p');meta.className='eyebrow';meta.textContent=`FONTE ${String(index+1).padStart(2,'0')} · ${item.ano||'ANO NÃO INFORMADO'}`;
        const title=document.createElement('h3');title.textContent=item.jornal||`Acervo ${item.bib}`;
        const quote=document.createElement('blockquote');quote.textContent=item.quote||'Abra a fonte para consultar o texto completo.';
        const label=document.createElement('label');label.textContent='O que você descobriu?';label.htmlFor=`note-${index}`;
        const notes=document.createElement('textarea');notes.id=label.htmlFor;notes.rows=3;notes.maxLength=10000;notes.placeholder='Uma hipótese, uma ligação para conferir, uma pergunta…';notes.value=item.note;
        notes.addEventListener('input',()=>{item.note=notes.value;const saved=persist();text('notebookStatus',saved?'Notas salvas neste navegador.':'Notas na memória. Exporte uma cópia para guardá-las.');});
        const actions=document.createElement('div');actions.className='notebook-card-actions';actions.append(button('Ler a fonte ↗',()=>readSource(item)),button('Retirar do caderno',()=>{
            comparison.delete(item.key);notebook=notebook.filter(i=>i.key!==item.key);persist();renderNotebook();updateReaderSave();
            text('notebookStatus','Fonte retirada.');const undo=button('Desfazer',()=>{if(!notebook.some(i=>i.key===item.key))notebook.splice(Math.min(index,notebook.length),0,item);persist();renderNotebook();updateReaderSave();text('notebookStatus','Fonte restaurada.');});el('notebookStatus').append(undo);
        },'quiet'));
        const compareLabel=document.createElement('label');compareLabel.className='compare-pick';const check=document.createElement('input');check.type='checkbox';check.checked=comparison.has(item.key);check.addEventListener('change',()=>{if(check.checked&&comparison.size>=2){check.checked=false;text('notebookStatus','Escolha apenas duas fontes para colocar lado a lado.');return;}if(check.checked)comparison.add(item.key);else comparison.delete(item.key);updateComparison();});compareLabel.append(check,document.createTextNode('Selecionar para confronto'));
        card.append(meta,title,quote,label,notes,actions,compareLabel);el('notebookItems').append(card);
    });
}
el('openNotebook').addEventListener('click',()=>{renderNotebook();el('notebookDialog').showModal();});
el('closeNotebook').addEventListener('click',()=>el('notebookDialog').close());
el('exportNotebook').addEventListener('click',()=>{
    const blob=new Blob([notebookMarkdown(notebook,location.origin)],{type:'text/markdown;charset=utf-8'});
    const url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download='caderno-hemeroteca.md';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);toast('Caderno exportado com trechos, notas e links das fontes.');
});
el('filmNotebook').addEventListener('click',()=>openFilm(notebook,'Meu caderno de investigação'));
updateCount();updateReaderSave();

async function loadPage(item) {
    if(pageCache.has(item.key))return pageCache.get(item.key);
    const response=await fetch(`/api/page/${encodeURIComponent(item.bib)}/${encodeURIComponent(item.pagina)}`,{signal:AbortSignal.timeout(15000)});
    if(!response.ok)throw new Error('Fonte indisponível');
    const data=await response.json();if(data.error)throw new Error(data.error);
    pageCache.set(item.key,data);if(pageCache.size>60)pageCache.delete(pageCache.keys().next().value);return data;
}
function pauseFilm() {
    film.playing=false;clearTimeout(film.timer);cancelAnimationFrame(film.frame);
    el('filmStage').classList.remove('playing');text('filmPlay','▶ Reproduzir');el('filmPlay').setAttribute('aria-label','Reproduzir sequência');el('filmProgress').style.width='0%';
}
function startClock() {
    clearTimeout(film.timer);cancelAnimationFrame(film.frame);if(!film.playing||film.loading)return;
    const duration=Number(el('filmSpeed').value);film.started=performance.now();
    const tick=()=>{if(!film.playing)return;el('filmProgress').style.width=`${Math.min(100,(performance.now()-film.started)/duration*100)}%`;film.frame=requestAnimationFrame(tick);};
    film.frame=requestAnimationFrame(tick);
    film.timer=setTimeout(()=>{if(film.index+1<film.items.length)showScene(film.index+1);else{pauseFilm();text('filmStatus','Fim desta sequência. Volte a uma cena, leia a fonte ou comece outra investigação.');}},duration);
}
function playFilm() {
    if(!film.items.length)return;
    film.playing=true;text('filmPlay','Ⅱ Pausar');el('filmPlay').setAttribute('aria-label','Pausar sequência');el('filmStage').classList.add('playing');startClock();
}
function buildFilmStrip() {
    el('filmStrip').replaceChildren();film.items.forEach((item,index)=>{
        const step=button(`${item.ano.match(/\d{4}/)?.[0]||'s/d'} · ${String(index+1).padStart(2,'0')}`,()=>{pauseFilm();showScene(index);},'film-frame');
        step.setAttribute('aria-label',`Cena ${index+1}, ${item.ano||'sem ano'}, página ${item.pagina}`);el('filmStrip').append(step);
    });
}
async function showScene(index) {
    const item=film.items[index];if(!item)return;
    text('filmSave',notebook.some(saved=>saved.key===item.key)?'✓ Fonte no caderno':'＋ Guardar no caderno');
    const generation=++film.generation;film.index=index;film.loading=true;clearTimeout(film.timer);cancelAnimationFrame(film.frame);el('filmProgress').style.width='0%';
    el('filmPrevious').disabled=index===0;el('filmNext').disabled=index===film.items.length-1;
    text('filmStatus','Carregando a fonte desta cena…');text('filmIndex',`${String(index+1).padStart(2,'0')} / ${String(film.items.length).padStart(2,'0')}`);
    text('filmYear',item.ano.match(/\d{4}/)?.[0]||'s/d');text('filmSceneTitle',item.jornal||'Uma ocorrência no jornal');text('filmQuote',item.quote.slice(0,600));text('filmMeta',`Acervo ${item.bib} · página ${item.pagina}`);text('filmKind','TRECHO DA TRANSCRIÇÃO');
    for(const [i,step] of [...el('filmStrip').children].entries())step.setAttribute('aria-current',i===index?'step':'false');
    const frame=el('filmStrip').children[index];
    if(frame)el('filmStrip').scrollTo({left:frame.offsetLeft-(el('filmStrip').clientWidth-frame.offsetWidth)/2,behavior:reducedMotion.matches?'instant':'smooth'});
    el('filmVisual').replaceChildren();const paper=document.createElement('div');paper.className='transcript-sheet';
    const caption=document.createElement('span');caption.textContent='TRANSCRIÇÃO LOCAL · NÃO É FAC-SÍMILE';
    const heading=document.createElement('h4');heading.textContent=item.jornal||'Do arquivo';
    const body=document.createElement('p');body.textContent=item.quote||'Abrindo transcrição…';paper.append(caption,heading,body);el('filmVisual').append(paper);
    try {
        const data=await loadPage(item);if(generation!==film.generation||!el('filmDialog').open)return;
        item.jornal=data.jornal||item.jornal;
        item.quote=item.quote||String(data.ocr_text||'').slice(0,1000);
        text('filmYear',item.ano.match(/\d{4}/)?.[0]||'s/d');text('filmSceneTitle',item.jornal||'Uma ocorrência no jornal');text('filmQuote',item.quote.slice(0,600));heading.textContent=item.jornal||'Do arquivo';body.textContent=String(data.ocr_text||item.quote||'Sem transcrição disponível.').slice(0,6500);
        if(typeof data.image_url==='string'&&data.image_url.startsWith('/images/')) {
            const img=new Image();img.alt=`Fac-símile da página ${item.pagina}`;img.className='film-facsimile';img.src=data.image_url;
            img.onload=()=>{if(generation!==film.generation)return;el('filmVisual').replaceChildren(img);text('filmKind','IMAGEM ORIGINAL');};
            img.onerror=()=>{if(generation===film.generation)text('filmStatus','Imagem indisponível. Exibindo a transcrição local.');};
        }
        text('filmStatus','');
    }catch(error){if(generation===film.generation)text('filmStatus','Não foi possível abrir a página. O trecho salvo permanece disponível.');}
    finally {if(generation===film.generation){film.loading=false;startClock();}}
}
function openFilm(items,title,highlight='') {
    const ordered=chronologicalDocuments(items);if(!ordered.length){toast('Abra uma pesquisa para reunir documentos primeiro.');return;}
    pauseFilm();film.items=ordered;film.index=0;film.highlight=highlight;text('filmTitle',title);buildFilmStrip();
    if(!el('filmDialog').open)el('filmDialog').showModal();
    showScene(0);
    // A viagem começa parada: a pessoa escolhe quando o tempo corre.
}
el('filmResults').addEventListener('click',()=>openFilm(sources,sourceTitle,sourceQuery));
el('closeFilm').addEventListener('click',()=>el('filmDialog').close());
el('filmDialog').addEventListener('close',()=>{pauseFilm();film.generation++;});
el('filmPlay').addEventListener('click',()=>film.playing?pauseFilm():playFilm());
el('filmPrevious').addEventListener('click',()=>{pauseFilm();showScene(film.index-1);});
el('filmNext').addEventListener('click',()=>{pauseFilm();showScene(film.index+1);});
el('filmSpeed').addEventListener('change',()=>{if(film.playing)startClock();});
el('filmRead').addEventListener('click',()=>readSource(film.items[film.index]));
el('filmSave').addEventListener('click',()=>saveSource(film.items[film.index]));
el('filmDialog').addEventListener('keydown',event=>{
    if(event.target.matches('select,input,textarea,button'))return;
    if(event.key==='ArrowRight'){event.preventDefault();pauseFilm();showScene(film.index+1);}
    if(event.key==='ArrowLeft'){event.preventDefault();pauseFilm();showScene(film.index-1);}
    if(event.key===' '){event.preventDefault();film.playing?pauseFilm():playFilm();}
});
document.addEventListener('visibilitychange',()=>{if(document.hidden)pauseFilm();});
el('heroSource').addEventListener('click',()=>readSource({bib:'029033_01',pagina:'029033_01_00001'}));
el('startJourney').addEventListener('click',async()=>{
    const trigger=el('startJourney'),original=trigger.innerHTML;trigger.disabled=true;trigger.textContent='Reunindo documentos…';
    try {
        const response=await fetch('/api/buscar?'+new URLSearchParams({q:'Botelho',n:'30',modo:'literal',score_min:'0'}),{signal:AbortSignal.timeout(30000)});
        if(!response.ok)throw new Error('A pesquisa está indisponível.');const data=await response.json();if(data.error)throw new Error(data.error);
        openFilm(data.resultados||[],'Botelho · um nome pelas páginas','Botelho');
    }catch {toast('Não foi possível carregar a viagem. Tente uma busca no acervo.');}finally{trigger.disabled=false;trigger.innerHTML=original;}
});
document.querySelectorAll('[data-trail]').forEach(trigger=>trigger.addEventListener('click',()=>{
    window.dispatchEvent(new CustomEvent('research:search',{detail:{query:trigger.dataset.trail,mode:'text'}}));el('research').scrollIntoView({behavior:reducedMotion.matches?'instant':'smooth'});
}));

const saveSelection=button('Guardar trecho selecionado',()=>{
    const selection=window.getSelection();
    if(!currentSource||!selection||!selection.rangeCount||!el('sourceText').contains(selection.anchorNode)||!el('sourceText').contains(selection.focusNode)||!selection.toString().trim()){
        text('sourceStatus','Selecione um trecho da transcrição para guardar.');return;
    }
    const quote=selection.toString().trim().slice(0,1800);
    const existing=notebook.find(item=>item.key===currentSource.key);
    if(existing){existing.quote=quote;const saved=persist();text('sourceStatus',saved?'Trecho desta fonte atualizado no caderno. Suas notas foram preservadas.':'Trecho atualizado na memória. Exporte o caderno para guardar.');}
    else {saveSource({...currentSource,quote});text('sourceStatus','Trecho selecionado guardado no caderno.');}
});
saveSelection.className='quiet';el('sourceDialog').querySelector('.text-find').append(saveSelection);
el('compareSources').addEventListener('click',async()=>{
    const selected=notebook.filter(item=>comparison.has(item.key));if(selected.length!==2)return;
    const generation=++comparisonGeneration;el('compareColumns').replaceChildren();el('compareDialog').showModal();
    const panels=selected.map(item=>{
        const panel=document.createElement('section');panel.className='compare-column';
        const meta=document.createElement('span');meta.className='eyebrow';meta.textContent=`${item.ano||'ANO NÃO INFORMADO'} · PÁGINA ${item.pagina}`;
        const title=document.createElement('h3');title.textContent=item.jornal||`Acervo ${item.bib}`;
        const quote=document.createElement('blockquote');quote.textContent=item.quote||'Sem trecho salvo.';
        const source=button('Abrir leitor da fonte ↗',()=>readSource(item));
        const label=document.createElement('p');label.className='small';label.textContent='TRANSCRIÇÃO COMPLETA · CARREGANDO';
        const full=document.createElement('pre');full.className='compare-text';
        panel.append(meta,title,quote,source,label,full);el('compareColumns').append(panel);return {item,full,label};
    });
    await Promise.all(panels.map(async({item,full,label})=>{
        try {const data=await loadPage(item);if(generation!==comparisonGeneration)return;full.textContent=data.ocr_text||'Sem transcrição local disponível.';label.textContent='TRANSCRIÇÃO COMPLETA · CONFERIR NO ORIGINAL';}
        catch {if(generation===comparisonGeneration)label.textContent='PÁGINA INDISPONÍVEL · TRECHO SALVO EXIBIDO ACIMA';}
    }));
});
el('closeCompare').addEventListener('click',()=>el('compareDialog').close());
el('compareDialog').addEventListener('close',()=>comparisonGeneration++);

export function getNotebook(){return notebook.map(item=>({...item}));}
export {saveSource};
