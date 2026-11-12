'use strict';
const $ = id => document.getElementById(id);
const state = {focus:0, query:'', requests:new Map(), graph:null, sourceText:'', match:0};
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function status(id, message='', error=false){$(id).textContent=message;$(id).classList.toggle('error',error);}
async function request(channel, path){
    state.requests.get(channel)?.abort();
    const controller=new AbortController();state.requests.set(channel,controller);
    const timer=setTimeout(()=>controller.abort('timeout'),30000);
    try{
        const response=await fetch(path,{signal:controller.signal});
        if(!response.ok)throw new Error('Não foi possível carregar os dados ('+response.status+').');
        const data=await response.json();
        if(controller.signal.aborted)throw new DOMException('Substituída','AbortError');
        if(data.error)throw new Error(data.error);
        return data;
    }catch(error){
        if(controller.signal.reason==='timeout')throw new Error('A consulta demorou mais que o esperado. Tente novamente.');
        throw error;
    }finally{clearTimeout(timer);}
}
function failure(id,error){if(error.name!=='AbortError')status(id,error.message || 'Falha de conexão. Tente novamente.',true);}
function chosenMode(){return document.querySelector('input[name=mode]:checked').value;}
function setMode(){ $('yearFields').hidden=chosenMode()!=='text';$('searchHint').textContent=chosenMode()==='text'?'Busca por todas as palavras, sem distinguir acentos. O período exclui páginas sem ano conhecido.':'Preservamos os nomes encontrados nas fontes.'; }
document.querySelectorAll('input[name=mode]').forEach(el=>el.addEventListener('change',setMode));
$('searchForm').addEventListener('submit',async event=>{
    event.preventDefault();const q=$('query').value.trim();if(!q)return;
    const mode=chosenMode(),from=$('yearFrom').value,to=$('yearTo').value;
    if(mode==='text'&&from&&to&&Number(from)>Number(to)){status('searchStatus','O ano inicial deve ser anterior ao ano final.',true);return;}
    state.query=q;$('resultsHeading').textContent='Resultados';status('searchStatus','Buscando no acervo…');$('results').replaceChildren();
    const params=new URLSearchParams({q,n:'15'});
    if(mode==='text'){params.set('modo','literal');params.set('score_min','0');if(from)params.set('year_from',from);if(to)params.set('year_to',to);}
    try{
        const data=await request('search',(mode==='people'?'/api/entity/search?':'/api/buscar?')+params);
        window.dispatchEvent(new CustomEvent('research:results',{detail:{items:data.resultados||[],query:q,mode}}));
        status('searchStatus',data.total ? `${data.total} resultados exibidos`:'Nenhum resultado. Tente um sobrenome ou uma grafia diferente.');
        for(const item of data.resultados||[]){
            const button=document.createElement('button');button.className='result';
            if(mode==='people'){
                const types={person:'Pessoa',place:'Lugar',institution:'Instituição'};
                button.innerHTML=`<strong>${esc(item.canonical_name)}</strong><span>${esc(types[item.type]||'Entidade')} · ${esc(item.mentions ?? item.mention_count ?? '—')} menções</span>`;
                button.addEventListener('click',()=>{document.querySelectorAll('.result').forEach(e=>e.classList.remove('selected'));button.classList.add('selected');selectEntity(item.id);});
            }else{
                const m=item.metadata||{};
                button.innerHTML=`<strong>${esc(item.jornal||m.jornal||'Página do jornal')}</strong><span>${esc(item.ano||m.ano||'Ano não informado')} · pág. ${esc(item.pagina||m.pagina||'?')}</span><p class="snippet">${esc(item.evidencia||item.texto?.slice(0,230)||'Abrir transcrição')}</p>`;
                button.addEventListener('click',()=>openPage(item.bib||m.bib,item.pagina||m.pagina));
            }
            $('results').append(button);
        }
    }catch(error){failure('searchStatus',error);}
});
document.querySelectorAll('[data-query]').forEach(button=>button.addEventListener('click',()=>{$('query').value=button.dataset.query;$('searchForm').requestSubmit();}));
function sourceCard(item){
    const card=document.createElement('article');card.className='evidence';
    const text=item.quote||item.snippet||item.source_snippet||item.text||'';
    const known=(value,fallback)=>value&&value!=='?'?value:fallback;
    card.innerHTML=`<span class="small">${esc(known(item.jornal,'Jornal não identificado'))} · ${esc(known(item.ano,'Data não informada'))}</span><blockquote>${esc(text||'O trecho não está disponível neste registro.')}</blockquote>`;
    if(item.bib&&item.pagina){const b=document.createElement('button');b.textContent='Ler a página original →';b.addEventListener('click',()=>openPage(item.bib,item.pagina));card.append(b);}
    return card;
}
async function selectEntity(id){
    state.focus=Number(id);status('detailStatus','Carregando ocorrências…');$('detailTitle').textContent='Ficha da entidade';$('detail').replaceChildren();
    loadGraph();
    try{
        const data=await request('detail','/api/entity/'+id);
        status('detailStatus');$('detailTitle').textContent=data.canonical_name;
        window.dispatchEvent(new CustomEvent('research:entity',{detail:data}));
        if(state.focus===Number(id))$('graphTitle').textContent='Em torno de '+data.canonical_name;
        const intro=document.createElement('div');intro.innerHTML=`<span class="badge">Identidade a conferir nas fontes</span><p class="small">Ocorrências documentais deste registro. Não implica que todos os homônimos sejam a mesma pessoa.</p>`;
        const timeline=document.createElement('div');timeline.className='timeline';
        for(const item of data.summary?.timeline||[]){const chip=document.createElement('span');chip.textContent=`${item.year} · ${item.mentions}`;timeline.append(chip);}
        $('detail').append(intro,timeline);
        const mentions=data.mentions||[];
        if(!mentions.length)$('detail').insertAdjacentHTML('beforeend','<p class="muted">Sem ocorrências disponíveis.</p>');
        for(const item of mentions.slice(0,12))$('detail').append(sourceCard(item));
        if(mentions.length>12){const p=document.createElement('p');p.className='small';p.textContent=`Exibindo 12 de ${mentions.length} ocorrências retornadas.`;$('detail').append(p);}
    }catch(error){failure('detailStatus',error);}
}
async function loadGraph(){
    const layers=[...document.querySelectorAll('input[name=layer]:checked')].map(e=>e.value);
    if(!layers.length){state.requests.get('graph')?.abort();renderGraph({nodes:[],edges:[]});status('graphStatus','Selecione uma camada para explorar.');return;}
    status('graphStatus','Carregando relações…');
    try{
        const data=await request('graph','/api/discovery/graph?'+new URLSearchParams({layers:layers.join(','),limit:'18',focus:String(state.focus),depth:'1'}));
        renderGraph(data);status('graphStatus',data.edges.length?'':'Sem relações nessas camadas. Experimente outra camada ou entidade.');
    }catch(error){failure('graphStatus',error);}
}
const ns='http://www.w3.org/2000/svg';
function svg(tag,attrs){const e=document.createElementNS(ns,tag);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);return e;}
function renderGraph(data){
    state.graph=data;const host=$('network');host.replaceChildren();$('edgeList').replaceChildren();
    const nodes=data.nodes||[],edges=data.edges||[];
    const positions=new Map();const central=nodes.find(n=>n.central);
    const others=nodes.filter(n=>n!==central);
    if(central)positions.set(central.id,{x:325,y:210});
    others.forEach((n,i)=>{const a=2*Math.PI*i/Math.max(others.length,1)-Math.PI/2;positions.set(n.id,{x:325+245*Math.cos(a),y:215+165*Math.sin(a)});});
    const labels=new Map(nodes.map(n=>[n.id,n.label]));
    for(const edge of edges){
        const a=positions.get(edge.source),b=positions.get(edge.target);if(!a||!b)continue;
        const line=svg('line',{x1:a.x,y1:a.y,x2:b.x,y2:b.y,class:'graph-line'+(edge.layer==='co_mention'?' co-line':''),tabindex:'0',role:'button','aria-label':`${labels.get(edge.source)}: ${edge.label}, ${labels.get(edge.target)}. Ver evidência`});
        line.addEventListener('click',()=>selectEdge(edge,labels));line.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();selectEdge(edge,labels);}});host.append(line);
        const button=document.createElement('button');button.className='edge';
        button.innerHTML=`<strong>${esc(labels.get(edge.source))} <span aria-hidden="true">→</span> ${esc(labels.get(edge.target))}</strong><small>${esc(edge.label||edge.predicate)} · ${edge.layer==='co_mention'?'Mesma página; vínculo não comprovado':'Ver relação extraída e fonte'}</small>`;
        button.addEventListener('click',()=>selectEdge(edge,labels));$('edgeList').append(button);
    }
    for(const node of nodes){
        const p=positions.get(node.id);const group=svg('g',{transform:`translate(${p.x},${p.y})`,class:'graph-node'});
        if(node.entity_id){group.setAttribute('tabindex','0');group.setAttribute('role','button');group.setAttribute('aria-label','Explorar '+node.label);group.addEventListener('click',()=>selectEntity(node.entity_id));group.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();selectEntity(node.entity_id);}});}
        group.append(svg('circle',{r:node.central?'18':'10',fill:node.entity_id?'#456d59':'#b9ad93',stroke:'#fffefa','stroke-width':'3'}));
        const text=svg('text',{y:node.central?'35':'26','text-anchor':'middle'});text.textContent=node.label.length>23?node.label.slice(0,21)+'…':node.label;group.append(text);
        const title=svg('title',{});title.textContent=node.label;group.append(title);host.append(group);
    }
    $('graphCount').textContent=`${nodes.length} nós · ${edges.length} ligações nesta seleção`;$('edgeCount').textContent=`(${edges.length})`;
}
async function selectEdge(edge,labels){
    status('detailStatus','Carregando evidência…');$('detailTitle').textContent='Evidência da ligação';$('detail').replaceChildren();
    const intro=document.createElement('div');intro.innerHTML=`<span class="badge ${edge.layer==='co_mention'?'co-badge':''}">${edge.layer==='co_mention'?'Coocorrência · não comprova relação':'Relação extraída · verificar'}</span><h3>${esc(labels.get(edge.source))}</h3><p>${esc(edge.label)} → ${esc(labels.get(edge.target))}</p>`;$('detail').append(intro);
    if(edge.relation_ids?.length>1){
        const label=document.createElement('label');label.className='small';label.textContent='Registros desta ligação: ';
        const select=document.createElement('select');select.setAttribute('aria-label','Registro de extração para conferir');
        edge.relation_ids.forEach((id,index)=>{const option=document.createElement('option');option.value=id;option.textContent=`Registro ${index+1} de ${edge.relation_ids.length}`;option.selected=id===edge.relation_id;select.append(option);});
        select.addEventListener('change',()=>selectEdge({...edge,relation_id:Number(select.value)},labels));label.append(select);$('detail').append(label);
    }
    try{
        if(edge.relation_id){
            const data=await request('detail','/api/graph/relation-evidence?relation_id='+edge.relation_id);
            status('detailStatus');const p=document.createElement('p');p.className='small';
            const names={confirmed:'Confirmada no registro',approved:'Aprovada em revisão',rejected:'Rejeitada em revisão',hypothesis:'Hipótese extraída',probable:'Provável'};
            p.textContent='Situação: '+(names[data.effective_status]||data.effective_status||'Sem revisão informada');$('detail').append(p);
            if(!data.evidences.length)$('detail').insertAdjacentHTML('beforeend','<p class="muted">Esta relação não possui trecho de evidência disponível. Não a trate como confirmada.</p>');
            data.evidences.forEach(e=>$('detail').append(sourceCard(e)));
        }else{
            const a=state.graph.nodes.find(n=>n.id===edge.source),b=state.graph.nodes.find(n=>n.id===edge.target);
            if(!a?.entity_id||!b?.entity_id){state.requests.get('detail')?.abort();status('detailStatus','Sem referência documental disponível para esta ligação.');return;}
            const data=await request('detail',`/api/graph/edge-evidence?source=${a.entity_id}&target=${b.entity_id}`);
            status('detailStatus','Os nomes aparecem na mesma página; isso não demonstra parentesco ou contato.');
            if(!data.shared_pages?.length)$('detail').insertAdjacentHTML('beforeend','<p class="muted">Nenhuma página compartilhada disponível.</p>');
            for(const item of data.shared_pages||[]){$('detail').append(sourceCard(item));}
        }
    }catch(error){failure('detailStatus',error);}
}
document.querySelectorAll('input[name=layer]').forEach(e=>e.addEventListener('change',loadGraph));
$('resetGraph').addEventListener('click',()=>{state.focus=0;$('graphTitle').textContent='Relações no acervo';loadGraph();});
async function openPage(bib,page,highlight=state.query,quote=''){
    if(!bib||!page)return;
    window.dispatchEvent(new CustomEvent('research:source-loading'));
    const dialog=$('sourceDialog');if(!dialog.open)dialog.showModal();
    $('sourceTitle').textContent='Página do jornal';$('sourceImage').replaceChildren();$('sourceText').textContent='';$('sourceMeta').replaceChildren();$('imageZoom').value='100';state.sourceText='';$('sourceFind').value='';$('matchCount').textContent='';status('sourceStatus','Abrindo a fonte local…');
    try{
        const data=await request('source',`/api/page/${encodeURIComponent(bib)}/${encodeURIComponent(page)}`);
        status('sourceStatus');$('sourceTitle').textContent=data.jornal||'Página do jornal';
        const meta=document.createElement('span');meta.textContent=`Ano: ${data.ano||'?'} · Edição: ${data.edicao||'?'} · Página: ${page}`;$('sourceMeta').append(meta);
        const url=data.docreader_url;
        if(url){const parsed=new URL(url,location.origin);if(parsed.protocol==='https:'&&parsed.hostname.endsWith('.bn.gov.br')){const a=document.createElement('a');a.href=parsed.href;a.target='_blank';a.rel='noopener noreferrer';a.textContent='Consultar na Biblioteca Nacional ↗';$('sourceMeta').append(a);}}
        if(data.image_url&&data.image_url.startsWith('/images/')){const img=document.createElement('img');img.src=data.image_url;img.alt=`Imagem original da página ${page}`;img.onerror=()=>{$('sourceImage').innerHTML='<p class="unavailable">A imagem local não pôde ser carregada.</p>';};$('sourceImage').append(img);}
        else $('sourceImage').innerHTML='<div class="unavailable"><h3>Imagem ainda não disponível</h3><p>A transcrição local pode ser consultada ao lado. O original depende da captura ou do acesso à BN.</p></div>';
        state.sourceText=data.ocr_text||'Não há transcrição local disponível para esta página.';
        const needle=highlight.toLocaleLowerCase('pt-BR');
        const hit=needle?state.sourceText.toLocaleLowerCase('pt-BR').indexOf(needle):-1;
        const start=Math.max(0,hit-120);
        window.dispatchEvent(new CustomEvent('research:source',{detail:{...data,bib,pagina:page,quote:quote||state.sourceText.slice(start,start+1000)}}));
        $('sourceFind').value=highlight;highlightSource();
    }catch(error){failure('sourceStatus',error);}
}
function highlightSource(){
    const text=state.sourceText,query=$('sourceFind').value.trim();
    $('sourceText').replaceChildren();state.match=-1;
    if(!query){$('sourceText').textContent=text;$('matchCount').textContent='';return;}
    const needle=query.toLocaleLowerCase('pt-BR'),lower=text.toLocaleLowerCase('pt-BR');
    let cursor=0,found=lower.indexOf(needle),count=0;
    while(found!==-1&&count<500){
        $('sourceText').append(document.createTextNode(text.slice(cursor,found)));
        const mark=document.createElement('mark');mark.textContent=text.slice(found,found+query.length);$('sourceText').append(mark);
        cursor=found+query.length;count++;found=lower.indexOf(needle,cursor);
    }
    $('sourceText').append(document.createTextNode(text.slice(cursor)));
    $('matchCount').textContent=count?`${count===500?'Até ':''}${count} ocorrências`:'Sem correspondência exata. Tente parte do nome.';
    if(count)nextMatch();
}
function nextMatch(){
    const marks=$('sourceText').querySelectorAll('mark');if(!marks.length)return;
    marks.forEach(m=>m.classList.remove('active-match'));state.match=(state.match+1)%marks.length;
    marks[state.match].classList.add('active-match');marks[state.match].scrollIntoView({block:'nearest'});
}
$('sourceFind').addEventListener('input',highlightSource);
$('nextMatch').addEventListener('click',nextMatch);
$('closeSource').addEventListener('click',()=>$('sourceDialog').close());
$('sourceDialog').addEventListener('close',()=>state.requests.get('source')?.abort());
$('imageZoom').addEventListener('change',()=>{const img=$('sourceImage').querySelector('img');if(img)img.style.width=$('imageZoom').value+'%';});
$('samplePage').addEventListener('click',()=>openPage('029033_01','029033_01_00001'));
loadGraph();

window.addEventListener('research:open-source',event=>openPage(event.detail.bib,event.detail.pagina,event.detail.highlight??state.query,event.detail.quote||''));
window.addEventListener('research:select-entity',event=>selectEntity(event.detail.id));
window.addEventListener('research:search',event=>{const mode=document.querySelector(`input[name=mode][value="${event.detail.mode==='text'?'text':'people'}"]`);mode.checked=true;setMode();$('query').value=event.detail.query;$('yearFrom').value='';$('yearTo').value='';$('searchForm').requestSubmit();});
