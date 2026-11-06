import {chronologicalDocuments} from './experience-model.mjs';
export function setupAtlas({$,api,status,failure,sourceCard,readJSON,writeJSON}){
    let pins=readJSON('hemeroteca.atlas.v1',[]);if(!Array.isArray(pins))pins=[];
    pins=pins.filter(p=>p&&Number.isFinite(p.lat)&&Number.isFinite(p.lon)&&Array.isArray(p.sources)).slice(0,20);
    let sources=[],query='',years=[],timer,marking=false,center={lat:-8.062778,lon:-34.888889},zoom=15,showUnknown=false;
    const map=$('atlasMap');
    const project=(lat,lon,z)=>{const n=256*2**z,s=Math.sin(lat*Math.PI/180);return {x:(lon+180)/360*n,y:(.5-Math.log((1+s)/(1-s))/(4*Math.PI))*n};};
    const unproject=(x,y,z)=>{const n=256*2**z;return {lon:x/n*360-180,lat:Math.atan(Math.sinh(Math.PI*(1-2*y/n)))*180/Math.PI};};
    const year=source=>Number(String(source.ano).match(/\b(1\d{3}|20\d{2})\b/)?.[1])||null;
    const btn=(label,fn)=>{const b=document.createElement('button');b.type='button';b.textContent=label;b.addEventListener('click',fn);return b;};
    function stop(){clearInterval(timer);timer=null;$('atlasPlay').textContent='▶ Percorrer anos';}
    function selectedYear(){return years.length?Number($('atlasYear').value):null;}
    function renderSources(){
        const current=selectedYear();$('atlasYearLabel').textContent=showUnknown?'Ano não informado':current||'Sem ano conhecido';
        $('atlasSources').replaceChildren();const filtered=sources.filter(s=>showUnknown?!year(s):year(s)===current);
        const info=document.createElement('p');info.className='small';info.textContent=`${filtered.length} de ${sources.length} páginas da amostra · ${sources.filter(s=>!year(s)).length} sem ano. Não é um levantamento completo.`;$('atlasSources').append(info);
        filtered.forEach(s=>$('atlasSources').append(sourceCard(s)));if(!filtered.length){const empty=document.createElement('p');empty.className='lab-empty';empty.textContent='Nenhuma ocorrência nesta parte da amostra.';$('atlasSources').append(empty);}renderMap();
    }
    function setSources(items,name){sources=chronologicalDocuments(items);query=name;showUnknown=false;years=[...new Set(sources.map(year).filter(Boolean))].sort((a,b)=>a-b);
        const slider=$('atlasYear');slider.disabled=!years.length;slider.min=years[0]||1825;slider.max=years.at(-1)||1899;slider.value=years[0]||1825;$('atlasPlay').disabled=years.length<2;$('atlasPinMode').disabled=!sources.length;renderSources();
    }
    function renderPins(){const host=$('atlasPins');host.replaceChildren();pins.forEach(pin=>{const row=document.createElement('div');row.className='atlas-place';row.append(btn(`${pin.query} · posição aproximada`,()=>{stop();center={lat:pin.lat,lon:pin.lon};$('atlasQuery').value=pin.query;setSources(pin.sources,pin.query);}));row.append(btn('Retirar',()=>{pins=pins.filter(p=>p!==pin);writeJSON('hemeroteca.atlas.v1',pins);renderPins();renderMap();}));host.append(row);});}
    function renderMap(){
        if($('lab-atlas').hidden||!$('labDialog').open)return;
        map.replaceChildren();const w=map.clientWidth,h=map.clientHeight,c=project(center.lat,center.lon,zoom),origin={x:c.x-w/2,y:c.y-h/2};
        for(let x=Math.floor(origin.x/256);x<=Math.floor((origin.x+w)/256);x++)for(let y=Math.floor(origin.y/256);y<=Math.floor((origin.y+h)/256);y++){
            const image=new Image();image.className='map-tile';image.alt='';image.style.left=`${x*256-origin.x}px`;image.style.top=`${y*256-origin.y}px`;image.src=`https://tile.openstreetmap.org/${zoom}/${x}/${y}.png`;image.referrerPolicy='strict-origin-when-cross-origin';map.append(image);
        }
        const controls=document.createElement('div');controls.className='map-controls';
        const add=(label,title,fn)=>{const b=btn(label,fn);b.setAttribute('aria-label',title);controls.append(b);};
        add('+','Aproximar mapa',()=>{zoom=Math.min(17,zoom+1);renderMap();});add('−','Afastar mapa',()=>{zoom=Math.max(13,zoom-1);renderMap();});
        for(const [symbol,title,dx,dy]of [['←','Mover mapa para oeste',-1,0],['→','Mover mapa para leste',1,0],['↑','Mover mapa para norte',0,-1],['↓','Mover mapa para sul',0,1]])add(symbol,title,()=>{center=unproject(c.x+dx*w/3,c.y+dy*h/3,zoom);renderMap();});
        map.append(controls);
        pins.forEach(pin=>{const count=pin.sources.filter(s=>showUnknown?!year(s):year(s)===selectedYear()).length;if(selectedYear()&&!count&&!showUnknown)return;
            const p=project(pin.lat,pin.lon,zoom);const marker=btn(`${pin.query} · ${count}`,()=>{stop();$('atlasQuery').value=pin.query;setSources(pin.sources,pin.query);});marker.className='map-pin';marker.style.left=`${p.x-origin.x}px`;marker.style.top=`${p.y-origin.y}px`;marker.title='Localização aproximada anotada pelo pesquisador';map.append(marker);
        });
    }
    map.addEventListener('click',event=>{if(!marking||event.target.closest('button'))return;const rect=map.getBoundingClientRect(),c=project(center.lat,center.lon,zoom),point=unproject(c.x-map.clientWidth/2+event.clientX-rect.left,c.y-map.clientHeight/2+event.clientY-rect.top,zoom);$('pinLat').value=point.lat.toFixed(6);$('pinLon').value=point.lon.toFixed(6);marking=false;map.classList.remove('pinning');status('Posição preenchida. Confira as coordenadas e escolha “Guardar posição aproximada”.');});
    $('atlasPinMode').addEventListener('click',()=>{marking=!marking;map.classList.toggle('pinning',marking);status(marking?'Clique no mapa para indicar uma posição moderna aproximada, ou use os campos de coordenadas.':'Marcação cancelada.');});
    $('atlasPinForm').addEventListener('submit',event=>{event.preventDefault();if(!sources.length){status('Pesquise um lugar e reúna fontes antes de marcar.');return;}const lat=Number($('pinLat').value),lon=Number($('pinLon').value);if(!Number.isFinite(lat)||!Number.isFinite(lon))return;
        const existing=pins.find(p=>p.query===query);const pin={query,lat,lon,sources};if(existing)Object.assign(existing,pin);else if(pins.length<20)pins.push(pin);else{status('Limite de 20 lugares. Exporte seu atlas antes de retirar um ponto.');return;}writeJSON('hemeroteca.atlas.v1',pins);renderPins();renderMap();status('Posição aproximada anotada. A correspondência histórica continua a conferir.');
    });
    $('atlasForm').addEventListener('submit',async event=>{event.preventDefault();stop();status('Buscando o lugar nas transcrições…');try{const data=await api('atlas','/api/buscar?'+new URLSearchParams({q:$('atlasQuery').value,n:'50',modo:'literal',score_min:'0'}));setSources(data.resultados,$('atlasQuery').value);status('Use o ano para percorrer a amostra e abra cada fonte para conferir o contexto.');}catch(error){failure(error);}});
    $('atlasYear').addEventListener('input',()=>{stop();showUnknown=false;renderSources();});
    $('atlasPlay').addEventListener('click',()=>{if(timer){stop();return;}showUnknown=false;$('atlasPlay').textContent='Ⅱ Pausar anos';timer=setInterval(()=>{const index=years.findIndex(y=>y>selectedYear());if(index<0){stop();return;}$('atlasYear').value=years[index];renderSources();},1800);});
    const unknown=btn('Ver fontes sem ano',()=>{stop();showUnknown=true;renderSources();});unknown.className='secondary';$('atlasPinMode').parentElement.append(unknown);
    const exportButton=btn('Exportar atlas (.geojson) ↓',()=>{const geo={type:'FeatureCollection',features:pins.map(pin=>({type:'Feature',geometry:{type:'Point',coordinates:[pin.lon,pin.lat]},properties:{name:pin.query,precision:'user_annotated_approximate_modern_location',sources:pin.sources}}))};const url=URL.createObjectURL(new Blob([JSON.stringify(geo,null,2)],{type:'application/geo+json'})),a=document.createElement('a');a.href=url;a.download='atlas-hemeroteca.geojson';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});exportButton.className='secondary';$('atlasPins').after(exportButton);
    new ResizeObserver(()=>{if(!$('lab-atlas').hidden&&$('labDialog').open)renderMap();}).observe(map);
    return {stop,open(){renderMap();renderPins();if(!sources.length)$('atlasSources').textContent='Pesquise um lugar para ver suas fontes ao longo dos anos.';}};
}
