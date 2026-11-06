import {normalizeDocument} from './experience-model.mjs';
export function setupStudio({$,getNotebook,getSources,status,download}){
    let scenes=[],assets=new Map(),raf,playing=false,recording=false,recorder=null,stream=null,audioContext=null,audioSource=null,run=0,started=0,downloadURL='',cancelled=false;
    const canvas=$('studioCanvas'),ctx=canvas.getContext('2d');
    try{const saved=JSON.parse(localStorage.getItem('hemeroteca.studio.v1')||'null');if(saved&&Array.isArray(saved.scenes)){scenes=saved.scenes.filter(s=>s&&s.doc&&normalizeDocument(s.doc).key).slice(0,12).map(s=>({doc:normalizeDocument(s.doc),caption:String(s.caption||'').slice(0,1000),duration:Math.max(2,Math.min(30,Number(s.duration)||6))}));$('studioTitle').value=String(saved.title||'Uma investigação nos jornais').slice(0,100);}}catch{}
    function persist(){try{localStorage.setItem('hemeroteca.studio.v1',JSON.stringify({title:$('studioTitle').value,scenes}));}catch{write('O roteiro não pôde ser salvo neste navegador. Exporte uma cópia.');}}

    const write=(text)=>{$('studioStatus').textContent=text;};
    const button=(text,fn)=>{const b=document.createElement('button');b.type='button';b.textContent=text;b.addEventListener('click',fn);return b;};
    function lock(value){document.querySelectorAll('#lab-studio input,#lab-studio textarea,#lab-studio button:not(#studioStop)').forEach(e=>e.disabled=value);$('studioStop').disabled=!value;}
    function stop(){run++;playing=false;cancelAnimationFrame(raf);try{audioSource?.stop();}catch{}audioSource=null;
        if(recording&&recorder?.state==='recording'){cancelled=true;recorder.stop();write('Exportação cancelada. O roteiro foi preservado.');}
        else{stream?.getTracks().forEach(t=>t.stop());stream=null;audioContext?.close();audioContext=null;lock(false);}
    }
    const total=()=>scenes.reduce((sum,s)=>sum+s.duration,0);
    function wrap(text,x,y,width,lineHeight,maxLines){
        const words=String(text).replace(/\s+/g,' ').split(' ');let line='',count=0;
        for(let i=0;i<words.length;i++){const next=line?line+' '+words[i]:words[i];if(ctx.measureText(next).width>width&&line){ctx.fillText(line,x,y+count*lineHeight);count++;line=words[i];if(count>=maxLines-1){const rest=words.slice(i).join(' ');let last=rest;while(ctx.measureText(last+'…').width>width&&last.length)last=last.slice(0,-1);ctx.fillText(last+(last===rest?'':'…'),x,y+count*lineHeight);return;}}else line=next;}if(line)ctx.fillText(line,x,y+count*lineHeight);
    }
    function draw(index=0,fraction=0){
        const s=scenes[index];ctx.fillStyle='#19291f';ctx.fillRect(0,0,1280,720);
        ctx.fillStyle='#eca578';ctx.font='14px sans-serif';ctx.fillText('HEMEROTECA · DOCUMENTÁRIO DE INVESTIGAÇÃO',50,42);
        ctx.fillStyle='#e8ebdb';ctx.font='24px Georgia';wrap($('studioTitle').value,50,78,1180,28,1);
        if(!s){ctx.font='40px Georgia';ctx.fillText('Toda descoberta começa com uma fonte.',75,320);ctx.font='20px sans-serif';ctx.fillText('Adicione páginas do caderno ou da sua pesquisa.',75,365);return;}
        const asset=assets.get(s.doc.key),image=asset?.image;
        ctx.save();ctx.beginPath();ctx.rect(50,118,545,520);ctx.clip();
        if(image){const scale=545/image.width*(1+fraction*.05);ctx.drawImage(image,50-(image.width*scale-545)/2,118-fraction*30,image.width*scale,image.height*scale);}
        else {ctx.fillStyle='#e5dfca';ctx.fillRect(50,118,545,520);ctx.fillStyle='#404834';ctx.font='12px sans-serif';ctx.fillText('TRANSCRIÇÃO LOCAL · NÃO É FAC-SÍMILE',76,150);ctx.font='bold 29px Georgia';wrap(asset?.jornal||s.doc.jornal||'Do arquivo',76,198,490,33,2);ctx.font='20px Georgia';wrap(asset?.text||s.doc.quote||'Sem transcrição disponível.',76,290,480,31,11);}
        ctx.restore();
        ctx.fillStyle='#efa979';ctx.font='82px Georgia';ctx.fillText(s.doc.ano.match(/\d{4}/)?.[0]||'s/d',645,208);
        ctx.fillStyle='#b4c4a5';ctx.font='12px sans-serif';ctx.fillText(`CENA ${index+1} / ${scenes.length} · LEGENDA DO PESQUISADOR`,648,245);
        ctx.fillStyle='#ecedde';ctx.font='28px Georgia';wrap(s.caption,645,297,565,40,6);
        ctx.fillStyle='#bbc8ac';ctx.font='14px sans-serif';wrap(`Fonte: ${s.doc.jornal||s.doc.bib} · página ${s.doc.pagina}`,645,575,565,20,2);
        ctx.fillStyle='#91a081';ctx.font='12px sans-serif';ctx.fillText('Transcrições e hipóteses exigem conferência na fonte original.',50,675);
        ctx.fillStyle='#efa979';ctx.fillRect(50,699,1180*(index+fraction)/scenes.length,2);
    }
    function render(){
        $('studioScenes').replaceChildren();scenes.forEach((scene,index)=>{const row=document.createElement('article');row.className='studio-scene';const main=document.createElement('div'),meta=document.createElement('small');meta.textContent=`CENA ${index+1} · ${scene.doc.ano||'s/d'} · ${scene.doc.bib} · pág. ${scene.doc.pagina}`;
            const label=document.createElement('label');label.textContent='Legenda / roteiro revisável';const input=document.createElement('textarea');input.maxLength=1000;input.value=scene.caption;label.append(input);input.addEventListener('input',()=>{scene.caption=input.value;persist();draw(index);});main.append(meta,label);
            const controls=document.createElement('div'),durationLabel=document.createElement('label');durationLabel.textContent='Segundos';const duration=document.createElement('input');duration.type='number';duration.min=2;duration.max=30;duration.value=scene.duration;durationLabel.append(duration);duration.addEventListener('input',()=>{const value=Number(duration.value);if(Number.isFinite(value)&&value>=2&&value<=30){scene.duration=value;persist();write(`${scenes.length} ${scenes.length===1?"cena":"cenas"} · ${total()} segundos. Revise antes de exportar.`);}});duration.addEventListener('change',()=>{scene.duration=Math.max(2,Math.min(30,Number(duration.value)||6));duration.value=scene.duration;persist();write(`${scenes.length} ${scenes.length===1?"cena":"cenas"} · ${total()} segundos. Revise antes de exportar.`);});controls.append(durationLabel,button('↑',()=>{if(index){[scenes[index-1],scenes[index]]=[scene,scenes[index-1]];render();}}),button('↓',()=>{if(index<scenes.length-1){[scenes[index+1],scenes[index]]=[scene,scenes[index+1]];render();}}),button('Retirar',()=>{scenes.splice(index,1);render();}));controls.children[1].setAttribute('aria-label',`Mover cena ${index+1} para antes`);controls.children[2].setAttribute('aria-label',`Mover cena ${index+1} para depois`);row.append(main,controls);$('studioScenes').append(row);
        });draw();persist();write(`${scenes.length} ${scenes.length===1?"cena":"cenas"} · ${total()} segundos. Revise antes de exportar.`);
    }
    function add(items){stop();for(const item of items){if(scenes.length===12)break;const doc=normalizeDocument(item);if(!doc.key||scenes.some(s=>s.doc.key===doc.key))continue;scenes.push({doc,duration:6,caption:doc.note?`Hipótese: ${doc.note}`:(doc.quote.slice(0,330)||`Uma página de ${doc.jornal||'nosso acervo'}. Confira o documento original.`)});}render();if(!items.length)write('Guarde fontes no caderno ou faça uma pesquisa primeiro.');}
    async function loadAssets(){
        for(const scene of scenes){if(assets.has(scene.doc.key))continue;
            const response=await fetch(`/api/page/${encodeURIComponent(scene.doc.bib)}/${encodeURIComponent(scene.doc.pagina)}`,{signal:AbortSignal.timeout(15000)});if(!response.ok)throw new Error('Uma fonte não pôde ser carregada. Tente novamente antes de exportar.');const data=await response.json();if(data.error)throw new Error(data.error);
            const asset={text:data.ocr_text||scene.doc.quote,jornal:data.jornal,image:null};
            if(typeof data.image_url==='string'&&data.image_url.startsWith('/images/')){asset.image=await new Promise(resolve=>{const img=new Image();img.onload=()=>resolve(img);img.onerror=()=>resolve(null);img.src=data.image_url;});}assets.set(scene.doc.key,asset);
        }
    }
    async function start(exporting){
        if(!scenes.length){write('Adicione ao menos uma fonte ao roteiro.');return;}
        stop();const generation=++run;cancelled=false;lock(true);write('Preparando fontes e áudio…');
        try{await loadAssets();if(generation!==run)return;
            let audio=null;const file=$('studioAudio').files[0];if(file){if(file.size>30*1024*1024)throw new Error('Use um áudio de até 30 MB.');audioContext=new AudioContext();await audioContext.resume();audio=await audioContext.decodeAudioData(await file.arrayBuffer());}
            if(generation!==run)return;
            if(exporting){if(typeof MediaRecorder==='undefined'||!canvas.captureStream)throw new Error('Este navegador não exporta vídeo. Abra no Chrome ou exporte o roteiro.');
                const mime=['video/webm;codecs=vp9','video/webm;codecs=vp8','video/mp4','video/webm'].find(t=>MediaRecorder.isTypeSupported(t));if(!mime)throw new Error('Formato de vídeo indisponível neste navegador.');
                stream=canvas.captureStream(24);
                if(audio){const dest=audioContext.createMediaStreamDestination();audioSource=audioContext.createBufferSource();audioSource.buffer=audio;audioSource.connect(dest);dest.stream.getAudioTracks().forEach(t=>stream.addTrack(t));}
                const hasAudio=stream.getAudioTracks().length>0;const chunks=[];recorder=new MediaRecorder(stream,{mimeType:mime,videoBitsPerSecond:3500000});recorder.ondataavailable=e=>{if(e.data.size)chunks.push(e.data);};
                recorder.onstop=()=>{recording=false;stream?.getTracks().forEach(t=>t.stop());stream=null;try{audioSource?.stop();}catch{}audioContext?.close();audioContext=null;audioSource=null;lock(false);
                    if(!cancelled&&chunks.length){if(downloadURL)URL.revokeObjectURL(downloadURL);downloadURL=URL.createObjectURL(new Blob(chunks,{type:recorder.mimeType}));const link=$('studioDownload');link.href=downloadURL;link.download='documentario-hemeroteca.'+(recorder.mimeType.includes('mp4')?'mp4':'webm');link.hidden=false;link.textContent='Baixar vídeo pronto ↓';$('studioRendered').src=downloadURL;$('studioRendered').hidden=false;write('Vídeo pronto · '+(hasAudio?'com áudio.':'sem áudio.')+' O roteiro e as referências podem ser exportados à parte.');}}
                recorder.onerror=()=>{cancelled=true;stop();write('Não foi possível gravar. Tente exportar novamente ou use outro navegador.');};recording=true;recorder.start(300);$('studioDownload').hidden=true;$('studioRendered').hidden=true;
            }else if(audio){audioSource=audioContext.createBufferSource();audioSource.buffer=audio;audioSource.connect(audioContext.destination);}
            audioSource?.start();started=performance.now();playing=true;const duration=total();
            const tick=()=>{if(generation!==run)return;const elapsed=(performance.now()-started)/1000;let index=0,start=0;while(index<scenes.length-1&&elapsed>=start+scenes[index].duration){start+=scenes[index].duration;index++;}draw(index,Math.min(1,(elapsed-start)/scenes[index].duration));write(`${exporting?'Exportando':'Prévia'} · ${Math.min(duration,Math.floor(elapsed))} / ${duration} s`);
                if(elapsed>=duration){playing=false;try{audioSource?.stop();}catch{}if(recording)recorder.stop();else{lock(false);audioContext?.close();audioContext=null;write('Prévia concluída. Revise ou exporte o documentário.');}return;}raf=requestAnimationFrame(tick);};raf=requestAnimationFrame(tick);
        }catch(error){stop();write(error.message||'Não foi possível preparar o vídeo.');}
    }
    $('studioFromNotebook').addEventListener('click',()=>add(getNotebook()));$('studioFromResearch').addEventListener('click',()=>add(getSources()));$('studioCover').addEventListener('click',()=>add([{bib:'029033_01',pagina:'029033_01_00001',ano:'1825',jornal:'Diario de Pernambuco',quote:'Capa de 1825. Consulte a imagem original para conferir os textos.'}]));
    $('studioPreview').addEventListener('click',()=>start(false));$('studioExport').addEventListener('click',()=>start(true));$('studioStop').addEventListener('click',()=>{stop();write('Reprodução ou exportação interrompida. O roteiro foi preservado.');});$('studioTitle').addEventListener('input',()=>{persist();draw();});
    $('studioAudio').addEventListener('change',()=>{$('studioAudioInfo').textContent=$('studioAudio').files[0]?'Áudio anexado: '+$('studioAudio').files[0].name:'Sem áudio anexado.';});
    $('studioRendered').addEventListener('loadedmetadata',()=>{$('studioVideoInfo').textContent=`Vídeo gerado: ${$('studioRendered').videoWidth} × ${$('studioRendered').videoHeight} pixels.`;});
    $('studioScript').addEventListener('click',()=>download('roteiro-documentario.md',`# ${$('studioTitle').value}\n\nRoteiro do pesquisador, com hipóteses sujeitas à conferência.\n\n${scenes.map((s,i)=>`## Cena ${i+1} · ${s.duration} s\n\n${s.caption}\n\nFonte: ${s.doc.jornal||s.doc.bib} · ${s.doc.ano||'s/d'} · pág. ${s.doc.pagina}\n\n${location.origin}/page/${encodeURIComponent(s.doc.bib)}/${encodeURIComponent(s.doc.pagina)}`).join('\n\n')}`));
    document.addEventListener('visibilitychange',()=>{if(document.hidden&&(playing||recording)){stop();write('A aba foi ocultada. A exportação foi interrompida para evitar um vídeo incompleto.');}});
    return {stop,open(){if(!scenes.length&&getNotebook().length)add(getNotebook());else render();}};
}
