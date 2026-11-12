/* Scriptorium: painel de transcrição da Hemeroteca PE.
 * Aplicativo de uma página, sem dependências. O estado chega por SSE (/api/stream) a cada 2 s;
 * cada tela tem mount() (monta a estrutura uma vez) e update() (troca só as regiões com dados),
 * para que campos digitados e a rolagem não se percam a cada atualização. */
(() => {
'use strict';

// ---------------------------------------------------------------- utilidades
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const NF = new Intl.NumberFormat('pt-BR');
const n = (v, d = 0) => v == null || isNaN(v) ? '—' : new Intl.NumberFormat('pt-BR', {minimumFractionDigits: d, maximumFractionDigits: d}).format(v);
const int = v => v == null || isNaN(v) ? '—' : NF.format(Math.round(v));
const usd = (v, d = 2) => v == null ? '—' : 'US$ ' + n(v, d);
const pct = (v, d = 1) => v == null ? '—' : n(v, d) + '%';
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
const ic = (name, size = 16, sw = 1.6) => `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${sw}" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name] || ''}</svg>`;
const kilo = v => v == null ? '—' : v >= 1e6 ? n(v / 1e6, 1) + ' mi' : v >= 1e4 ? n(v / 1e3, 0) + ' mil' : v >= 1e3 ? n(v / 1e3, 1) + ' mil' : int(v);
const short = v => v >= 1e6 ? n(v / 1e6, 1) + 'M' : v >= 1e3 ? Math.round(v / 1e3) + 'k' : String(Math.round(v));
const dur = s => {
  if (s == null || isNaN(s)) return '—';
  s = Math.round(s);
  if (s < 60) return s + ' s';
  if (s < 3600) return Math.floor(s / 60) + ' min' + (s % 60 ? ' ' + (s % 60) + ' s' : '');
  if (s < 86400) return Math.floor(s / 3600) + ' h' + (Math.floor(s % 3600 / 60) ? ' ' + Math.floor(s % 3600 / 60) + ' min' : '');
  const d = s / 86400;
  return d < 14 ? n(d, d < 3 ? 1 : 0) + ' dias' : n(d / 7, 1) + ' semanas';
};
const MESES = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
const DIAS = ['domingo', 'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sábado'];
const hhmm = d => String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
const when = ts => {
  if (!ts) return '—';
  const d = new Date(ts * 1000), now = new Date();
  if (d.toDateString() === now.toDateString()) return hhmm(d);
  return `${d.getDate()} ${MESES[d.getMonth()]} · ${hhmm(d)}`;
};
const ago = ts => ts ? dur(Date.now() / 1000 - ts) : '—';
const futureDate = secs => {
  const d = new Date(Date.now() + secs * 1000);
  return secs < 86400 ? `${DIAS[d.getDay()].replace('-feira', '')} às ${hhmm(d)}` : `${DIAS[d.getDay()]}, ${d.getDate()} ${MESES[d.getMonth()]}`;
};
const journal = nome => String(nome || '').replace(/\s*\(PE\).*$/, '').replace(/\s*:.*$/, '').trim();
const years = nome => { const m = String(nome || '').match(/(\d{4})\s*a\s*(\d{4})/); return m ? [+m[1], +m[2]] : null; };
const jobTitle = j => { const y = years(j.nome); return journal(j.nome) + (y ? ` · ${y[0]}–${y[1]}` : ''); };

async function api(path, body) {
  const opt = body === undefined ? {} : {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)};
  const r = await fetch(path, opt);
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.detail || `Erro ${r.status}`);
  return data;
}

let toastTimer;
function toast(msg, kind = '') {
  $('.toast')?.remove();
  const el = document.createElement('div');
  el.className = 'toast ' + kind;
  el.innerHTML = (kind === 'err' ? ic('alert', 15) : ic('check', 15)) + `<span>${esc(msg)}</span>`;
  document.body.appendChild(el);
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.remove(), 3800);
}
async function act(fn, ok) {
  try { const r = await fn(); if (ok) toast(ok); await refreshState(); return r; }
  catch (e) { toast(e.message, 'err'); }
}

// ---------------------------------------------------------------- estado global
let S = null;               // último /api/state
let ACERVOS = null;         // catálogo (carregado sob demanda)
let CONFIGS = null;
const cache = {};

async function refreshState() {
  try { S = await api('/api/state'); onState(); } catch (e) { /* o SSE tenta de novo */ }
}
function connect() {
  let es;
  try { es = new EventSource('/api/stream'); } catch (e) { setInterval(refreshState, 3000); return; }
  es.onmessage = ev => { S = JSON.parse(ev.data); onState(); };
  es.onerror = () => { if (S) { S._offline = true; chrome(); } };
}
async function loadAcervos(force) {
  if (!ACERVOS || force) ACERVOS = await api('/api/acervos');
  return ACERVOS;
}
async function loadConfigs() {
  if (!CONFIGS) CONFIGS = await api('/api/configs');
  return CONFIGS;
}

const derived = () => {
  const s = S.settings, t = S.totals, u = S.usage;
  const active = S.jobs.filter(j => j.status === 'ativo');
  const current = active[0] || null;
  const reading = S.in_progress.filter(p => p.status === 'lendo');
  // tokens por hora na última hora, para projetar a cota
  const perHourPct = u.per_pct ? (t.tokens_last_hour || 0) / u.per_pct : 0;
  const toLimit = u.pct != null && perHourPct > 0 ? (s.limite_pct - u.pct) / perHourPct * 3600 : null;
  return {s, t, u, active, current, reading, perHourPct, toLimit};
};

// ---------------------------------------------------------------- moldura (trilho + barra do topo)
const NAV = [
  ['Operação'], ['home', 'Visão geral', '#/'], ['queue', 'Trabalhos', '#/trabalhos', 'jobs'], ['page', 'Inspetor de página', '#/pagina'],
  ['Acervo'], ['map', 'Mapa do acervo', '#/acervo', 'acervo'], ['check', 'Revisão e qualidade', '#/revisao', 'rev'],
  ['Recursos'], ['gauge', 'Cota e custos', '#/cota'], ['cog', 'Ajustes', '#/ajustes'],
];
function railHTML(active) {
  const counts = {
    jobs: S ? S.jobs.filter(j => j.status === 'ativo' || j.status === 'pausado').length || '' : '',
    acervo: 303, rev: S ? (S.quality.revisar || '') : '',
  };
  const nav = NAV.map(([a, b, href, c]) => b === undefined ? `<div class="lbl">${a}</div>` :
    `<a class="${b === active ? 'on' : ''}" href="${href}"><span class="ic">${ic(a)}</span>${b}<span class="ct num">${c ? counts[c] : ''}</span></a>`).join('');
  let card = '';
  if (S) {
    const {s, u} = derived();
    const p = u.pct;
    card = `<div class="railcard" title="Estimativa a partir dos tokens de saída desde o último registro do Usage">
      <div class="row"><span class="t" style="color:#D9CFBE">Cota da semana</span><span class="spacer"></span><span class="t">${u.last_mark ? 'estimada' : 'sem registro'}</span></div>
      <div class="row" style="align-items:baseline;gap:6px"><span class="v num">${p == null ? '—' : pct(p)}</span><span class="t">de ${n(s.limite_pct, 0)}% permitido</span></div>
      <div class="rbar"><i style="width:${Math.min(100, p || 0)}%"></i><b style="position:absolute;left:${s.limite_pct}%;top:-2px;bottom:-2px;width:2px;background:#F4D0B8"></b></div>
    </div>`;
  }
  return `<a class="brand" href="#/" style="text-decoration:none"><div class="mark">S</div><div><div class="name">Scriptorium</div><div class="sub">Hemeroteca Digital · PE</div></div></a>
    <nav class="nav">${nav}</nav>${card}
    <div class="who"><div class="avatar">LS</div><div><div style="font-size:12.5px">Luiz Soares</div><div style="font-size:11px;color:var(--rail-muted)">Pesquisador responsável</div></div></div>`;
}
function runPill() {
  if (!S) return '<div class="runpill"><span class="dot"></span><span>Conectando…</span></div>';
  if (S._offline) return '<div class="runpill"><span class="dot err"></span><span>Sem conexão com o painel</span></div>';
  const {s, reading} = derived();
  if (s.paused) return `<div class="runpill" title="${esc(s.pause_reason || '')}"><span class="dot pause"></span><span>Pausado</span><button class="btn sm acc" data-act="resume">${ic('play', 13)} Continuar</button></div>`;
  const label = reading.length ? `Rodando · ${reading.length} ${reading.length === 1 ? 'leitor' : 'leitores'}` : 'Rodando · aguardando páginas';
  return `<div class="runpill"><span class="dot live"></span><span>${label}</span><button class="btn sm pri" data-act="pause">${ic('pause', 13)} Pausar</button></div>`;
}
function topHTML(crumbs) {
  return `<div class="crumbs">${crumbs}</div><div class="spacer"></div>
    <label class="search">${ic('search', 15)}<input id="q" placeholder="Ir para página (029033_04_00002) ou periódico…" autocomplete="off"><span class="kbd">/</span></label>
    <span id="pill">${runPill()}</span>`;
}
let currentNav = '', currentCrumbs = '';
function chrome() {
  $('#rail').innerHTML = railHTML(currentNav);
  const pill = $('#pill');
  if (pill) pill.innerHTML = runPill();
}
document.addEventListener('click', e => {
  const b = e.target.closest('[data-act]');
  if (!b) return;
  const a = b.dataset.act;
  if (a === 'pause') act(() => api('/api/control', {action: 'pause'}), 'Pausado. As leituras em andamento terminam e nada novo começa.');
  if (a === 'resume') act(() => api('/api/control', {action: 'resume'}), 'Processamento retomado');
});
document.addEventListener('keydown', e => {
  if (e.key === '/' && !/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)) { e.preventDefault(); $('#q')?.focus(); }
  if (e.key === 'Escape') closeDrawer();
});
document.addEventListener('keydown', async e => {
  if (e.key !== 'Enter' || e.target.id !== 'q') return;
  const q = e.target.value.trim();
  if (!q) return;
  const m = q.match(/^([0-9A-Za-z]+(?:_[0-9A-Za-z]+)*)_(\d{5})$/);
  if (m) {
    const job = S?.jobs.find(j => j.bib === m[1] && +m[2] >= j.first_page && +m[2] <= j.last_page);
    if (job) { location.hash = `#/pagina/${job.id}/${q}`; return; }
    toast('Essa página não está em nenhum trabalho. Programe a leitura no Mapa do acervo.', 'err');
    return;
  }
  location.hash = '#/acervo?q=' + encodeURIComponent(q);
});

// ---------------------------------------------------------------- gráficos (SVG à mão)
function spark(vals, w = 64, h = 26, color = '#8E2F14') {
  if (!vals.length || vals.every(v => !v)) return '';
  const mx = Math.max(...vals), mn = Math.min(...vals), rng = (mx - mn) || 1;
  const pts = vals.map((v, i) => [i * w / (vals.length - 1 || 1), h - 3 - (v - mn) / rng * (h - 6)]);
  const d = 'M' + pts.map(p => p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' L');
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><path d="${d} L${w},${h} L0,${h} Z" fill="${color}" opacity=".10"/><path d="${d}" fill="none" stroke="${color}" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
}

function rhythmChart(tl, hours) {
  const W = 640, H = 230, L = 34, R = 10, T = 12, B = 26;
  if (!tl.length) return '';
  const maxP = Math.max(4, ...tl.map(b => b.pages));
  const raw = maxP / 4, mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map(f => f * mag).find(v => v >= raw);
  const top = step * 4;
  const maxT = Math.max(1, ...tl.map(b => b.tokens));
  const bw = (W - L - R) / tl.length;
  const y = v => T + (H - T - B) * (1 - v / top);
  let g = '';
  for (let i = 0; i <= 4; i++) {
    const v = top * i / 4, yy = y(v);
    g += `<line x1="${L}" x2="${W - R}" y1="${yy}" y2="${yy}" stroke="#DDD5C5" stroke-dasharray="${i ? '2 3' : ''}"/><text x="${L - 8}" y="${yy + 3.5}" font-size="10" text-anchor="end" fill="#776F62">${n(v, v % 1 ? 1 : 0)}</text>`;
  }
  // faixas sem leitura (pausas longas) aparecem como área clara
  let bars = '', line = [], labels = '';
  const every = Math.ceil(tl.length / 8);
  tl.forEach((b, i) => {
    const x = L + i * bw;
    if (b.pages) bars += `<rect x="${x + bw * .14}" y="${y(b.pages)}" width="${bw * .72}" height="${H - B - y(b.pages)}" rx="1.5" fill="${i === tl.length - 1 ? '#8E2F14' : '#CF9C83'}"><title>${b.pages} páginas · ${int(b.tokens)} tokens</title></rect>`;
    line.push([x + bw / 2, T + (H - T - B) * (1 - b.tokens / maxT)]);
    if (i % every === 0) {
      const d = new Date(b.ts * 1000);
      const lab = hours > 24 ? `${DIAS[d.getDay()].slice(0, 3)} ${String(d.getHours()).padStart(2, '0')}h` : hhmm(d).replace(':00', 'h');
      labels += `<text x="${x + bw / 2}" y="${H - 8}" font-size="10" text-anchor="middle" fill="#776F62">${lab}</text>`;
    }
  });
  const path = 'M' + line.map(p => p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' L');
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" style="display:block">${g}${bars}<path d="${path}" fill="none" stroke="#1B1814" stroke-width="1.4" stroke-linejoin="round"/>${labels}</svg>`;
}

function quotaChart(marks, est, limit, toLimit) {
  const W = 420, H = 180, L = 26, R = 12, T = 14, B = 24;
  const now = Date.now() / 1000, start = now - 7 * 86400, end = now + 1.2 * 86400;
  const x = ts => L + (W - L - R) * (ts - start) / (end - start);
  const y = v => T + (H - T - B) * (1 - v / 100);
  let g = '';
  [0, 50, 100].forEach(v => { g += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}" stroke="#DDD5C5" stroke-dasharray="2 3"/><text x="${L - 6}" y="${y(v) + 3.5}" font-size="10" text-anchor="end" fill="#776F62">${v}</text>`; });
  g += `<line x1="${L}" x2="${W - R}" y1="${y(limit)}" y2="${y(limit)}" stroke="#A12E28" stroke-dasharray="4 3"/><text x="${W - R}" y="${y(limit) - 5}" font-size="10" text-anchor="end" fill="#A12E28">pausa automática ${n(limit, 0)}%</text>`;
  for (let i = 0; i <= 8; i++) {
    const ts = start + i * 86400;
    if (ts > end) break;
    const d = new Date(ts * 1000);
    g += `<text x="${x(ts)}" y="${H - 6}" font-size="10" text-anchor="middle" fill="#776F62">${DIAS[d.getDay()].slice(0, 3)}</text>`;
  }
  const pts = marks.filter(m => m.ts >= start).sort((a, b) => a.ts - b.ts).map(m => [m.ts, m.pct]);
  if (est != null) pts.push([now, est]);
  let body = '';
  if (pts.length) {
    const d = 'M' + pts.map(p => x(p[0]).toFixed(1) + ',' + y(p[1]).toFixed(1)).join(' L');
    body += `<path d="${d} L${x(pts[pts.length - 1][0])},${y(0)} L${x(pts[0][0])},${y(0)} Z" fill="#8E2F14" opacity=".08"/><path d="${d}" fill="none" stroke="#8E2F14" stroke-width="1.8"/>`;
    marks.filter(m => m.ts >= start).forEach(m => { body += `<rect x="${x(m.ts) - 3.5}" y="${y(m.pct) - 3.5}" width="7" height="7" transform="rotate(45 ${x(m.ts)} ${y(m.pct)})" fill="#1B1814"><title>${when(m.ts)} · ${m.pct}% registrado</title></rect>`; });
    if (est != null) {
      body += `<circle cx="${x(now)}" cy="${y(est)}" r="5" fill="#FBF9F3" stroke="#8E2F14" stroke-width="2"/>`;
      if (toLimit != null && toLimit > 0 && now + toLimit < end) {
        body += `<path d="M${x(now)},${y(est)} L${x(now + toLimit)},${y(limit)}" stroke="#8E2F14" stroke-dasharray="3 3" stroke-width="1.5"/><circle cx="${x(now + toLimit)}" cy="${y(limit)}" r="3.5" fill="#A12E28"/>`;
      }
    }
  }
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" style="display:block">${g}${body}</svg>`;
}

// ---------------------------------------------------------------- telas
const views = {};
let V = null;  // tela atual {name, params, mount, update}

/* ---------- Visão geral ---------- */
views.overview = {
  nav: 'Visão geral', crumbs: () => '<b>Visão geral</b>',
  hours: 24,
  mount() {
    return `<div class="content view-enter">
      <div id="ov-banner"></div>
      <div class="row" style="align-items:flex-end;gap:24px;flex-wrap:wrap">
        <div style="flex:1;min-width:300px"><div class="eyebrow" id="ov-date"></div><h1 class="h1" id="ov-h1" style="margin-top:8px"></h1><p class="lede" id="ov-lede"></p></div>
        <div class="row"><a class="btn" href="#/cota">${ic('coins', 15)} Cota e custos</a><a class="btn acc" href="#/acervo">${ic('plus', 15)} Programar leitura</a></div>
      </div>
      <div class="kpis" id="ov-kpis"></div>
      <div class="ov-grid">
        <div class="card"><div class="cardhead"><span class="t">Ritmo</span><span class="s">páginas lidas e tokens de saída</span><span class="spacer"></span>
          <div class="seg" id="ov-hours">${[6, 24, 168].map(h => `<button data-h="${h}" class="${h === this.hours ? 'on' : ''}">${h === 168 ? '7 dias' : h + ' h'}</button>`).join('')}</div></div>
          <div id="ov-chart" style="min-height:230px"></div>
          <div class="legend"><span><i style="background:#CF9C83"></i>páginas</span><span><i style="background:#1B1814;height:2px"></i>tokens de saída</span><span class="spacer"></span><span id="ov-chart-sum" class="muted"></span></div>
        </div>
        <div class="card"><div class="cardhead"><span class="t">Cota da assinatura</span><span class="s">últimos 7 dias</span><span class="spacer"></span><span id="ov-cal"></span></div>
          <div class="row" style="align-items:baseline;gap:12px"><span id="ov-qv" style="font-size:38px;font-weight:600;letter-spacing:-.02em" class="num"></span><span class="muted" id="ov-qs" style="font-size:12.5px"></span></div>
          <div id="ov-qchart" style="margin-top:6px"></div>
          <form class="row" id="ov-usage" style="margin-top:10px;flex-wrap:wrap">
            <label class="input" style="width:150px">Usage: <input name="pct" inputmode="decimal" style="border:0;outline:0;background:none;width:44px;font:600 13px var(--sans)" placeholder="74"> %</label>
            <button class="btn">Registrar leitura do Usage</button>
            <a class="link" href="https://claude.ai/settings/usage" target="_blank" rel="noopener">abrir Usage ${ic('chev', 13)}</a>
          </form>
        </div>
      </div>
      <div class="ov-bottom">
        <div class="col" style="gap:16px">
          <div><div class="row" style="margin-bottom:10px"><span style="font-weight:600">Lendo agora</span><span class="muted" style="font-size:12px">atualiza a cada 2 s</span><span class="spacer"></span><a class="link" href="#/trabalhos">Trabalhos ${ic('chev', 13)}</a></div>
            <div class="readers" id="ov-readers"></div></div>
          <div class="card"><div class="cardhead"><span class="t">Fila de trabalhos</span><span class="s">ordem de prioridade</span><span class="spacer"></span><a class="link" href="#/trabalhos">Gerenciar ${ic('chev', 13)}</a></div>
            <div id="ov-queue"></div></div>
        </div>
        <div class="col" style="gap:16px">
          <div class="card dark" id="ov-acervo"></div>
          <div class="card needs"><div class="cardhead"><span class="t">Precisa de você</span></div><div id="ov-needs"></div></div>
        </div>
      </div>
    </div>`;
  },
  bind(root) {
    $('#ov-hours', root).addEventListener('click', e => {
      const b = e.target.closest('button'); if (!b) return;
      this.hours = +b.dataset.h; $$('#ov-hours button').forEach(x => x.classList.toggle('on', x === b));
      this.loadTimeline();
    });
    $('#ov-usage', root).addEventListener('submit', e => {
      e.preventDefault();
      const v = parseFloat(e.target.pct.value.replace(',', '.'));
      if (isNaN(v)) return toast('Digite a porcentagem que o Usage mostra', 'err');
      act(() => api('/api/usage', {pct: v}), `Registrado: ${n(v, 0)}% da semana`).then(() => { e.target.pct.value = ''; });
    });
    this.loadTimeline();
  },
  async loadTimeline() {
    this.tl = await api('/api/timeline?hours=' + this.hours);
    this.drawTimeline();
  },
  drawTimeline() {
    if (!this.tl || !$('#ov-chart')) return;
    const total = this.tl.reduce((a, b) => a + b.pages, 0);
    $('#ov-chart').innerHTML = total ? rhythmChart(this.tl, this.hours) :
      `<div class="empty" style="padding:70px 0">Nenhuma página lida ${this.hours === 168 ? 'nos últimos 7 dias' : `nas últimas ${this.hours} h`}.</div>`;
    $('#ov-chart-sum').textContent = `${int(total)} páginas em ${this.hours === 168 ? '7 dias' : this.hours + ' h'}`;
  },
  update() {
    const {s, t, u, active, current, reading, toLimit} = derived();
    const now = new Date();
    $('#ov-date').textContent = `${DIAS[now.getDay()]}, ${now.getDate()} de ${MESES[now.getMonth()]} · ${hhmm(now)}`;
    // título e frase de situação
    let h1, lede;
    if (!S.jobs.length) { h1 = 'Nada na fila ainda.'; lede = 'Escolha um periódico no Mapa do acervo e programe a leitura. O painel baixa as imagens, lê com o modelo escolhido e mede cada página.'; }
    else if (s.paused) { h1 = 'O processamento está pausado.'; lede = esc(s.pause_reason || 'Aperte Continuar quando quiser retomar.'); }
    else if (!active.length) { h1 = 'Todos os trabalhos terminaram.'; lede = 'Programe a próxima leitura no Mapa do acervo.'; }
    else {
      h1 = 'O acervo está sendo lido.';
      const cfg = current.config;
      lede = `${reading.length || 'Nenhum'} ${reading.length === 1 ? 'leitor trabalha' : 'leitores trabalham'} no <b style="color:var(--ink);font-weight:500">${esc(jobTitle(current))}</b> com <span class="mono">${esc(cfg)}</span>.`;
      if (toLimit != null && toLimit > 0) lede += ` No ritmo da última hora, a cota chega a ${n(s.limite_pct, 0)}% ${futureDate(toLimit)} e o painel pausa sozinho.`;
      else if (u.pct == null) lede += ' Registre a % do Usage para o painel projetar a cota.';
    }
    $('#ov-h1').innerHTML = h1; $('#ov-lede').innerHTML = lede;
    $('#ov-banner').innerHTML = s.paused && s.pause_reason && !/aguardando início|pausado no painel/.test(s.pause_reason)
      ? `<div class="banner err">${ic('alert', 17)}<span>${esc(s.pause_reason)}</span><span class="spacer"></span><button class="btn sm acc" data-act="resume">${ic('play', 13)} Continuar</button></div>` : '';

    // indicadores
    const tl = this.tl || [];
    const sum24 = S.timeline.reduce((a, b) => a + b.pages, 0), tok24 = S.timeline.reduce((a, b) => a + b.tokens, 0);
    const series = S.timeline.map(b => b.pages), tseries = S.timeline.map(b => b.tokens);
    const q = S.quality;
    $('#ov-kpis').innerHTML = [
      ['Páginas lidas em 24 h', int(sum24), '', `${int(t.ok)} no total · ${int(t.restantes)} na fila`, spark(series)],
      ['Ritmo da última hora', n(t.pages_last_hour, 0), 'pág/h', t.avg_seconds ? `${dur(t.avg_seconds)} por página em média` : 'sem medição ainda', ''],
      ['Tokens de saída em 24 h', kilo(tok24).replace(/ (mi|mil)$/, ''), / mi$/.test(kilo(tok24)) ? 'mi' : / mil$/.test(kilo(tok24)) ? 'mil' : '', t.avg_tokens_out ? `${kilo(t.avg_tokens_out)} por página em média` : '—', spark(tseries)],
      ['Custo equivalente na API', usd(t.cost_usd, t.cost_usd >= 100 ? 0 : 2), '', `${usd(t.avg_cost)} por página · lote ${usd(t.cost_batch, 0)}`, ''],
      ['Qualidade estimada', q.confidence ? n(q.confidence * 100, 0) : '—', q.confidence ? '% confiança' : '', `${int(q.uncertain)} marcas de incerteza · ${int(q.revisar)} para revisar`, ''],
    ].map(([l, v, sm, d, sp]) => `<div class="card"><div class="kpi">${sp ? `<span class="sp">${sp}</span>` : ''}<span class="l">${l}</span><span class="v num">${v}${sm ? ` <small>${sm}</small>` : ''}</span><span class="d">${d}</span></div></div>`).join('');
    if (!this.tl || (this.hours === 24 && Date.now() - (this._tlAt || 0) > 30000)) { this._tlAt = Date.now(); if (this.hours === 24) { this.tl = S.timeline; this.drawTimeline(); } }

    // cota
    $('#ov-qv').textContent = u.pct == null ? '—' : pct(u.pct);
    $('#ov-qs').innerHTML = u.last_mark ? `usados · estimativa com ${kilo(u.tokens_since_mark)} tokens desde o registro de ${when(u.last_mark.ts)}` : 'registre a % que o painel de Usage do Claude mostra';
    $('#ov-cal').innerHTML = u.calibrated ? '<span class="tag acc">calibrada</span>' : '<span class="tag neutral">padrão 186 mil/1%</span>';
    $('#ov-qchart').innerHTML = quotaChart(S.usage_marks, u.pct, s.limite_pct, toLimit);

    // leitores
    const avg = t.avg_seconds || 150;
    const lendo = S.in_progress.filter(p => p.status === 'lendo');
    const shown = (lendo.length ? lendo : S.in_progress).slice(0, 6);
    const dl = S.in_progress.length - lendo.length;
    $('#ov-readers').innerHTML = shown.length ? shown.map((p, i) => {
      const prog = Math.min(96, (p.elapsed || 0) / avg * 100);
      const reading = p.status === 'lendo';
      return `<a class="card reader" href="#/trabalho/${p.job_id}" style="text-decoration:none">
        <div class="th" style="${reading ? `background-image:url(/api/image/${esc(p.page_id)})` : ''}"></div>
        <div class="col" style="flex:1;min-width:0;gap:4px">
          <div class="row"><span class="tag ${reading ? 'read' : 'neutral'}">${reading ? '● ' + esc((p.worker || '').replace('leitor-', 'Leitor ')) : 'Baixando'}</span><span class="spacer"></span><span class="muted num" style="font-size:12px">${dur(p.elapsed)}</span></div>
          <div class="nm">${esc(journal(p.nome))}</div>
          <div class="muted" style="font-size:12px">página ${int(p.page)}</div>
          <div class="mono faint" style="font-size:11px">${esc(p.page_id)}</div>
          <div class="bar" style="margin-top:4px"><i style="width:${prog}%;background:${reading ? 'var(--read)' : '#7E9BC4'}"></i></div>
        </div></a>`;
    }).join('') + (lendo.length && dl ? `<div class="muted" style="grid-column:1/-1;font-size:12px;display:flex;gap:6px;align-items:center">${ic('download', 14)} baixando ${dl} ${dl === 1 ? 'imagem' : 'imagens'} da Hemeroteca à frente da leitura</div>` : '') : `<div class="card" style="grid-column:1/-1"><div class="empty" style="padding:18px">${s.paused ? 'Pausado: nenhuma leitura em andamento.' : active.length ? 'Esperando a próxima imagem ficar pronta…' : 'Nenhum trabalho ativo.'}</div></div>`;

    // fila
    const list = S.jobs.filter(j => j.status !== 'cancelado').slice(0, 6);
    $('#ov-queue').innerHTML = list.length ? list.map(j => {
      const p = j.total ? j.ok / j.total : 0;
      const rest = j.total - j.ok - j.erro;
      const eta = j.status === 'concluido' ? 'concluído' : j.status === 'pausado' ? 'pausado' : t.avg_seconds && rest ? '~' + dur(rest * t.avg_seconds / Math.max(1, s.workers)) : '—';
      return `<div class="queue-row" onclick="location.hash='#/trabalho/${j.id}'">
        <span class="qid muted num">#${j.id}</span>
        <div style="min-width:0"><div class="row"><span class="qn" style="font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(jobTitle(j))}</span>${statusTag(j.status)}</div>
          <div class="muted" style="font-size:12px">págs ${int(j.first_page)}–${int(j.last_page)} · <span class="mono" style="font-size:11px">${esc(j.config)}</span> · ${j.backend === 'api' ? 'API' : 'assinatura'}</div></div>
        <div class="qbar"><div class="bar"><i style="width:${p * 100}%"></i></div><div class="muted num" style="font-size:12px;margin-top:5px">${int(j.ok)} de ${int(j.total)}</div></div>
        <div class="num" style="text-align:right;font-size:12.5px">${eta}</div></div>`;
    }).join('') + (S.eta_s ? `<div class="muted" style="font-size:12px;padding-top:12px;display:flex;gap:6px;align-items:center">${ic('clock', 14)}<span>Fim previsto da fila no ritmo atual: <b style="color:var(--ink);font-weight:500">${futureDate(S.eta_s)}</b>, sem contar pausas de cota.</span></div>` : '')
      : `<div class="empty">Nenhum trabalho. <a class="link" href="#/acervo">Programar leitura</a></div>`;

    // acervo transcrito
    const tot = S.acervo_total || 1125402;
    $('#ov-acervo').innerHTML = `<div class="eyebrow">Acervo transcrito</div>
      <div style="margin-top:10px"><span class="big num">${int(t.ok)}</span> <span style="color:var(--rail-muted)">de ${int(tot)} páginas</span></div>
      <div class="rbar"><i style="width:${Math.max(0.6, t.ok / tot * 100)}%"></i></div>
      <div class="row" style="font-size:12px;color:var(--rail-muted)"><span>${n(t.ok / tot * 100, 2)}% do acervo de PE</span><span class="spacer"></span><span>303 periódicos</span></div>
      <div class="divider" style="background:#3A342B;margin:14px 0 6px"></div>
      <div class="li"><span>Pessoas mencionadas</span><b class="num">${int(q.people)}</b></div>
      <div class="li"><span>Trechos (segmentos)</span><b class="num">${int(q.segments)}</b></div>
      <div class="li"><span>Tokens de saída</span><b class="num">${kilo(t.tokens_out)}</b></div>`;

    // precisa de você
    const needs = [];
    if (s.paused && s.pause_reason && !/aguardando início|pausado no painel/.test(s.pause_reason)) needs.push(['alert', 'var(--err)', 'Pausa automática', s.pause_reason, '<button class="btn sm" data-act="resume">Continuar</button>']);
    if (q.revisar) needs.push(['flag', 'var(--warn)', `${int(q.revisar)} ${q.revisar === 1 ? 'página' : 'páginas'} com leitura incerta`, `${s.revisar_marcas}+ trechos marcados com [..]`, '<a class="btn sm" href="#/revisao">Revisar</a>']);
    const errs = S.jobs.reduce((a, j) => a + (j.status !== 'cancelado' ? j.erro : 0), 0);
    if (errs) { const j = S.jobs.find(j => j.erro && j.status !== 'cancelado'); needs.push(['alert', 'var(--err)', `${int(errs)} ${errs === 1 ? 'página falhou' : 'páginas falharam'}`, 'imagem não baixou ou leitura sem resposta', `<a class="btn sm" href="#/trabalho/${j.id}">Ver</a>`]); }
    if (!u.last_mark || Date.now() / 1000 - u.last_mark.ts > 6 * 3600) needs.push(['gauge', 'var(--read)', 'Registre o Usage de agora', u.last_mark ? `último registro há ${ago(u.last_mark.ts)}` : 'sem registro: a projeção usa a calibração padrão', '']);
    if (!S.jobs.length) needs.push(['map', 'var(--accent)', 'Programe a primeira leitura', 'Mapa do acervo, escolha o periódico e as páginas', '<a class="btn sm" href="#/acervo">Abrir</a>']);
    $('#ov-needs').innerHTML = needs.length ? needs.map(([i, c, tt, ss, b]) => `<div class="it"><span class="ic" style="color:${c}">${ic(i, 16)}</span><div style="flex:1;min-width:0"><div class="t">${esc(tt)}</div><div class="s">${esc(ss)}</div></div>${b}</div>`).join('')
      : `<div class="it"><span class="ic" style="color:var(--ok)">${ic('check', 16)}</span><div><div class="t">Tudo em ordem</div><div class="s">nada esperando por você agora</div></div></div>`;
  },
};

function statusTag(st) {
  return {ativo: '<span class="tag read">Ativo</span>', pausado: '<span class="tag warn">Pausado</span>', cancelado: '<span class="tag neutral">Cancelado</span>', concluido: '<span class="tag ok">Concluído</span>'}[st] || `<span class="tag neutral">${esc(st)}</span>`;
}
function pageTag(r, limit) {
  if (r.status === 'ok') {
    if (r.review === 'aprovada') return '<span class="tag ok">Aprovada</span>';
    if (r.review === 'revisar' || (r.uncertain || 0) >= limit) return '<span class="tag warn">Revisar</span>';
    return '<span class="tag ok">Lida</span>';
  }
  return {lendo: '<span class="tag read">Lendo</span>', baixando: '<span class="tag read" style="opacity:.75">Baixando</span>', pronta: '<span class="tag neutral">Imagem pronta</span>',
    pendente: '<span class="tag neutral">Na fila</span>', erro: '<span class="tag err">Erro</span>', cancelada: '<span class="tag neutral">Cancelada</span>'}[r.status] || r.status;
}

/* ---------- Trabalhos ---------- */
views.jobs = {
  nav: 'Trabalhos', crumbs: () => 'Operação / <b>Trabalhos</b>',
  mount() {
    return `<div class="content view-enter">
      <div class="row" style="align-items:flex-end;flex-wrap:wrap;gap:16px"><div style="flex:1"><div class="eyebrow">Operação</div><h1 class="h1" style="margin-top:8px">Trabalhos</h1>
        <p class="lede">Cada trabalho é um intervalo de páginas de um periódico lido com um método. A fila segue a prioridade; pausar um trabalho não para os outros.</p></div>
        <a class="btn acc" href="#/acervo">${ic('plus', 15)} Programar leitura</a></div>
      <div class="card"><div class="tbl-wrap"><table><thead><tr><th>#</th><th>Periódico e páginas</th><th>Método</th><th>Situação</th><th style="width:200px">Progresso</th><th class="r">Tokens</th><th class="r">US$ API</th><th class="r">Prioridade</th><th></th></tr></thead><tbody id="jb-rows"></tbody></table></div></div>
      <div class="card"><div class="cardhead"><span class="t">Atividade</span><span class="s">o que o processador fez por último</span></div><div class="events" id="jb-events"></div></div>
    </div>`;
  },
  bind(root) {
    $('#jb-rows', root).addEventListener('click', e => {
      const b = e.target.closest('button[data-job]');
      if (b) { e.stopPropagation(); jobAction(+b.dataset.job, b.dataset.do); return; }
      const tr = e.target.closest('tr[data-id]');
      if (tr) location.hash = '#/trabalho/' + tr.dataset.id;
    });
  },
  update() {
    $('#jb-rows').innerHTML = S.jobs.length ? S.jobs.map(j => {
      const p = j.total ? j.ok / j.total : 0;
      return `<tr class="click" data-id="${j.id}"><td class="muted num">#${j.id}</td>
        <td><div style="font-weight:500">${esc(jobTitle(j))}</div><div class="muted" style="font-size:12px">págs ${int(j.first_page)}–${int(j.last_page)} · <span class="mono" style="font-size:11px">${esc(j.bib)}</span></div></td>
        <td><span class="mono">${esc(j.config)}</span><div class="muted" style="font-size:11.5px">${j.backend === 'api' ? 'API' : 'assinatura'}</div></td>
        <td>${statusTag(j.status)}${j.erro ? ` <span class="tag err">${j.erro} erro${j.erro > 1 ? 's' : ''}</span>` : ''}</td>
        <td><div class="bar"><i style="width:${p * 100}%"></i></div><div class="muted num" style="font-size:11.5px;margin-top:4px">${int(j.ok)} de ${int(j.total)} · ${pct(p * 100)}</div></td>
        <td class="r num">${kilo(j.tokens_out)}</td><td class="r num">${n(j.cost_usd, 2)}</td>
        <td class="r num"><div class="row" style="justify-content:flex-end;gap:2px"><button class="btn sm ghost" data-job="${j.id}" data-do="down" title="Baixar prioridade">−</button><span style="min-width:18px;text-align:center">${j.priority}</span><button class="btn sm ghost" data-job="${j.id}" data-do="up" title="Subir prioridade">+</button></div></td>
        <td class="r">${jobButtons(j)}</td></tr>`;
    }).join('') : `<tr><td colspan="9"><div class="empty"><div class="h2">Nenhum trabalho ainda</div>Escolha um periódico no <a class="link" href="#/acervo">Mapa do acervo</a>.</div></td></tr>`;
    $('#jb-events').innerHTML = eventsHTML(S.events);
  },
};
function jobButtons(j) {
  if (j.status === 'ativo') return `<button class="btn sm" data-job="${j.id}" data-do="pausado">${ic('pause', 13)} Pausar</button>`;
  if (j.status === 'pausado') return `<button class="btn sm" data-job="${j.id}" data-do="ativo">${ic('play', 13)} Retomar</button>`;
  if (j.status === 'cancelado') return `<button class="btn sm ghost" data-job="${j.id}" data-do="ativo">Reativar</button>`;
  return '';
}
function jobAction(id, what) {
  const j = S.jobs.find(x => x.id === id);
  if (what === 'up' || what === 'down') return act(() => api('/api/jobs/' + id, {priority: j.priority + (what === 'up' ? 1 : -1)}));
  if (what === 'retry') return act(() => api('/api/jobs/' + id, {retry_errors: true}), 'As páginas com erro voltaram para a fila');
  if (what === 'cancelado' && !confirm(`Cancelar o trabalho #${id}? Páginas já lidas ficam guardadas; as que faltam saem da fila.`)) return;
  const msg = {pausado: `Trabalho #${id} pausado`, ativo: `Trabalho #${id} retomado`, cancelado: `Trabalho #${id} cancelado`}[what];
  return act(() => api('/api/jobs/' + id, {status: what}), msg);
}
function eventsHTML(evs) {
  return evs.length ? evs.map(e => `<div class="ev"><span class="muted num">${when(e.ts)}</span><span class="lv-${e.level}">${esc(e.msg)}</span></div>`).join('') : '<div class="empty">Sem eventos.</div>';
}

/* ---------- Trabalho (detalhe) ---------- */
const CELL = {o: ['c-ok', 'Lida'], r: ['c-rev', 'Revisar'], e: ['c-err', 'Erro'], l: ['c-read', 'Lendo'], p: ['c-ready', 'Imagem pronta'], d: ['c-dl', 'Baixando'], q: ['c-pend', 'Na fila'], c: ['c-canc', 'Cancelada']};
views.job = {
  nav: 'Trabalhos',
  crumbs(p) { const j = S?.jobs.find(x => x.id === +p[0]); return `<a href="#/trabalhos" style="text-decoration:none">Trabalhos</a> / <b>#${esc(p[0])}${j ? ' · ' + esc(jobTitle(j)) : ''}</b>`; },
  filter: '',
  mount(p) {
    this.id = +p[0]; this.data = null; this.filter = ''; this.limit = 25;
    return `<div class="content view-enter" id="jd"><div class="empty">Carregando…</div></div>`;
  },
  async fetch() {
    const q = `?limit=${this.limit}` + (this.filter ? '&status=' + this.filter : '');
    try { this.data = await api(`/api/jobs/${this.id}${q}`); } catch (e) { $('#jd').innerHTML = `<div class="empty"><div class="h2">Trabalho não encontrado</div><a class="link" href="#/trabalhos">Voltar aos trabalhos</a></div>`; return; }
    this._at = Date.now();
    this.render();
  },
  bind() { this.fetch(); },
  update() { if (Date.now() - (this._at || 0) > 3500) this.fetch(); },
  render() {
    const {job: j, stats: st, ribbon, rows} = this.data;
    const limit = S.settings.revisar_marcas;
    const root = $('#jd');
    if (!root.dataset.built) {
      root.dataset.built = 1;
      root.innerHTML = `
        <div class="row" style="align-items:flex-end;gap:16px;flex-wrap:wrap"><div style="flex:1;min-width:300px">
          <div class="row" id="jd-tags"></div><h1 class="h1" style="margin-top:10px" id="jd-title"></h1><p class="lede" id="jd-sub"></p></div>
          <div class="row" id="jd-actions"></div></div>
        <div class="kpis" id="jd-kpis"></div>
        <div class="card"><div class="cardhead"><span class="t">Fita de páginas</span><span class="s" id="jd-scale"></span><span class="spacer"></span><span class="s">clique numa página lida para abrir no inspetor</span></div>
          <div class="ribbon" id="jd-ribbon"></div><div class="legend" id="jd-legend"></div></div>
        <div class="job-grid">
          <div class="card"><div class="cardhead"><span class="t">Tokens de saída por página</span><span class="s">distribuição</span></div><div id="jd-hist"></div></div>
          <div class="card"><div class="cardhead"><span class="t">Erros e avisos</span><span class="spacer"></span><span id="jd-retry"></span></div><div class="errs" id="jd-errs"></div></div>
        </div>
        <div class="card"><div class="cardhead"><span class="t">Páginas</span><span class="s">mais recentes primeiro</span><span class="spacer"></span>
          <div class="seg" id="jd-filter">${[['', 'Todas'], ['ok', 'Lidas'], ['lendo', 'Lendo'], ['erro', 'Erros'], ['pendente', 'Na fila']].map(([k, l]) => `<button data-f="${k}" class="${k === this.filter ? 'on' : ''}">${l}</button>`).join('')}</div></div>
          <div class="tbl-wrap"><table><thead><tr><th>Página</th><th class="r">Trechos</th><th class="r">Pessoas</th><th class="r">Incertezas</th><th class="r">Tokens</th><th class="r">Tempo</th><th class="r">US$</th><th>Situação</th><th></th></tr></thead><tbody id="jd-rows"></tbody></table></div><div id="jd-more"></div></div>`;
      $('#jd-filter').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; this.filter = b.dataset.f; $$('#jd-filter button').forEach(x => x.classList.toggle('on', x === b)); this.fetch(); });
      $('#jd-actions').addEventListener('click', e => { const b = e.target.closest('button[data-do]'); if (b) jobAction(this.id, b.dataset.do).then(() => this.fetch()); });
      $('#jd-retry').addEventListener('click', e => { if (e.target.closest('button')) jobAction(this.id, 'retry').then(() => this.fetch()); });
      $('#jd-ribbon').addEventListener('click', e => {
        const c = e.target.closest('.cell'); if (!c) return;
        const pg = +c.dataset.p, code = c.dataset.c;
        if (code === 'o' || code === 'r') location.hash = `#/pagina/${this.id}/${this.data.job.bib}_${String(pg).padStart(5, '0')}`;
      });
      $('#jd-rows').addEventListener('click', e => { const tr = e.target.closest('tr[data-pid]'); if (tr && tr.dataset.ok) location.hash = `#/pagina/${this.id}/${tr.dataset.pid}`; });
    }
    $('#jd-tags').innerHTML = `<span class="eyebrow">Trabalho #${j.id}</span>${statusTag(j.status)}<span class="tag neutral">${esc(j.config)} · ${j.backend === 'api' ? 'API' : 'assinatura'}</span><span class="tag neutral">Prioridade ${j.priority}</span>`;
    $('#jd-title').textContent = jobTitle(j);
    $('#jd-sub').innerHTML = `Páginas ${int(j.first_page)} a ${int(j.last_page)} · criado em ${when(Date.parse(j.created_at) / 1000)} · resultados em <span class="mono">data/extractions/visao/${esc(j.config)}/${esc(j.bib)}/</span>`;
    $('#jd-actions').innerHTML = jobButtons(j).replace('Pausar', 'Pausar trabalho') + (j.status !== 'cancelado' && j.status !== 'concluido' ? `<button class="btn ghost danger" data-do="cancelado">${ic('x', 14)} Cancelar</button>` : '');
    const rest = j.total - j.ok - j.erro;
    const eta = st.avg_s && rest ? rest * st.avg_s / Math.max(1, S.settings.workers) : null;
    const attn = (ribbon.codes.match(/r/g) || []).length;
    $('#jd-kpis').innerHTML = [
      ['Progresso', `${int(j.ok)} <small>de ${int(j.total)}</small>`, `<div class="bar acc" style="margin:2px 0 4px"><i style="width:${j.total ? j.ok / j.total * 100 : 0}%"></i></div>${pct(j.total ? j.ok / j.total * 100 : 0)}${eta ? ' · termina ' + futureDate(eta) : ''}`],
      ['Tempo médio', st.avg_s ? `${Math.round(st.avg_s)} <small>s/pág</small>` : '—', st.min_s ? `mín. ${dur(st.min_s)} · máx. ${dur(st.max_s)}` : 'sem páginas lidas'],
      ['Tokens de saída', kilo(j.tokens_out).replace(/ (mi|mil)$/, '') + (/ mi$/.test(kilo(j.tokens_out)) ? ' <small>mi</small>' : / mil$/.test(kilo(j.tokens_out)) ? ' <small>mil</small>' : ''), st.avg_out ? `${kilo(st.avg_out)} por página` : '—'],
      ['Custo equivalente', usd(j.cost_usd, j.cost_usd >= 100 ? 0 : 2), j.backend === 'api' ? `lote seria ${usd(st.cost_batch)}` : 'pago pela assinatura'],
      ['Atenção', int(attn + j.erro), `<span style="color:var(--warn)">${attn} para revisar</span> · <span style="color:var(--err)">${j.erro} erros</span>`],
    ].map(([l, v, d], i) => `<div class="card ${i === 4 && attn + j.erro ? 'k-attn' : ''}"><div class="kpi"><span class="l">${l}</span><span class="v num">${v}</span><span class="d">${d}</span></div></div>`).join('');

    // fita: até 2.000 quadrados; acima disso cada quadrado agrupa k páginas
    const codes = ribbon.codes, N = codes.length, k = Math.max(1, Math.ceil(N / 2000));
    const order = 'erldp';
    let cells = '';
    for (let i = 0; i < N; i += k) {
      const chunk = codes.slice(i, i + k);
      let c = [...order].find(x => chunk.includes(x));
      if (!c) c = (chunk.split('o').length - 1) >= chunk.length / 2 ? 'o' : chunk.includes('q') ? 'q' : chunk[0];
      const pg = ribbon.first + i;
      cells += `<i class="cell ${CELL[c][0]}" data-p="${pg}" data-c="${c}" title="pág. ${pg}${k > 1 ? '–' + Math.min(ribbon.first + N - 1, pg + k - 1) : ''} · ${CELL[c][1]}"></i>`;
    }
    $('#jd-ribbon').innerHTML = cells;
    $('#jd-scale').textContent = k > 1 ? `cada quadrado = ${k} páginas, de ${int(ribbon.first)} a ${int(ribbon.first + N - 1)}` : `uma página por quadrado, de ${int(ribbon.first)} a ${int(ribbon.first + N - 1)}`;
    $('#jd-legend').innerHTML = Object.entries(CELL).map(([c, [cls, l]]) => { const cnt = (codes.split(c).length - 1); return cnt ? `<span><i class="${cls}"></i>${l} <b class="num">${int(cnt)}</b></span>` : ''; }).join('');

    // histograma (faixas de 2.500 tokens)
    const h = st.histogram || {}, keys = Object.keys(h).map(Number);
    if (keys.length) {
      const mxB = Math.max(...keys, 15), mxN = Math.max(...Object.values(h));
      let bars = '';
      for (let b = 0; b <= mxB; b++) bars += `<i class="${h[b] === mxN ? 'mx' : ''}" style="height:${(h[b] || 0) / mxN * 100}%" title="${int(b * 2500)}–${int((b + 1) * 2500)} tokens: ${h[b] || 0} páginas"></i>`;
      $('#jd-hist').innerHTML = `<div class="hist">${bars}</div><div class="row muted" style="font-size:11px;margin-top:6px"><span>0</span><span class="spacer"></span><span>${short(mxB * 2500 / 2)}</span><span class="spacer"></span><span>${short((mxB + 1) * 2500)} tokens</span></div>
        <p class="muted" style="font-size:12px;margin-top:10px">Páginas densas, com 5 a 8 colunas de anúncios, gastam mais saída e levam mais tempo. A média deste trabalho é ${kilo(st.avg_out)} tokens.</p>`;
    } else $('#jd-hist').innerHTML = '<div class="empty">Sem páginas lidas ainda.</div>';
    $('#jd-retry').innerHTML = j.erro ? `<button class="btn sm">Refazer ${j.erro} ${j.erro === 1 ? 'erro' : 'erros'}</button>` : '';
    const errs = st.errors.map(e => `<div class="it"><span class="count">${e.n}</span><div style="min-width:0"><div style="font-weight:500">${esc(friendlyError(e.error))}</div><div class="muted mono" style="font-size:11px;word-break:break-all">págs ${esc((e.pages || '').split(',').slice(0, 8).join(', '))}${(e.pages || '').split(',').length > 8 ? '…' : ''}</div></div></div>`);
    if (attn) errs.push(`<div class="it"><span class="count" style="background:var(--warn-soft);color:var(--warn)">${attn}</span><div><div style="font-weight:500">Leitura incerta</div><div class="muted" style="font-size:12px">${limit} ou mais marcas [..] na página · <a class="link" href="#/revisao">revisar</a></div></div></div>`);
    $('#jd-errs').innerHTML = errs.join('') || `<div class="it"><span style="color:var(--ok)">${ic('check', 16)}</span><span>Nenhum erro neste trabalho.</span></div>`;

    $('#jd-rows').innerHTML = rows.length ? rows.map(r => `<tr class="${r.status === 'ok' ? 'click' : ''}" data-pid="${esc(r.page_id)}" ${r.status === 'ok' ? 'data-ok="1"' : ''}>
      <td><span class="mono">${esc(r.page_id)}</span>${r.error ? `<div class="muted" style="font-size:11.5px;color:var(--err)">${esc(friendlyError(r.error))}</div>` : ''}</td>
      <td class="r num">${r.segments ?? '—'}</td><td class="r num">${r.people ?? '—'}</td><td class="r num">${r.uncertain ?? '—'}</td>
      <td class="r num">${r.tokens_out ? int(r.tokens_out) : '—'}</td><td class="r num">${r.seconds ? dur(r.seconds) : '—'}</td><td class="r num">${r.cost_usd ? n(r.cost_usd, 2) : '—'}</td>
      <td>${pageTag(r, limit)}</td><td class="r">${r.status === 'ok' ? `<span class="link">Abrir ${ic('chev', 13)}</span>` : ''}</td></tr>`).join('')
      : '<tr><td colspan="9"><div class="empty">Nenhuma página nesta situação.</div></td></tr>';
    $('#jd-more').innerHTML = rows.length >= this.limit ? '<div class="empty" style="padding:14px"><button class="btn sm">Mostrar mais 50</button></div>' : '';
    const mb = $('#jd-more button'); if (mb) mb.onclick = () => { this.limit += 50; this.fetch(); };
  },
};
function friendlyError(e) {
  e = String(e || '');
  if (/baixar a imagem/.test(e)) return 'Imagem não baixou da Hemeroteca';
  if (/timeout|timed out/i.test(e)) return 'A leitura passou do tempo limite';
  if (/json|schema|parse/i.test(e)) return 'Resposta fora do formato esperado';
  return e.length > 120 ? e.slice(0, 120) + '…' : e;
}

/* ---------- Inspetor ---------- */
const SEGTYPE = {edital: 'Edital', ato_governo: 'Ato do governo', nomeacao: 'Nomeação', sentenca_judicial: 'Sentença', fuga_escravizado: 'Fuga', venda_escravizado: 'Venda', movimento_porto: 'Movimento do porto', obito: 'Óbito', leilao: 'Leilão', anuncio: 'Anúncio', correspondencia: 'Correspondência', noticia: 'Notícia', outro: 'Outro'};
const PRED = {spouse_of: 'cônjuge de', child_of: 'filho(a) de', parent_of: 'pai/mãe de', widow_of: 'viúva(o) de', holds_role: 'exerce', appointed_to: 'nomeado para', member_of: 'membro de', resident_of: 'morador de', slave_of: 'escravizado por', accused_of: 'acusado de', victim_of: 'vítima de', sentenced_to: 'condenado a', absolved: 'absolvido', deceased: 'falecido', traveled_on: 'viajou no', signed_by: 'assinado por', creditor_of: 'credor de', debtor_of: 'devedor de', owner_of: 'dono de'};
views.page = {
  nav: 'Inspetor de página', full: true,
  crumbs(p) { return p.length ? `<a href="#/trabalho/${esc(p[0])}" style="text-decoration:none">Trabalho #${esc(p[0])}</a> / <b class="mono" style="font-size:12.5px">${esc(p[1])}</b>` : 'Operação / <b>Inspetor de página</b>'; },
  mount(p) {
    this.p = p; this.tab = 'texto'; this.zoom = null; this.sel = null;
    if (!p.length) return `<div class="content view-enter"><div class="eyebrow">Inspetor</div><h1 class="h1" style="margin-top:8px">Escolha uma página</h1>
      <p class="lede">As últimas páginas lidas. Também dá para digitar o identificador na busca do topo.</p><div class="card" id="pg-list"></div></div>`;
    return `<div class="insp-wrap view-enter"><div id="pg-head" class="insp-head"><div class="empty" style="padding:6px">Carregando página…</div></div>
      <div class="insp"><div class="scan" id="pg-scan"></div><aside class="side" id="pg-side"></aside></div></div>`;
  },
  async bind() {
    if (!this.p.length) {
      const rows = S.recent.filter(r => r.status === 'ok');
      $('#pg-list').innerHTML = rows.length ? `<div class="tbl-wrap"><table><thead><tr><th>Página</th><th>Periódico</th><th class="r">Trechos</th><th class="r">Pessoas</th><th class="r">Lida</th><th></th></tr></thead><tbody>${rows.map(r => `<tr class="click" onclick="location.hash='#/pagina/${r.job_id}/${r.page_id}'"><td class="mono">${esc(r.page_id)}</td><td>${esc(journal(r.nome))}</td><td class="r num">${r.segments ?? '—'}</td><td class="r num">${r.people ?? '—'}</td><td class="r num">${when(r.finished_at)}</td><td class="r"><span class="link">Abrir ${ic('chev', 13)}</span></td></tr>`).join('')}</tbody></table></div>` : '<div class="empty">Nenhuma página lida ainda.</div>';
      return;
    }
    let d;
    try { d = await api(`/api/page/${this.p[0]}/${this.p[1]}`); } catch (e) { $('#pg-head').innerHTML = `<div class="empty">${esc(e.message)}</div>`; return; }
    this.d = d; this.renderHead(); this.renderScan(); this.renderSide();
  },
  update() {},
  renderHead() {
    const {row, extraction: x, neighbors: nb, nome, meta} = this.d;
    const segs = x?.segments || [];
    const unc = row.uncertain || 0;
    const title = [journal(x?.newspaper ? titleCase(x.newspaper) : nome) || journal(nome), x?.issue_date ? niceDate(x.issue_date) : null, x?.page_number ? 'pág. ' + x.page_number : null].filter(Boolean).join(' · ');
    const tagModel = row.status === 'ok' ? `<span class="tag ok">${ic('check', 12)} Lida por ${esc(modelName(meta.model || row.config))}</span>` : pageTag(row, S.settings.revisar_marcas);
    $('#pg-head').innerHTML = `
      <a class="btn sm" ${nb.prev ? `href="#/pagina/${row.job_id}/${nb.prev}"` : 'aria-disabled="true" style="opacity:.4;pointer-events:none"'} title="Página lida anterior">${ic('arrowl', 15)}</a>
      <a class="btn sm" ${nb.next ? `href="#/pagina/${row.job_id}/${nb.next}"` : 'aria-disabled="true" style="opacity:.4;pointer-events:none"'} title="Próxima página lida">${ic('chev', 15)}</a>
      <div style="min-width:0;flex:1"><div class="serif" style="font-size:21px;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(title)}</div>
        <div class="mono muted" style="font-size:11px">${esc(row.page_id)}${x?.columns ? ` · ${x.columns} colunas` : ''} · ${segs.length} trechos</div></div>
      ${tagModel}${unc ? `<span class="tag warn">${unc} incerteza${unc > 1 ? 's' : ''}</span>` : ''}
      <button class="btn sm" id="pg-cite">${ic('quote', 14)} Citar</button>
      <button class="btn sm" id="pg-flag">${ic('flag', 14)} ${row.review === 'revisar' ? 'Tirar da revisão' : 'Marcar para revisão'}</button>
      <button class="btn sm ${row.review === 'aprovada' ? '' : 'pri'}" id="pg-ok">${ic('check', 14)} ${row.review === 'aprovada' ? 'Aprovada' : 'Aprovar página'}</button>`;
    $('#pg-cite').onclick = () => { navigator.clipboard?.writeText(this.citation()); toast('Citação copiada'); };
    $('#pg-flag').onclick = () => this.review(row.review === 'revisar' ? null : 'revisar');
    $('#pg-ok').onclick = () => this.review(row.review === 'aprovada' ? null : 'aprovada');
  },
  async review(v) {
    const {row} = this.d;
    await act(() => api(`/api/page/${row.job_id}/${row.page_id}/review`, {value: v}), v === 'aprovada' ? 'Página aprovada' : v === 'revisar' ? 'Marcada para revisão' : 'Marcação removida');
    row.review = v; this.renderHead();
    if (v === 'aprovada' && this.d.neighbors.next && location.hash.includes('fila=1')) location.hash = `#/pagina/${row.job_id}/${this.d.neighbors.next}?fila=1`;
  },
  citation() {
    const {row, extraction: x, nome, meta} = this.d;
    const d = new Date();
    return `${(x?.newspaper || journal(nome)).toUpperCase()}. Recife, ${x?.issue_date ? niceDate(x.issue_date) : 's.d.'}${x?.page_number ? ', p. ' + x.page_number : ''}. Transcrição: Scriptorium · ${modelName(meta.model || row.config)}, ${d.getDate()} ${MESES[d.getMonth()]}. ${d.getFullYear()}. Hemeroteca Digital Brasileira, ${row.page_id}.`;
  },
  renderScan() {
    const {row, meta, has_image} = this.d;
    const el = $('#pg-scan');
    if (!has_image) { el.innerHTML = `<div class="empty" style="padding:80px 30px"><div class="h2">A imagem não está no disco</div>Ela foi apagada depois da leitura (ajuste “apagar imagens”) ou ainda não foi baixada.</div>`; return; }
    const boxes = meta.tile_boxes || [];
    el.innerHTML = `<div class="stage" id="pg-stage"><img id="pg-img" src="/api/image/${esc(row.page_id)}" alt="Scan da página ${esc(row.page_id)}">${boxes.map((b, i) => `<div class="tile" data-t="${i}"><b>${i + 1}</b></div>`).join('')}</div>
      <div class="zoombar"><button class="btn sm ghost" data-z="-">−</button><span class="z num" id="pg-z"></span><button class="btn sm ghost" data-z="+">+</button><span style="width:1px;height:18px;background:var(--rule)"></span><button class="btn sm ghost" data-z="fit">Ajustar</button><button class="btn sm ghost" data-z="tiles">${ic('layers', 14)} Recortes</button></div>`;
    const img = $('#pg-img');
    const place = () => {
      const W = img.naturalWidth, H = img.naturalHeight;
      if (!W) return;
      if (this.zoom == null) this.zoom = Math.min(1, (el.clientWidth - 48) / W);
      const st = $('#pg-stage'); st.style.width = W * this.zoom + 'px';
      $$('.tile', st).forEach(t => { const b = boxes[+t.dataset.t]; Object.assign(t.style, {left: b[0] / W * 100 + '%', top: b[1] / H * 100 + '%', width: (b[2] - b[0]) / W * 100 + '%', height: (b[3] - b[1]) / H * 100 + '%'}); });
      $('#pg-z').textContent = Math.round(this.zoom * 100) + '%';
    };
    img.onload = place; if (img.complete) place();
    el.querySelector('.zoombar').onclick = e => {
      const b = e.target.closest('button'); if (!b) return;
      const z = b.dataset.z;
      if (z === 'tiles') { el.classList.toggle('no-tiles'); return; }
      this.zoom = z === 'fit' ? null : Math.max(.1, Math.min(2, (this.zoom || .5) * (z === '+' ? 1.25 : .8)));
      place();
    };
  },
  renderSide() {
    const {row, extraction: x, meta} = this.d;
    const segs = x?.segments || [];
    const people = segs.reduce((a, s) => a + (s.people || []).length, 0);
    const rels = segs.reduce((a, s) => a + (s.relations || []).length, 0);
    $('#pg-side').innerHTML = `<div class="tabs" id="pg-tabs">${[['texto', 'Transcrição'], ['pessoas', `Pessoas e relações ${people}`], ['prov', 'Proveniência']].map(([k, l]) => `<button data-t="${k}" class="${k === this.tab ? 'on' : ''}">${l}</button>`).join('')}</div>
      <div class="side-body" id="pg-body"></div>
      <div class="side-foot"><div style="font-size:11px;margin-bottom:4px">Como citar</div><div class="row" style="align-items:flex-start;gap:12px"><div class="cite" style="flex:1">${esc(this.citation())}</div><button class="btn sm" id="pg-copy">Copiar</button></div>
        <div class="row num" style="margin-top:10px;font-size:11.5px;gap:14px;flex-wrap:wrap"><span>${esc(modelName(meta.model || row.config))}${meta.grid ? ` · grade ${meta.grid.join('×')}` : ''}</span><span>${int(row.tokens_out)} tokens</span><span>${dur(row.seconds)}</span><span>${usd(row.cost_usd)}</span><span class="spacer"></span><span>${when(row.finished_at)}</span></div></div>`;
    $('#pg-tabs').onclick = e => { const b = e.target.closest('button'); if (!b) return; this.tab = b.dataset.t; $$('#pg-tabs button').forEach(z => z.classList.toggle('on', z === b)); this.renderBody(); };
    $('#pg-copy').onclick = () => { navigator.clipboard?.writeText(this.citation()); toast('Citação copiada'); };
    this.renderBody();
  },
  renderBody() {
    const {row, extraction: x, meta} = this.d;
    const segs = x?.segments || [];
    const body = $('#pg-body');
    if (!x) { body.innerHTML = `<div class="empty" style="padding:50px 20px">${row.status === 'erro' ? 'A leitura falhou: ' + esc(friendlyError(row.error)) : 'Esta página ainda não foi lida.'}</div>`; return; }
    if (this.tab === 'texto') {
      body.innerHTML = `<div class="filters-mark"><span style="border-bottom:1.5px solid #9DB3D3">Nome</span><span style="border-bottom:1.5px solid #C9A15E">Lugar ou navio</span><span><span class="unc">[da]</span> leitura incerta</span><span><span class="ill">ilegível</span></span></div>` +
        (x.illegible_notes ? `<div style="padding:12px 20px;border-bottom:1px solid var(--rule);font-size:12.5px;color:var(--warn);display:flex;gap:8px">${ic('alert', 15)}<span>${esc(x.illegible_notes)}</span></div>` : '') +
        segs.map((s, i) => {
          const unc = (s.text || '').match(/\[[^\[\]\n]{0,30}\]/g)?.length || 0;
          const long = (s.text || '').length > 900;
          return `<div class="segc" data-i="${i}"><div class="hd"><span class="n">${i + 1}</span><span class="tag type">${esc(SEGTYPE[s.type] || s.type || 'trecho')}</span><span class="ttl">${esc(s.title || '')}</span><span class="muted num" style="font-size:11.5px;white-space:nowrap">${(s.people || []).length ? (s.people.length + (s.people.length === 1 ? ' pessoa' : ' pessoas')) : ''}${unc ? ` · ${unc} incert.` : ''}</span></div>
            <div class="doc" data-full="${long ? 0 : 1}">${markText(long ? s.text.slice(0, 900) + '…' : s.text, s)}</div>${long ? '<button class="link more">Ler o trecho inteiro</button>' : ''}
            ${s.confidence != null && s.confidence < .6 ? `<div style="margin-top:8px"><span class="tag warn">confiança ${n(s.confidence * 100, 0)}%</span></div>` : ''}</div>`;
        }).join('');
      body.onclick = e => {
        const m = e.target.closest('.more'); if (!m) return;
        const c = m.closest('.segc'), s = segs[+c.dataset.i];
        $('.doc', c).innerHTML = markText(s.text, s); m.remove();
      };
      body.onmouseover = e => { const c = e.target.closest('.segc'); this.highlight(c ? segs[+c.dataset.i].tiles : null); };
      body.onmouseleave = () => this.highlight(null);
    } else if (this.tab === 'pessoas') {
      body.innerHTML = segs.map((s, i) => (s.people || []).length || (s.relations || []).length ? `<div class="segc" data-i="${i}"><div class="hd"><span class="n">${i + 1}</span><span class="tag type">${esc(SEGTYPE[s.type] || s.type)}</span><span class="ttl">${esc(s.title || '')}</span></div>
        <div class="people">${(s.people || []).map(p => `<span class="chip" title="${esc([p.role, p.context].filter(Boolean).join(' · '))}">${ic('user', 12)} ${esc(p.name)}${p.role ? ` <span class="muted">· ${esc(p.role)}</span>` : ''}</span>`).join('')}</div>
        ${(s.relations || []).length ? `<div style="margin-top:12px">${s.relations.map(r => `<div class="rel"><span>${esc(r.subject)}</span><span class="tag neutral">${esc(PRED[r.predicate] || r.predicate)}</span><span>${esc(r.object)}</span><span class="q">“${esc(r.quote)}”</span></div>`).join('')}</div>` : ''}
        ${[...(s.places || []), ...(s.ships || [])].length ? `<div class="people">${(s.places || []).map(p => `<span class="chip">${ic('pin', 12)} ${esc(p)}</span>`).join('')}${(s.ships || []).map(p => `<span class="chip">${ic('ship', 12)} ${esc(p)}</span>`).join('')}</div>` : ''}</div>` : '').join('') || '<div class="empty">Nenhuma pessoa identificada nesta página.</div>';
      body.onmouseover = e => { const c = e.target.closest('.segc'); this.highlight(c ? segs[+c.dataset.i].tiles : null); };
      body.onclick = null;
    } else {
      const rows = [['Arquivo de origem', `Hemeroteca Digital Brasileira · ${row.page_id}`], ['Periódico', x.newspaper || '—'], ['Data impressa', x.issue_date || '—'], ['Número', x.issue_number || '—'], ['Página', x.page_number || '—'], ['Colunas', x.columns || '—'],
        ['Modelo', modelName(meta.model || row.config)], ['Configuração', row.config], ['Backend', meta.backend === 'api' ? 'API' : 'assinatura (Claude Code)'], ['Esforço', meta.effort || '—'], ['Recortes', meta.grid ? `${meta.grid.join(' × ')} (${(meta.tile_boxes || []).length})` : '—'],
        ['Tokens de entrada', int(row.tokens_in)], ['Tokens de saída', int(row.tokens_out)], ['Raciocínio', int(row.thinking)], ['Tempo', dur(row.seconds)], ['Custo equivalente', usd(row.cost_usd) + ' · lote ' + usd(row.cost_batch)],
        ['Confiança média', row.confidence != null ? pct(row.confidence * 100, 0) : '—'], ['Tentativas', row.attempts], ['Lida em', when(row.finished_at)], ['Resultado', row.out_path || '—']];
      body.innerHTML = `<div style="padding:8px 20px 20px"><table>${rows.map(([k, v]) => `<tr><td class="muted" style="width:42%">${k}</td><td class="${/Resultado|Configuração|Arquivo/.test(k) ? 'mono' : 'num'}" style="word-break:break-all">${esc(v)}</td></tr>`).join('')}</table></div>`;
      body.onmouseover = null; body.onclick = null;
    }
  },
  highlight(tiles) {
    const set = new Set(tiles || []);
    $$('#pg-stage .tile').forEach(t => t.classList.toggle('hi', set.has(+t.dataset.t)));
    if (tiles && tiles.length) {
      const first = $(`#pg-stage .tile[data-t="${tiles[0]}"]`), scan = $('#pg-scan');
      if (first && scan) {
        const r = first.getBoundingClientRect(), sr = scan.getBoundingClientRect();
        if (r.top < sr.top || r.top > sr.bottom - 60) scan.scrollBy({top: r.top - sr.top - 40, behavior: 'smooth'});
      }
    }
  },
};
function titleCase(s) { return String(s).toLowerCase().replace(/(^|\s)(\p{L})/gu, (m, a, b) => a + b.toUpperCase()).replace(/\b(De|Da|Do|Dos|Das|E)\b/g, w => w.toLowerCase()); }
function niceDate(s) { return titleCase(s).replace(/^(Segunda|Terça|Terca|Quarta|Quinta|Sexta|Sabbado|Sábado|Domingo)[\s-]*(Feira)?\s*/i, ''); }
function modelName(m) {
  m = String(m || '');
  if (/opus-5-5|opus55/.test(m)) return 'Opus 5.5';
  if (/opus/.test(m)) return 'Opus';
  if (/sonnet/.test(m)) return 'Sonnet';
  if (/haiku/.test(m)) return 'Haiku';
  if (/ds|deepseek/.test(m)) return 'DeepSeek';
  return m;
}
function markText(text, seg) {
  let h = esc(text || '');
  // marcas de incerteza e ilegível
  h = h.replace(/\[(ileg[ií]vel[^\]]*)\]/gi, '<span class="ill">$1</span>').replace(/\[([^\[\]\n<]{1,30})\]/g, '<span class="unc">[$1]</span>');
  // nomes de pessoas, lugares e navios citados no trecho
  const names = (seg.people || []).map(p => [p.name, 'ent']).concat((seg.places || []).map(p => [p, 'ent place']), (seg.ships || []).map(p => [p, 'ent place']))
    .filter(([s]) => s && s.length > 2).sort((a, b) => b[0].length - a[0].length);
  const seen = new Set();
  for (const [name, cls] of names) {
    if (seen.has(name)) continue; seen.add(name);
    const e = esc(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    h = h.replace(new RegExp(`(^|[^\\p{L}>])(${e})(?![\\p{L}])(?![^<]*</span>)`, 'gu'), `$1<span class="${cls}">$2</span>`);
  }
  return h;
}

/* ---------- Mapa do acervo ---------- */
views.acervo = {
  nav: 'Mapa do acervo', crumbs: () => 'Acervo / <b>Mapa do acervo</b>',
  filter: 'todos',
  mount(p, q) {
    this.q = q.get('q') || '';
    return `<div class="content view-enter">
      <div><div class="eyebrow">Hemeroteca Digital Brasileira · Pernambuco</div><h1 class="h1" style="margin-top:8px">Mapa do acervo</h1>
        <p class="lede">Cada linha é um acervo da Hemeroteca (um periódico num intervalo de anos). Escolha um e programe a leitura de um trecho de páginas.</p></div>
      <div class="strip" id="ac-strip"></div>
      <div class="card"><div class="cardhead"><span class="t">Páginas por década</span><span class="s">os cinco maiores periódicos e o resto; vermelho = já transcrito</span></div><div id="ac-heat"></div></div>
      <div class="row" style="flex-wrap:wrap;gap:8px">
        <label class="search" style="width:340px;max-width:100%">${ic('search', 15)}<input id="ac-q" placeholder="Filtrar por periódico, ano ou código…" value="${esc(this.q)}"></label>
        <div class="row" id="ac-chips" style="gap:6px;flex-wrap:wrap">${[['todos', 'Todos'], ['xix', 'Século XIX'], ['lidos', 'Com transcrição'], ['nunca', 'Nunca lidos']].map(([k, l]) => `<span class="chip ${k === this.filter ? 'on' : ''}" data-f="${k}">${l}</span>`).join('')}</div>
      </div>
      <div class="card" style="padding:8px 18px"><div class="tbl-wrap"><table><thead><tr><th>Periódico</th><th>Período</th><th class="r">Páginas</th><th style="width:170px">Transcrito</th><th class="r" title="Semanas de cota da assinatura para ler tudo com Opus 5.5">Opus pela assinatura</th><th></th></tr></thead><tbody id="ac-rows"></tbody></table></div><div id="ac-more"></div></div>
    </div>`;
  },
  async bind() {
    await loadAcervos(true);
    $('#ac-q').addEventListener('input', e => { this.q = e.target.value; this.limit = 30; this.renderRows(); });
    $('#ac-chips').addEventListener('click', e => { const c = e.target.closest('.chip'); if (!c) return; this.filter = c.dataset.f; $$('#ac-chips .chip').forEach(x => x.classList.toggle('on', x === c)); this.limit = 30; this.renderRows(); });
    $('#ac-rows').addEventListener('click', e => { const b = e.target.closest('button[data-bib]'); if (b) openProgram(b.dataset.bib); });
    this.limit = 30;
    this.renderTop(); this.renderRows();
  },
  update() { if (ACERVOS && $('#ac-strip')) this.renderTop(); },
  renderTop() {
    const A = ACERVOS, tot = A.reduce((a, x) => a + (x.paginas || 0), 0), done = A.reduce((a, x) => a + (x.processadas || 0), 0);
    const dp = A.filter(a => a.bib.startsWith('029033')).reduce((a, x) => a + x.paginas, 0);
    const xix = A.reduce((a, x) => { const y = years(x.nome); return a + (y && y[0] < 1900 ? x.paginas * Math.max(0, Math.min(1, (Math.min(1899, y[1]) - y[0] + 1) / (y[1] - y[0] + 1))) : 0); }, 0);
    $('#ac-strip').innerHTML = [['Acervos', int(A.length)], ['Páginas no catálogo', int(tot)], ['Transcritas', int(done)], ['Diario de Pernambuco', int(dp) + ' págs'], ['Século XIX', '≈ ' + kilo(xix).replace(' mil', ' mil') + ' págs']]
      .map(([l, v]) => `<div><div class="l">${l}</div><div class="v num">${v}</div></div>`).join('');
    // mapa por década: distribui as páginas de cada acervo igualmente pelos anos do intervalo
    const groups = {};
    A.forEach(a => {
      const y = years(a.nome); if (!y) return;
      const g = journal(a.nome);
      groups[g] = groups[g] || {pages: 0, dec: {}, done: {}};
      groups[g].pages += a.paginas;
      const span = y[1] - y[0] + 1;
      for (let yr = y[0]; yr <= y[1]; yr++) {
        const d = Math.floor(yr / 10) * 10;
        groups[g].dec[d] = (groups[g].dec[d] || 0) + a.paginas / span;
        groups[g].done[d] = (groups[g].done[d] || 0) + (a.processadas || 0) / span;
      }
    });
    const ranked = Object.entries(groups).sort((a, b) => b[1].pages - a[1].pages);
    const top = ranked.slice(0, 5), rest = ranked.slice(5);
    const other = {pages: 0, dec: {}, done: {}};
    rest.forEach(([, g]) => { other.pages += g.pages; for (const d in g.dec) { other.dec[d] = (other.dec[d] || 0) + g.dec[d]; other.done[d] = (other.done[d] || 0) + g.done[d]; } });
    const rowsH = [...top, [`Outros ${rest.length} periódicos`, other]];
    const decs = []; for (let d = 1820; d <= 1980; d += 10) decs.push(d);
    const mx = Math.max(...rowsH.flatMap(([, g]) => Object.values(g.dec)));
    const shade = v => { const t = Math.pow(v / mx, .45); const c0 = [226, 218, 202], c1 = [110, 92, 70]; return `rgb(${c0.map((c, i) => Math.round(c + (c1[i] - c) * t)).join(',')})`; };
    $('#ac-heat').innerHTML = `<div style="overflow-x:auto"><div class="heat" style="grid-template-columns:190px repeat(${decs.length},minmax(34px,1fr)) 80px;min-width:820px">
      <span></span>${decs.map(d => `<span class="h">${d}</span>`).join('')}<span class="h" style="text-align:right">páginas</span>
      ${rowsH.map(([name, g]) => `<span class="nm" title="${esc(name)}">${esc(name)}</span>${decs.map(d => { const v = g.dec[d] || 0, dn = g.done[d] || 0; return v ? `<span class="hc" style="background:${shade(v)}" title="${esc(name)} · década de ${d}: ${int(v)} páginas${dn ? ', ' + int(dn) + ' transcritas' : ''}"><i style="height:${dn ? Math.max(3, dn / v * 100) : 0}%"></i><span>${short(v)}</span></span>` : '<span class="hc none"></span>'; }).join('')}<span class="num" style="text-align:right;font-size:12.5px">${int(g.pages)}</span>`).join('')}
    </div></div><div class="legend"><span>menos</span>${[.05, .2, .5, 1].map(f => `<i style="width:18px;background:${shade(mx * f)}"></i>`).join('')}<span>mais páginas</span><span><i style="background:var(--accent-2)"></i>já transcrito</span><span><i class="c-canc" style="width:14px"></i>sem acervo</span><span class="spacer"></span><span class="muted">Fonte: catálogo da Hemeroteca em data/cache/acervos_pe.json</span></div>`;
  },
  renderRows() {
    const q = this.q.trim().toLowerCase();
    const ppw = S?.pages_per_week || 1100;
    let A = ACERVOS.filter(a => !q || (a.nome + ' ' + a.bib + ' ' + (a.periodo_busca || '')).toLowerCase().includes(q));
    if (this.filter === 'xix') A = A.filter(a => { const y = years(a.nome); return y && y[0] < 1900; });
    if (this.filter === 'lidos') A = A.filter(a => a.processadas);
    if (this.filter === 'nunca') A = A.filter(a => !a.processadas);
    A = A.slice().sort((a, b) => (b.processadas || 0) - (a.processadas || 0) || b.paginas - a.paginas);
    const jobsByBib = {}; (S?.jobs || []).forEach(j => { if (j.status !== 'cancelado') (jobsByBib[j.bib] = jobsByBib[j.bib] || []).push(j); });
    $('#ac-rows').innerHTML = A.slice(0, this.limit).map(a => {
      const y = years(a.nome), p = a.paginas ? (a.processadas || 0) / a.paginas : 0, js = jobsByBib[a.bib] || [];
      return `<tr><td><div style="font-weight:500">${esc(journal(a.nome))}</div><div class="mono faint" style="font-size:11px">${esc(a.bib)}</div></td>
        <td class="num" style="white-space:nowrap">${y ? `${y[0]}–${y[1]}` : esc((a.periodo_busca || '—').replace(/ - /, '–'))}</td><td class="r num">${int(a.paginas)}</td>
        <td><div class="row"><div class="bar acc" style="flex:1"><i style="width:${Math.max(p ? 2 : 0, p * 100)}%"></i></div><span class="muted num" style="font-size:11.5px;width:44px;text-align:right">${pct(p * 100)}</span></div></td>
        <td class="r num">${n(a.paginas / ppw, a.paginas / ppw < 10 ? 1 : 0)} sem.</td>
        <td class="r" style="white-space:nowrap">${js.length ? `<a class="btn sm ghost" href="#/trabalho/${js[0].id}">Trabalho #${js[0].id}</a>` : ''}<button class="btn sm" data-bib="${esc(a.bib)}">Programar</button></td></tr>`;
    }).join('') || '<tr><td colspan="6"><div class="empty">Nenhum acervo com esse filtro.</div></td></tr>';
    $('#ac-more').innerHTML = A.length > this.limit ? `<div class="empty" style="padding:14px"><button class="btn sm" id="ac-more-b">Mostrar mais ${Math.min(60, A.length - this.limit)} de ${A.length - this.limit}</button></div>` : '';
    $('#ac-more-b')?.addEventListener('click', () => { this.limit += 60; this.renderRows(); });
  },
};

/* ---------- Programar leitura (gaveta) ---------- */
async function openProgram(bib) {
  const [A, C] = await Promise.all([loadAcervos(), loadConfigs()]);
  const a = A.find(x => x.bib === bib);
  if (!a) return;
  const done = S.jobs.filter(j => j.bib === bib && j.status !== 'cancelado').reduce((m, j) => Math.max(m, j.last_page), 0);
  const first = Math.min(a.paginas, done + 1), last = Math.min(a.paginas, first + 499);
  const PREF = ['opus55-full', 'dsflash-bands6', 'sonnet55-full', 'opus-full'];
  const rank = c => { const i = PREF.indexOf(c.name); return i < 0 ? 99 : i; };
  const cfgs = C.slice().sort((x, y) => rank(x) - rank(y));
  const y = years(a.nome);
  $('#layer').innerHTML = `<div class="drawer-bg" id="dr-bg"></div><form class="drawer" id="dr" autocomplete="off">
    <div class="dh"><div class="eyebrow">Programar leitura</div><div class="serif" style="font-size:24px;margin-top:6px;line-height:1.15">${esc(journal(a.nome))}${y ? `, ${y[0]}–${y[1]}` : ''}</div>
      <div class="mono muted" style="font-size:11.5px;margin-top:4px">${esc(a.bib)} · ${int(a.paginas)} páginas${a.processadas ? ` · ${int(a.processadas)} já lidas` : ''}</div></div>
    <div class="db">
      <div class="two"><div class="field"><label>Da página</label><input class="input num" name="first" type="number" min="1" max="${a.paginas}" value="${first}"></div>
        <div class="field"><label>Até a página</label><input class="input num" name="last" type="number" min="1" max="${a.paginas}" value="${last}"></div></div>
      <div class="row" style="gap:6px;flex-wrap:wrap" id="dr-quick">${[[10, '10 páginas (teste)'], [100, '100'], [500, '500'], [a.paginas, 'acervo inteiro']].map(([k, l]) => `<span class="chip" data-k="${k}">${l}</span>`).join('')}</div>
      <div class="field"><label>Método de leitura</label><select class="input" name="config">${cfgs.map(c => `<option value="${esc(c.name)}">${esc(c.name)} · ${esc(modelName(c.model))}${c.deepseek ? ' (API DeepSeek)' : ''}</option>`).join('')}</select></div>
      <div class="field" id="dr-backend"><label>Como pagar a leitura</label>
        <label class="opt on"><input type="radio" name="backend" value="claude-code" checked><div><div style="font-weight:500">Assinatura do Claude</div><div class="muted" style="font-size:12px">usa a cota semanal; o painel pausa sozinho no limite</div></div></label>
        <label class="opt"><input type="radio" name="backend" value="api"><div><div style="font-weight:500">API da Anthropic</div><div class="muted" style="font-size:12px">cobrado por token na conta da API</div></div></label></div>
      <div class="field"><label>Prioridade</label><select class="input" name="priority"><option value="0">Normal</option><option value="5">Alta: passa na frente da fila</option><option value="-5">Baixa: só quando o resto acabar</option></select></div>
      <div class="est" id="dr-est"></div>
    </div>
    <div class="df"><button type="button" class="btn ghost" id="dr-x">Cancelar</button><button class="btn acc">${ic('plus', 15)} Adicionar à fila</button></div></form>`;
  const f = $('#dr');
  const est = () => {
    const pages = Math.max(0, (+f.last.value || 0) - (+f.first.value || 0) + 1);
    const cfg = C.find(c => c.name === f.config.value) || {};
    const ds = cfg.deepseek, sub = !ds && f.backend.value === 'claude-code';
    $('#dr-backend').style.display = ds ? 'none' : '';
    $$('#dr-backend .opt').forEach(o => o.classList.toggle('on', o.querySelector('input').checked));
    const t = S.totals, u = S.usage;
    const avgOut = t.avg_tokens_out || 17000, avgS = t.avg_seconds || 150;
    const pctUse = pages * avgOut / (u.per_pct || 186000);
    const perPage = ds ? 0.039 : (t.avg_cost || 0.42);
    const lines = [['Páginas', int(pages)]];
    if (sub) {
      lines.push(['Cota da semana', `≈ ${n(pctUse, pctUse < 10 ? 1 : 0)}%`]);
      lines.push(['Semanas de cota', `≈ ${n(pages / (S.pages_per_week || 1100), 1)}`]);
    } else lines.push(['Custo estimado', `≈ ${usd(pages * perPage, 0)}`]);
    lines.push([`Tempo com ${S.settings.workers} ${S.settings.workers === 1 ? 'leitor' : 'leitores'}`, `≈ ${dur(pages * avgS / Math.max(1, S.settings.workers))}`]);
    $('#dr-est').innerHTML = `<div style="font-weight:600;color:var(--accent)">Estimativa</div>${lines.map(([k, v]) => `<div class="row"><span>${k}</span><b class="num">${v}</b></div>`).join('')}<div class="muted" style="font-size:11.5px">Pela média medida até agora: ${kilo(avgOut)} tokens e ${dur(avgS)} por página.</div>`;
  };
  f.addEventListener('input', est); f.addEventListener('change', est); est();
  $('#dr-quick').onclick = e => { const c = e.target.closest('.chip'); if (!c) return; f.last.value = Math.min(a.paginas, (+f.first.value || 1) + +c.dataset.k - 1); est(); };
  $('#dr-x').onclick = closeDrawer; $('#dr-bg').onclick = closeDrawer;
  f.onsubmit = async e => {
    e.preventDefault();
    const body = {bib, config: f.config.value, backend: f.backend.value, first_page: +f.first.value, last_page: +f.last.value, priority: +f.priority.value};
    const r = await act(() => api('/api/jobs', body), `Trabalho criado: ${int(body.last_page - body.first_page + 1)} páginas na fila`);
    if (r && r.id) { closeDrawer(); location.hash = '#/trabalho/' + r.id; if (S.settings.paused) toast('O processamento está pausado. Aperte Continuar no topo para começar.'); }
  };
  f.first.focus();
}
function closeDrawer() { $('#layer').innerHTML = ''; }

/* ---------- Revisão ---------- */
views.review = {
  nav: 'Revisão e qualidade', crumbs: () => 'Acervo / <b>Revisão e qualidade</b>',
  mount() {
    return `<div class="content view-enter"><div><div class="eyebrow">Qualidade</div><h1 class="h1" style="margin-top:8px">Revisão</h1>
      <p class="lede">Páginas com muitas marcas de leitura incerta, ou que você marcou, esperam um olhar humano. Aprovar tira a página da fila.</p></div>
      <div class="kpis" id="rv-kpis" style="grid-template-columns:repeat(4,minmax(0,1fr))"></div>
      <div class="card"><div class="cardhead"><span class="t">Para revisar</span><span class="s" id="rv-sub"></span></div><div class="tbl-wrap"><table><thead><tr><th>Página</th><th>Periódico</th><th class="r">Incertezas</th><th class="r">Confiança</th><th class="r">Pessoas</th><th>Motivo</th><th class="r">Lida</th><th></th></tr></thead><tbody id="rv-rows"></tbody></table></div></div></div>`;
  },
  async bind() { this.load(); },
  async load() { this.rows = await api('/api/review'); this._at = Date.now(); this.update(); },
  update() {
    if (Date.now() - (this._at || 0) > 8000) { this.load(); return; }
    const q = S.quality;
    $('#rv-kpis').innerHTML = [['Páginas lidas', int(q.lidas), ''], ['Para revisar', int(q.revisar), `${S.settings.revisar_marcas}+ marcas ou marcadas`], ['Aprovadas', int(q.aprovadas), q.lidas ? pct(q.aprovadas / q.lidas * 100) + ' das lidas' : ''], ['Confiança média', q.confidence ? pct(q.confidence * 100, 0) : '—', `${int(q.uncertain)} marcas no total`]]
      .map(([l, v, d]) => `<div class="card"><div class="kpi"><span class="l">${l}</span><span class="v num">${v}</span><span class="d">${d}</span></div></div>`).join('');
    const rows = this.rows || [];
    $('#rv-sub').textContent = rows.length ? `${rows.length} páginas, mais incertas primeiro` : '';
    $('#rv-rows').innerHTML = rows.length ? rows.map(r => `<tr class="click" onclick="location.hash='#/pagina/${r.job_id}/${r.page_id}?fila=1'"><td class="mono">${esc(r.page_id)}</td><td>${esc(journal(r.nome))}</td>
      <td class="r num">${r.uncertain ?? 0}</td><td class="r num">${r.confidence != null ? pct(r.confidence * 100, 0) : '—'}</td><td class="r num">${r.people ?? '—'}</td>
      <td>${r.review === 'revisar' ? '<span class="tag warn">marcada por você</span>' : '<span class="tag neutral">leitura incerta</span>'}</td><td class="r num">${when(r.finished_at)}</td><td class="r"><span class="link">Revisar ${ic('chev', 13)}</span></td></tr>`).join('')
      : `<tr><td colspan="8"><div class="empty"><div class="h2">Nada para revisar</div>Quando uma página tiver ${S.settings.revisar_marcas} ou mais marcas de incerteza, ela aparece aqui.</div></td></tr>`;
  },
};

/* ---------- Cota e custos ---------- */
const PLAN = [
  {k: 'sub', name: 'Claude Opus 5.5 · assinatura', sub: 'usa a cota semanal que sobra, sem custo extra', cer: '0,8%'},
  {k: 'api', name: 'Claude Opus 5.5 · API em lote', sub: 'cobrado por token, metade do preço normal', cer: '0,8%'},
  {k: 'ds', name: 'DeepSeek flash · 6 faixas', sub: 'API DeepSeek, preço fora do horário de pico', cer: '13%'},
  {k: 'mix', name: 'Misto: DeepSeek em tudo e Opus onde mais se consulta', sub: 'Opus revisa pela assinatura o que for prioridade', cer: '0,8% onde importa'},
];
views.cota = {
  nav: 'Cota e custos', crumbs: () => 'Recursos / <b>Cota e custos</b>',
  scope: 'dp1899', pick: 'mix',
  mount() {
    return `<div class="content view-enter"><div><div class="eyebrow">Assinatura Claude · API · DeepSeek</div><h1 class="h1" style="margin-top:8px">Quanto custa ler o acervo</h1>
      <p class="lede">O Claude não informa a cota da assinatura por API. O painel estima pelos tokens de saída e se corrige cada vez que você registra a % do painel de Usage.</p></div>
      <div class="cota-grid">
        <div class="card"><div class="cardhead"><span class="t">Calibração</span><span class="s">quanto vale 1% da semana</span></div>
          <div class="mini" id="ct-mini"></div>
          <div class="tbl-wrap" style="margin-top:14px"><table><thead><tr><th>Registro</th><th class="r">Usage</th><th class="r">Tokens desde o anterior</th><th class="r">Leitura</th></tr></thead><tbody id="ct-marks"></tbody></table></div>
          <form class="row" id="ct-form" style="margin-top:14px"><label class="input" style="flex:1"><input name="pct" inputmode="decimal" placeholder="Quanto o Usage mostra agora? ex. 74" style="border:0;outline:0;background:none;font:inherit;flex:1"> %</label><button class="btn pri">Registrar</button></form>
        </div>
        <div class="card"><div class="cardhead"><span class="t">Gasto medido</span><span class="s">tudo o que o painel já leu</span></div>
          <div class="mini" id="ct-spent" style="grid-template-columns:repeat(2,1fr)"></div>
          <div class="banner" style="margin-top:14px">${ic('alert', 16)}<span>A pausa automática usa a cota estimada. Se você usa o Claude para outras coisas na semana, registre o Usage com frequência para a estimativa não ficar para trás.</span></div>
        </div>
      </div>
      <div class="card"><div class="cardhead" style="flex-wrap:wrap"><div><div class="t">Planejar o restante</div><div class="s" style="margin-top:3px">Erro (CER) e preços do benchmark de 31 out em 3 páginas de referência; tokens e tempo pela média medida aqui.</div></div><span class="spacer"></span>
        <div class="row" id="ct-scope" style="gap:6px;flex-wrap:wrap"></div></div>
        <div class="tbl-wrap"><table><thead><tr><th>Estratégia</th><th class="r">Erro (CER)</th><th class="r">Custo</th><th class="r">Prazo</th></tr></thead><tbody id="ct-plan"></tbody></table></div></div>
    </div>`;
  },
  bind() {
    $('#ct-form').addEventListener('submit', e => {
      e.preventDefault();
      const v = parseFloat(e.target.pct.value.replace(',', '.'));
      if (isNaN(v)) return toast('Digite a porcentagem que o Usage mostra', 'err');
      act(() => api('/api/usage', {pct: v}), `Registrado: ${n(v, 0)}% da semana`).then(() => { e.target.pct.value = ''; });
    });
    $('#ct-scope').addEventListener('click', e => { const c = e.target.closest('.chip'); if (!c) return; this.scope = c.dataset.s; this.update(); });
    $('#ct-plan').addEventListener('click', e => { const tr = e.target.closest('tr[data-k]'); if (!tr) return; this.pick = tr.dataset.k; this.update(); });
    loadAcervos().then(() => this.update());
  },
  update() {
    const {t, u} = derived();
    const avgOut = t.avg_tokens_out || 17000;
    $('#ct-mini').innerHTML = [['1% da semana', kilo(u.per_pct), u.calibrated ? 'tokens de saída, medido' : 'tokens de saída, padrão'], ['ou seja', '≈ ' + n(u.per_pct / avgOut, 0) + ' págs', t.avg_tokens_out ? `com ${kilo(avgOut)} tokens/pág` : 'página densa do Opus'], ['semana inteira', '≈ ' + int(S.pages_per_week || u.per_pct * 100 / avgOut), 'páginas']]
      .map(([l, v, d]) => `<div><div class="l">${l}</div><div class="v num">${v}</div><div class="l">${d}</div></div>`).join('');
    const marks = S.usage_marks;
    $('#ct-marks').innerHTML = marks.length ? marks.slice(0, 8).map((m, i) => {
      const older = marks[i + 1];
      const dt = older ? m.tokens_out_total - older.tokens_out_total : null, dp = older ? m.pct - older.pct : null;
      const read = !older ? '<span class="tag neutral">ponto de partida</span>' : dp >= 1 && dt > 0 ? `<span class="tag ok">+${n(dp, 0)}% = ${kilo(dt / dp)}/1%</span>` : dp < 0 ? '<span class="tag neutral">semana renovou</span>' : '<span class="tag neutral">sem variação</span>';
      return `<tr><td class="num">${when(m.ts)}</td><td class="r num">${n(m.pct, 0)}%</td><td class="r num">${dt == null ? '—' : int(dt)}</td><td class="r">${read}</td></tr>`;
    }).join('') : '<tr><td colspan="4"><div class="empty" style="padding:14px">Nenhum registro ainda. Abra claude.ai/settings/usage e digite a % da semana.</div></td></tr>';
    $('#ct-spent').innerHTML = [['Valor equivalente na API', usd(t.cost_usd, 0), `${int(t.ok)} páginas lidas`], ['Se fosse API em lote', usd(t.cost_batch, 0), 'metade do preço'], ['Por página (API)', usd(t.avg_cost), 'média medida'], ['Tokens de saída', kilo(t.tokens_out), `${kilo(t.tokens_in)} de entrada`]]
      .map(([l, v, d]) => `<div><div class="l">${l}</div><div class="v num">${v}</div><div class="l">${d}</div></div>`).join('');

    // escopos
    const cur = derived().current;
    const A = ACERVOS || [];
    const dp04 = A.find(a => a.bib === '029033_04');
    const scopes = [];
    if (cur) scopes.push(['job', `Este trabalho · ${int(cur.total - cur.ok)}`, cur.total - cur.ok]);
    if (dp04) scopes.push(['dp04', `Diario 1860–69 · ${int(dp04.paginas)}`, dp04.paginas]);
    scopes.push(['dp1899', 'Diario até 1899 · 122.467', 122467]);
    scopes.push(['all', `Todo o acervo PE · ${int(S.acervo_total || 1125402)}`, S.acervo_total || 1125402]);
    if (!scopes.find(s => s[0] === this.scope)) this.scope = scopes[0][0];
    $('#ct-scope').innerHTML = scopes.map(([k, l]) => `<span class="chip ${k === this.scope ? 'on' : ''}" data-s="${k}">${l}</span>`).join('');
    const pages = scopes.find(s => s[0] === this.scope)[2];
    const ppw = S.pages_per_week || 1100;
    const perH = 8 * 3600 / (t.avg_seconds || 150);
    const batch = t.cost_batch && t.ok ? t.cost_batch / t.ok : 0.21;
    const cost = {sub: 'US$ 0 extra', api: '≈ ' + usd(pages * batch, 0), ds: '≈ ' + usd(pages * 0.019, 0), mix: '≈ ' + usd(pages * 0.019, 0)};
    const prazo = {sub: '≈ ' + dur(pages / ppw * 7 * 86400), api: `≈ ${dur(pages / perH * 3600)} com 8 leitores`, ds: 'depende da API DeepSeek', mix: 'DeepSeek + Opus contínuo'};
    $('#ct-plan').innerHTML = PLAN.map(p => `<tr class="click ${p.k === this.pick ? 'pick' : ''}" data-k="${p.k}"><td><div class="row" style="gap:12px"><span class="radio"></span><div><div style="font-weight:500">${p.name}${p.k === 'mix' ? ' <span class="tag acc">recomendado</span>' : ''}</div><div class="muted" style="font-size:12px">${p.sub}</div></div></div></td>
      <td class="r num">${p.cer}</td><td class="r num" style="font-weight:600">${cost[p.k]}</td><td class="r num">${prazo[p.k]}</td></tr>`).join('');
  },
};

/* ---------- Ajustes ---------- */
views.settings = {
  nav: 'Ajustes', crumbs: () => 'Recursos / <b>Ajustes</b>',
  mount() {
    const s = S.settings;
    const f = (k, l, hint, attrs) => `<div class="field"><label for="st-${k}">${l}</label><input class="input num" id="st-${k}" name="${k}" ${attrs} value="${esc(s[k])}"><span class="hint">${hint}</span></div>`;
    return `<div class="content view-enter" style="max-width:980px"><div><div class="eyebrow">Recursos</div><h1 class="h1" style="margin-top:8px">Ajustes do processador</h1>
      <p class="lede">Valem na hora, sem reiniciar. O painel roda só neste computador (127.0.0.1).</p></div>
      <form class="card" id="st-form"><div class="set-grid">
        ${f('workers', 'Leitores em paralelo', 'De 1 a 8. Na assinatura, mais leitores gastam a cota mais rápido, não mais barato.', 'type="number" min="1" max="8"')}
        ${f('limite_pct', 'Pausar quando a cota estimada chegar a (%)', 'Deixe folga para o seu uso normal do Claude.', 'type="number" min="1" max="100" step="1"')}
        ${f('prefetch', 'Imagens baixadas à frente', 'Quantas páginas ficam prontas esperando um leitor.', 'type="number" min="1" max="50"')}
        ${f('max_tentativas', 'Tentativas por página', 'Depois disso a página fica como erro.', 'type="number" min="1" max="5"')}
        ${f('revisar_marcas', 'Mandar para revisão com', 'marcas [..] de leitura incerta na página.', 'type="number" min="1" max="100"')}
        ${f('tokens_por_pct', 'Tokens de saída por 1% (padrão)', 'Usado até existirem dois registros do Usage para calibrar.', 'type="number" min="1000" step="1000"')}
      </div>
      <div class="divider" style="margin:20px 0"></div>
      <label class="check"><input type="checkbox" name="apagar_imagens" ${s.apagar_imagens ? 'checked' : ''}><div><div style="font-weight:500">Apagar a imagem depois da leitura</div><div class="muted" style="font-size:12px">Só imagens que o painel baixou. Economiza disco, mas o inspetor deixa de mostrar o scan.</div></div></label>
      <div class="row" style="margin-top:20px;justify-content:flex-end"><button class="btn pri">Salvar ajustes</button></div></form></div>`;
  },
  bind() {
    $('#st-form').addEventListener('submit', e => {
      e.preventDefault();
      const fd = new FormData(e.target), body = {};
      ['workers', 'limite_pct', 'prefetch', 'max_tentativas', 'revisar_marcas', 'tokens_por_pct'].forEach(k => body[k] = Number(fd.get(k)));
      body.apagar_imagens = !!fd.get('apagar_imagens');
      act(() => api('/api/settings', body), 'Ajustes salvos');
    });
  },
  update() {},
};

// ---------------------------------------------------------------- roteador
const ROUTES = [
  [/^\/?$/, 'overview'], [/^\/trabalhos$/, 'jobs'], [/^\/trabalho\/(\d+)$/, 'job'], [/^\/pagina$/, 'page'], [/^\/pagina\/(\d+)\/([0-9A-Za-z_]+)$/, 'page'],
  [/^\/acervo$/, 'acervo'], [/^\/revisao$/, 'review'], [/^\/cota$/, 'cota'], [/^\/ajustes$/, 'settings'],
];
function route() {
  if (!S) return;
  closeDrawer();
  const raw = location.hash.replace(/^#/, '') || '/';
  const [path, qs] = raw.split('?');
  let name = 'overview', params = [];
  for (const [re, v] of ROUTES) { const m = path.match(re); if (m) { name = v; params = m.slice(1); break; } }
  const view = views[name];
  V = view; V.name = name;
  currentNav = view.nav;
  document.title = (view.nav === 'Visão geral' ? '' : view.nav + ' · ') + 'Scriptorium';
  $('#top').innerHTML = topHTML(typeof view.crumbs === 'function' ? view.crumbs(params) : view.crumbs);
  chrome();
  $('#view').innerHTML = view.mount(params, new URLSearchParams(qs || ''));
  window.scrollTo(0, 0);
  view.bind?.($('#view'));
  try { view.update(); } catch (e) { console.error(e); }
}
function onState() {
  if (!V) { route(); return; }
  chrome();
  try { V.update(); } catch (e) { console.error(e); }
}
window.addEventListener('hashchange', route);
refreshState().then(connect);
})();
