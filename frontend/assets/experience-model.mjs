// Regras compartilhadas pelo cinema e pelo caderno. Não alteram o corpus.
export function documentKey(item) {
    const meta = item.metadata || item;
    const bib = String(item.bib || meta.bib || '');
    const raw = String(item.pagina || meta.pagina || '');
    const last = raw.match(/(?:^|_)(\d+)$/)?.[1];
    return bib && raw ? `${bib}:${last ? Number(last) : raw}` : '';
}
export function normalizeDocument(item) {
    const meta = item.metadata || item;
    const clean = value => value && value !== '?' ? String(value) : '';
    return { key: documentKey(item), bib: String(item.bib || meta.bib || ''),
        pagina: String(item.pagina || meta.pagina || ''),
        jornal: clean(item.jornal || meta.jornal || meta.periodico),
        ano: clean(item.ano || meta.ano), edicao: clean(item.edicao || meta.edicao),
        quote: String(item.quote || item.evidencia || item.snippet || item.source_snippet || item.texto || '').slice(0, 1800),
        note: typeof item.note === 'string' ? item.note.slice(0, 10000) : '' };
}
export function chronologicalDocuments(items) {
    const seen = new Set();
    return items.map(normalizeDocument).filter(item => {
        if (!item.key || seen.has(item.key)) return false;
        seen.add(item.key); return true;
    }).sort((a, b) => {
        const year = item => Number(item.ano.match(/\b(1\d{3}|20\d{2})\b/)?.[1] || Infinity);
        return year(a) - year(b) || a.bib.localeCompare(b.bib) || a.pagina.localeCompare(b.pagina, 'pt-BR', {numeric:true});
    });
}
export function notebookMarkdown(items, origin) {
    const quoteLines = value => String(value).split('\n').map(line => '> ' + line).join('\n');
    return '# Caderno de investigação · Hemeroteca\n\nFontes reunidas pelo pesquisador. Transcrições automáticas exigem conferência no original; notas são hipóteses pessoais.\n\n' + items.map((item, i) => {
        const doc = normalizeDocument(item);
        const url = new URL(`/page/${encodeURIComponent(doc.bib)}/${encodeURIComponent(doc.pagina)}`, origin).href;
        return `## Fonte ${i + 1}\n\nJornal: ${doc.jornal || 'Não identificado'}\n\nAno: ${doc.ano || 'Não informado'} · Página: ${doc.pagina} · Acervo: ${doc.bib}\n\nFonte local: ${url}\n\n### Trecho da transcrição\n\n${quoteLines(doc.quote || 'Sem trecho salvo.')}\n\n### Minhas notas\n\n${quoteLines(doc.note || 'Sem notas.')}\n`;
    }).join('\n---\n\n');
}
