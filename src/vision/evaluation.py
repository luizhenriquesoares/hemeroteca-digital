"""Offline excerpt checks against visually reviewed references (no model calls).

A best matching span is aligned inside the full transcription. Scores concern
only those excerpts, not page completeness, layout, entities or global quality.
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text)).strip()


def edit_distance(expected, actual):
    row = list(range(len(actual) + 1))
    for i, char in enumerate(expected, 1):
        nxt = [i]
        for j, other in enumerate(actual, 1):
            nxt.append(min(nxt[-1] + 1, row[j] + 1, row[j-1] + (char != other)))
        row = nxt
    return row[-1]


def best_span(expected: str, actual: str):
    """Semiglobal edit alignment, free prefix/suffix only on the actual text."""
    if not expected:
        raise ValueError("Referência vazia")
    row = [0] * (len(actual) + 1)
    starts = list(range(len(actual) + 1))
    for i, char in enumerate(expected, 1):
        nxt, ns = [i], [0]
        for j, other in enumerate(actual, 1):
            candidates = [(row[j-1] + (char != other), starts[j-1]),
                          (row[j] + 1, starts[j]), (nxt[-1] + 1, ns[-1])]
            cost, start = min(candidates, key=lambda p: p[0])
            nxt.append(cost)
            ns.append(start)
        row, starts = nxt, ns
    end = min(range(len(row)), key=lambda j: row[j])
    return row[end], starts[end], end


def evaluate_result(reference: dict, result: dict) -> dict:
    label = {"page": result.get("page"), "config": result.get("config"), "grid": result.get("grid"),
             "tiling": result.get("tiling", "grid"), "reference_status": reference["status"]}
    if result.get("error"):
        return {**label, "skipped": "Falha na leitura: " + str(result["error"])}
    if result.get("mode") != "full":
        return {**label, "skipped": "Referência de transcrição integral: modo structure não comparável"}
    excerpts = [s for s in reference["excerpts"] if s["page"] == result.get("page")]
    if not excerpts:
        return {**label, "skipped": "Página sem trechos de referência"}
    actual = normalize("\n\n".join(s.get("text", "") for s in (result.get("extraction") or {}).get("segments", [])))
    checks = []
    for excerpt in excerpts:
        expected = normalize(excerpt["text"])
        edits, start, end = best_span(expected, actual)
        found = actual[start:end]
        word_edits = edit_distance(expected.split(), found.split())
        checks.append({"id": excerpt["id"], "expected": expected, "matched": found,
                       "char_edits": edits, "reference_chars": len(expected), "cer": edits/len(expected),
                       "word_edits": word_edits, "reference_words": len(expected.split()),
                       "wer": word_edits/len(expected.split()), "exact_occurrences": actual.count(expected)})
    return {**label, "checks": checks,
            "excerpt_cer": sum(c["char_edits"] for c in checks)/sum(c["reference_chars"] for c in checks),
            "excerpt_wer": sum(c["word_edits"] for c in checks)/sum(c["reference_words"] for c in checks)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True, type=Path)
    parser.add_argument('--result', required=True, action='append', type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    reference = json.loads(args.reference.read_text())
    results = [{"source": str(p), **evaluate_result(reference, json.loads(p.read_text()))} for p in args.result]
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out/'scores.json').write_text(json.dumps(results, ensure_ascii=False, indent=2))
    lines = ['# Avaliação provisória de trechos', '',
             'Referência transcrita visualmente pelo assistente; revisão humana pendente. Menor erro é melhor.',
             'NFC e espaços normalizados; caixa, acentos e grafia preservados. CER = erros por caractere; WER = erros por palavra.',
             'Alinhamento do melhor trecho: estes números NÃO medem qualidade global, completude ou precisão de entidades.', '',
             '| Configuração | Grade | CER dos trechos | WER dos trechos |', '|---|---|---:|---:|']
    for r in results:
        if 'skipped' in r:
            continue
        lines.append(f"| {r['config']} | {r['grid']} | {r['excerpt_cer']:.1%} | {r['excerpt_wer']:.1%} |")
    (args.out/'scores.md').write_text('\n'.join(lines)+'\n')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
