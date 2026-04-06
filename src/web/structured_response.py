"""Parsing e fallback das respostas estruturadas do RAG."""

from __future__ import annotations

import re

from src.processing.search import normalize_text, tokenize_significant


def extract_section(answer: str, title: str, next_titles: list[str]) -> str:
    pattern = rf"(?:^|\n)\s*(?:#+\s*)?(?:\d+\.\s*)?{re.escape(title)}\s*:?\s*\n"
    match = re.search(pattern, answer, flags=re.IGNORECASE)
    if not match:
        return ""

    start = match.end()
    end = len(answer)
    for next_title in next_titles:
        next_pattern = rf"(?:^|\n)\s*(?:#+\s*)?(?:\d+\.\s*)?{re.escape(next_title)}\s*:?\s*\n"
        next_match = re.search(next_pattern, answer[start:], flags=re.IGNORECASE)
        if next_match:
            end = start + next_match.start()
            break
    return answer[start:end].strip()


def bulletize(section: str) -> list[str]:
    lines = []
    for raw in section.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(("-", "*", "•")):
            line = line[1:].strip()
        lines.append(line)
    return lines


def extract_person_mentions(query: str, fontes: list[dict]) -> list[str]:
    surname_tokens = tokenize_significant(query)
    surname_focus = surname_tokens[-1] if surname_tokens else ""
    results = []
    seen = set()

    name_pattern = re.compile(
        r"\b(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)"
        r"(?:\s+(?:(?:de|da|do|das|dos|d['’])\s+)?(?:[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+|d['’][A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)){1,5}\b"
    )

    for fonte in fontes:
        text = fonte.get("evidencia") or ""
        for match in name_pattern.findall(text):
            normalized = normalize_text(match)
            if surname_focus and surname_focus not in normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            results.append(f"{match} ({fonte.get('jornal', '?')}, p. {fonte.get('pagina', '?')})")

    return results[:10]


def parse_structured_answer(answer: str, fontes: list[dict]) -> dict:
    sections = {
        "resumo": extract_section(
            answer,
            "Resumo interpretativo",
            ["Pessoas e entidades citadas", "Evidências dos jornais", "Fontes"],
        ),
        "pessoas": bulletize(
            extract_section(answer, "Pessoas e entidades citadas", ["Evidências dos jornais", "Fontes"])
        ),
        "evidencias": bulletize(extract_section(answer, "Evidências dos jornais", ["Fontes"])),
        "fontes": [],
    }

    seen = set()
    for fonte in fontes:
        key = (fonte.get("bib"), fonte.get("pagina"))
        if key in seen:
            continue
        seen.add(key)
        sections["fontes"].append(
            {
                "jornal": fonte.get("jornal", "?"),
                "bib": fonte.get("bib", "?"),
                "pagina": fonte.get("pagina", "?"),
                "ano": fonte.get("ano", "?"),
                "edicao": fonte.get("edicao", "?"),
                "evidencia": fonte.get("evidencia", ""),
            }
        )

    return sections


def build_prosopographic_fallback(query: str, fontes: list[dict]) -> dict:
    people = extract_person_mentions(query, fontes)
    evidence = []
    for fonte in fontes[:8]:
        snippet = fonte.get("evidencia", "").strip()
        if snippet:
            evidence.append(
                f"{snippet} ({fonte.get('jornal', '?')}, {fonte.get('bib', '?')}, p. {fonte.get('pagina', '?')})"
            )
    return {"pessoas": people, "evidencias": evidence}
