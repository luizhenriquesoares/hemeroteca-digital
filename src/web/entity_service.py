"""Serviços de entidades e páginas para a API web."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from urllib.parse import quote


def _is_generic_journal_name(value: str | None, bib: str) -> bool:
    if not value:
        return True
    normalized = value.strip().lower()
    return normalized in {"?", "", f"acervo {bib.lower()}"}


def _load_acervo_cache(text_dir) -> dict[str, dict]:
    cache_file = text_dir.parent / "cache" / "acervos_pe.json"
    if not cache_file.exists():
        return {}
    try:
        acervos = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {
        item["bib"]: item
        for item in acervos
        if isinstance(item, dict) and item.get("bib")
    }


def _build_mentions_summary(mentions: list[dict]) -> dict:
    mentions_by_journal = Counter()
    mentions_by_year = Counter()

    for item in mentions:
        jornal = item.get("jornal") or "?"
        mentions_by_journal[jornal] += 1
        ano = str(item.get("ano") or "").strip()
        if ano and ano != "?":
            mentions_by_year[ano] += 1

    timeline = [
        {"year": year, "mentions": mentions}
        for year, mentions in sorted(mentions_by_year.items(), key=lambda entry: entry[0])
    ]
    journals = [
        {"jornal": jornal, "mentions": mentions}
        for jornal, mentions in mentions_by_journal.most_common(6)
    ]
    return {"timeline": timeline, "journals": journals}


def _build_relations_summary(entity: dict) -> dict:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for relation in entity.get("relations", []):
        grouped[relation.get("predicate") or "?"].append(relation)

    grouped_relations = []
    for predicate, items in sorted(grouped.items()):
        grouped_relations.append(
            {
                "predicate": predicate,
                "count": len(items),
                "confirmed_like": sum(1 for item in items if item.get("status") in {"confirmed", "probable"}),
            }
        )
    return {"grouped_relations": grouped_relations}


def _build_entity_summary(entity: dict) -> dict:
    mentions_summary = _build_mentions_summary(entity.get("mentions", []))
    relation_summary = _build_relations_summary(entity)
    first_year = entity.get("first_seen_year") or ""
    last_year = entity.get("last_seen_year") or ""
    if not first_year or first_year == "?":
        timeline = mentions_summary["timeline"]
        if timeline:
            first_year = timeline[0]["year"]
            last_year = timeline[-1]["year"]
    return {
        "first_year": first_year or "?",
        "last_year": last_year or "?",
        **mentions_summary,
        **relation_summary,
    }


def serialize_entity(entity: dict, resolve_image_url_fn) -> dict:
    entity["aliases"] = json.loads(entity.get("aliases_json", "[]"))
    entity["attributes"] = json.loads(entity.get("attributes_json", "{}"))
    for collection_name in ("mentions", "relations", "evidences"):
        for item in entity.get(collection_name, []):
            if item.get("bib") and item.get("pagina"):
                highlight = item.get("surface_form") or item.get("quote") or entity.get("canonical_name") or ""
                item["page_api_url"] = f"/api/page/{item['bib']}/{item['pagina']}"
                item["page_view_url"] = f"/page/{item['bib']}/{item['pagina']}"
                if highlight:
                    item["page_view_url"] += f"?q={quote(str(highlight))}"
                item["image_url"] = resolve_image_url_fn(item["bib"], item["pagina"])
    story = entity.get("story") or {}
    for item in story.get("milestones", []):
        if item.get("bib") and item.get("pagina"):
            highlight = item.get("surface_form") or item.get("snippet") or entity.get("canonical_name") or ""
            item["page_api_url"] = f"/api/page/{item['bib']}/{item['pagina']}"
            item["page_view_url"] = f"/page/{item['bib']}/{item['pagina']}"
            if highlight:
                item["page_view_url"] += f"?q={quote(str(highlight))}"
            item["image_url"] = resolve_image_url_fn(item["bib"], item["pagina"])
    entity["summary"] = _build_entity_summary(entity)
    return entity


def serialize_entity_search_results(results: list[dict]) -> list[dict]:
    for row in results:
        row["aliases"] = json.loads(row.get("aliases_json", "[]"))
    return results


def build_fallback_page(*, bib: str, pagina: str, text_dir, images_dir):
    # Tentar múltiplos formatos de nome de arquivo
    candidates = [
        pagina,                                    # "029033_02_00001"
        f"{bib}_{int(pagina):05d}" if pagina.isdigit() else None,  # "1" -> "029033_02_00001"
    ]
    candidates = [c for c in candidates if c]

    text_path = None
    json_path = None
    corrigido_path = None
    for c in candidates:
        tp = text_dir / bib / f"{c}.txt"
        jp = text_dir / bib / f"{c}.json"
        cp = text_dir / bib / f"{c}_corrigido.txt"
        if tp.exists() or jp.exists():
            text_path = tp
            json_path = jp
            corrigido_path = cp
            break

    if not text_path and not json_path:
        return None

    metadata = {"bib": bib, "pagina": pagina, "jornal": "?", "ano": "?", "edicao": "?"}
    if json_path and json_path.exists():
        metadata.update(json.loads(json_path.read_text(encoding="utf-8")))

    acervo = _load_acervo_cache(text_dir).get(bib, {})
    jornal = metadata.get("jornal") or metadata.get("periodico") or "?"
    if _is_generic_journal_name(jornal, bib):
        jornal = acervo.get("nome") or jornal

    image_path = images_dir / bib / f"{pagina}.jpg"

    # Preferir texto corrigido
    best_text_path = corrigido_path if corrigido_path and corrigido_path.exists() else text_path

    return {
        "bib": bib,
        "pagina": pagina,
        "jornal": jornal,
        "ano": str(metadata.get("ano", "?")).replace("Ano ", "", 1),
        "edicao": str(metadata.get("edicao", "?")).replace("Edição ", "", 1),
        "text_path": str(best_text_path) if best_text_path and best_text_path.exists() else None,
        "image_path": str(image_path) if image_path.exists() else None,
    }
