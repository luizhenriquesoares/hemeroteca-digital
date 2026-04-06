"""Serviços de entidades e páginas para a API web."""

from __future__ import annotations

import json


def serialize_entity(entity: dict, resolve_image_url_fn) -> dict:
    entity["aliases"] = json.loads(entity.get("aliases_json", "[]"))
    entity["attributes"] = json.loads(entity.get("attributes_json", "{}"))
    for collection_name in ("mentions", "relations", "evidences"):
        for item in entity.get(collection_name, []):
            if item.get("bib") and item.get("pagina"):
                item["page_api_url"] = f"/api/page/{item['bib']}/{item['pagina']}"
                item["image_url"] = resolve_image_url_fn(item["bib"], item["pagina"])
    return entity


def serialize_entity_search_results(results: list[dict]) -> list[dict]:
    for row in results:
        row["aliases"] = json.loads(row.get("aliases_json", "[]"))
    return results


def build_fallback_page(*, bib: str, pagina: str, text_dir, images_dir):
    text_path = text_dir / bib / f"{pagina}.txt"
    json_path = text_dir / bib / f"{pagina}.json"
    if not text_path.exists() and not json_path.exists():
        return None

    metadata = {"bib": bib, "pagina": pagina, "jornal": "?", "ano": "?", "edicao": "?"}
    if json_path.exists():
        metadata.update(json.loads(json_path.read_text(encoding="utf-8")))

    image_path = images_dir / bib / f"{pagina}.jpg"
    return {
        "bib": bib,
        "pagina": pagina,
        "jornal": metadata.get("jornal") or metadata.get("periodico") or "?",
        "ano": str(metadata.get("ano", "?")),
        "edicao": str(metadata.get("edicao", "?")),
        "text_path": str(text_path) if text_path.exists() else None,
        "image_path": str(image_path) if image_path.exists() else None,
    }
