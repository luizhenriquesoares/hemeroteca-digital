"""Mappers do repositório estruturado."""

from __future__ import annotations

import json


def dump_aliases(aliases: tuple[str, ...]) -> str:
    return json.dumps(sorted(set(filter(None, aliases))), ensure_ascii=False)


def dump_attributes(attributes: dict) -> str:
    return json.dumps(attributes or {}, ensure_ascii=False)


def rows_to_dicts(rows) -> list[dict]:
    return [dict(row) for row in rows]


def build_entity_payload(entity_row, mentions, relations, evidences) -> dict:
    data = dict(entity_row)
    data["mentions"] = rows_to_dicts(mentions)
    data["relations"] = rows_to_dicts(relations)
    data["evidences"] = rows_to_dicts(evidences)
    return data
