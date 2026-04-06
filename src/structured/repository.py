"""Persistência SQLite da camada estruturada orientada a evidências."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.config import STRUCTURED_DB
from src.structured.entities import normalize_name
from src.structured.models import PageReference
from src.structured.repository_mappers import (
    build_entity_payload,
    dump_aliases,
    dump_attributes,
    rows_to_dicts,
)
from src.structured.repository_queries import (
    GET_ENTITY_EVIDENCES_SQL,
    GET_ENTITY_MENTIONS_SQL,
    GET_ENTITY_RELATIONS_SQL,
    GET_ENTITY_SQL,
    GET_PAGE_SQL,
    INSERT_MENTION_SQL,
    INSERT_RELATION_EVIDENCE_SQL,
    INSERT_RELATION_SQL,
    MENTION_EXISTS_SQL,
    RELATION_EVIDENCE_EXISTS_SQL,
    SEARCH_ENTITIES_SQL,
    SELECT_ENTITY_ID_SQL,
    SELECT_PAGE_ID_SQL,
    SELECT_RELATION_ID_SQL,
    UPDATE_RELATION_SQL,
    UPSERT_ENTITY_SQL,
    UPSERT_PAGE_SQL,
)
from src.structured.schema import SCHEMA_SQL


class StructuredRepository:
    """Repositório SQLite para entidades, menções, relações e evidências."""

    def __init__(self, db_path: Path = STRUCTURED_DB):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def upsert_page(self, page: PageReference) -> int:
        with self.connect() as conn:
            conn.execute(UPSERT_PAGE_SQL, (page.bib, page.pagina, page.jornal, page.ano, page.edicao, page.text_path, page.image_path))
            row = conn.execute(SELECT_PAGE_ID_SQL, (page.bib, page.pagina)).fetchone()
            return int(row["id"])

    def upsert_entity(
        self,
        *,
        entity_type: str,
        canonical_name: str,
        normalized_name: str,
        aliases: tuple[str, ...],
        attributes: dict,
        year: str = "",
    ) -> int:
        aliases_json = dump_aliases(aliases)
        attributes_json = dump_attributes(attributes)
        with self.connect() as conn:
            conn.execute(UPSERT_ENTITY_SQL, (entity_type, canonical_name, normalized_name, aliases_json, attributes_json, year or None, year or None))
            row = conn.execute(SELECT_ENTITY_ID_SQL, (entity_type, normalized_name)).fetchone()
            return int(row["id"])

    def add_mention(
        self,
        *,
        entity_id: int,
        page_id: int,
        chunk_id: str,
        surface_form: str,
        snippet: str,
        confidence: float,
        source_text: str,
    ) -> None:
        with self.connect() as conn:
            exists = conn.execute(MENTION_EXISTS_SQL, (entity_id, page_id, chunk_id, surface_form)).fetchone()
            if exists:
                return
            conn.execute(INSERT_MENTION_SQL, (entity_id, page_id, chunk_id, surface_form, snippet, confidence, source_text))

    def upsert_relation(
        self,
        *,
        subject_entity_id: int,
        predicate: str,
        object_entity_id: int | None,
        object_literal: str,
        confidence: float,
        status: str,
        extraction_method: str,
    ) -> int:
        with self.connect() as conn:
            existing = conn.execute(SELECT_RELATION_ID_SQL, (subject_entity_id, predicate, object_entity_id, object_literal)).fetchone()
            if existing:
                conn.execute(UPDATE_RELATION_SQL, (confidence, status, extraction_method, int(existing["id"])))
                return int(existing["id"])

            conn.execute(INSERT_RELATION_SQL, (subject_entity_id, predicate, object_entity_id, object_literal, confidence, status, extraction_method))
            row = conn.execute(SELECT_RELATION_ID_SQL, (subject_entity_id, predicate, object_entity_id, object_literal)).fetchone()
            return int(row["id"])

    def add_relation_evidence(
        self,
        *,
        relation_id: int,
        page_id: int,
        chunk_id: str,
        quote: str,
        confidence: float,
    ) -> None:
        with self.connect() as conn:
            exists = conn.execute(RELATION_EVIDENCE_EXISTS_SQL, (relation_id, page_id, chunk_id, quote)).fetchone()
            if exists:
                return
            conn.execute(INSERT_RELATION_EVIDENCE_SQL, (relation_id, page_id, chunk_id, quote, confidence))

    def search_entities(self, query: str, limit: int = 10) -> list[dict]:
        terms = normalize_name(query)
        with self.connect() as conn:
            rows = conn.execute(SEARCH_ENTITIES_SQL, (f"%{terms}%", f"%{query.strip()}%", limit)).fetchall()
        return rows_to_dicts(rows)

    def get_entity(self, entity_id: int) -> dict | None:
        with self.connect() as conn:
            entity = conn.execute(GET_ENTITY_SQL, (entity_id,)).fetchone()
            if not entity:
                return None

            mentions = conn.execute(GET_ENTITY_MENTIONS_SQL, (entity_id,)).fetchall()
            relations = conn.execute(GET_ENTITY_RELATIONS_SQL, (entity_id, entity_id)).fetchall()
            evidences = conn.execute(GET_ENTITY_EVIDENCES_SQL, (entity_id, entity_id)).fetchall()

        return build_entity_payload(entity, mentions, relations, evidences)

    def get_page(self, bib: str, pagina: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(GET_PAGE_SQL, (bib, pagina)).fetchone()
        return dict(row) if row else None
