"""SQL queries reutilizáveis do repositório estruturado."""

UPSERT_PAGE_SQL = """
INSERT INTO pages (bib, pagina, jornal, ano, edicao, text_path, image_path, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(bib, pagina) DO UPDATE SET
    jornal = excluded.jornal,
    ano = excluded.ano,
    edicao = excluded.edicao,
    text_path = COALESCE(excluded.text_path, pages.text_path),
    image_path = COALESCE(excluded.image_path, pages.image_path),
    updated_at = CURRENT_TIMESTAMP
"""

UPSERT_ENTITY_SQL = """
INSERT INTO entities (
    type, canonical_name, normalized_name, aliases_json, attributes_json, first_seen_year, last_seen_year, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(type, normalized_name) DO UPDATE SET
    canonical_name = excluded.canonical_name,
    aliases_json = excluded.aliases_json,
    attributes_json = excluded.attributes_json,
    first_seen_year = COALESCE(entities.first_seen_year, excluded.first_seen_year),
    last_seen_year = COALESCE(excluded.last_seen_year, entities.last_seen_year),
    updated_at = CURRENT_TIMESTAMP
"""

SELECT_PAGE_ID_SQL = "SELECT id FROM pages WHERE bib = ? AND pagina = ?"
SELECT_ENTITY_ID_SQL = "SELECT id FROM entities WHERE type = ? AND normalized_name = ?"

MENTION_EXISTS_SQL = """
SELECT 1 FROM entity_mentions
WHERE entity_id = ? AND page_id = ? AND chunk_id = ? AND surface_form = ?
"""

INSERT_MENTION_SQL = """
INSERT INTO entity_mentions (
    entity_id, page_id, chunk_id, surface_form, snippet, confidence, source_text
)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

SELECT_RELATION_ID_SQL = """
SELECT id FROM relations
WHERE subject_entity_id = ? AND predicate = ? AND COALESCE(object_entity_id, 0) = COALESCE(?, 0) AND object_literal = ?
"""

UPDATE_RELATION_SQL = """
UPDATE relations
SET confidence = MAX(confidence, ?),
    status = ?,
    extraction_method = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
"""

INSERT_RELATION_SQL = """
INSERT INTO relations (
    subject_entity_id, predicate, object_entity_id, object_literal, confidence, status, extraction_method, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
"""

RELATION_EVIDENCE_EXISTS_SQL = """
SELECT 1 FROM relation_evidence
WHERE relation_id = ? AND page_id = ? AND chunk_id = ? AND quote = ?
"""

INSERT_RELATION_EVIDENCE_SQL = """
INSERT INTO relation_evidence (relation_id, page_id, chunk_id, quote, confidence)
VALUES (?, ?, ?, ?, ?)
"""

SEARCH_ENTITIES_SQL = """
SELECT
    e.id,
    e.type,
    e.canonical_name,
    e.normalized_name,
    e.aliases_json,
    COUNT(DISTINCT m.id) AS mentions,
    MIN(p.ano) AS first_year,
    MAX(p.ano) AS last_year
FROM entities e
LEFT JOIN entity_mentions m ON m.entity_id = e.id
LEFT JOIN pages p ON p.id = m.page_id
WHERE e.normalized_name LIKE ? OR e.canonical_name LIKE ?
GROUP BY e.id
ORDER BY mentions DESC, e.canonical_name ASC
LIMIT ?
"""

GET_ENTITY_SQL = """
SELECT id, type, canonical_name, normalized_name, aliases_json, attributes_json, first_seen_year, last_seen_year
FROM entities
WHERE id = ?
"""

GET_ENTITY_MENTIONS_SQL = """
SELECT m.surface_form, m.snippet, m.confidence, p.bib, p.pagina, p.jornal, p.ano, p.edicao, p.image_path
FROM entity_mentions m
JOIN pages p ON p.id = m.page_id
WHERE m.entity_id = ?
ORDER BY m.confidence DESC, p.ano ASC, p.pagina ASC
LIMIT 25
"""

GET_ENTITY_RELATIONS_SQL = """
SELECT
    r.id,
    r.predicate,
    r.confidence,
    r.status,
    r.object_literal,
    subject.canonical_name AS subject_name,
    object_entity.canonical_name AS object_name
FROM relations r
JOIN entities subject ON subject.id = r.subject_entity_id
LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
WHERE r.subject_entity_id = ? OR r.object_entity_id = ?
ORDER BY r.confidence DESC, r.predicate ASC
LIMIT 50
"""

GET_ENTITY_EVIDENCES_SQL = """
SELECT
    re.quote,
    re.confidence,
    r.predicate,
    p.bib,
    p.pagina,
    p.jornal,
    p.ano,
    p.edicao,
    p.image_path
FROM relation_evidence re
JOIN relations r ON r.id = re.relation_id
JOIN pages p ON p.id = re.page_id
WHERE r.subject_entity_id = ? OR r.object_entity_id = ?
ORDER BY re.confidence DESC, p.ano ASC, p.pagina ASC
LIMIT 50
"""

GET_PAGE_SQL = """
SELECT id, bib, pagina, jornal, ano, edicao, text_path, image_path
FROM pages
WHERE bib = ? AND pagina = ?
"""
