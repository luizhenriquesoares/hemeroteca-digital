"""Schema SQLite da camada estruturada."""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bib TEXT NOT NULL,
    pagina TEXT NOT NULL,
    jornal TEXT NOT NULL DEFAULT '?',
    ano TEXT NOT NULL DEFAULT '?',
    edicao TEXT NOT NULL DEFAULT '?',
    text_path TEXT,
    image_path TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bib, pagina)
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    first_seen_year TEXT,
    last_seen_year TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(type, normalized_name)
);

CREATE TABLE IF NOT EXISTS entity_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    chunk_id TEXT NOT NULL,
    surface_form TEXT NOT NULL,
    snippet TEXT NOT NULL,
    confidence REAL NOT NULL,
    source_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(entity_id) REFERENCES entities(id),
    FOREIGN KEY(page_id) REFERENCES pages(id)
);

CREATE TABLE IF NOT EXISTS relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_entity_id INTEGER NOT NULL,
    predicate TEXT NOT NULL,
    object_entity_id INTEGER,
    object_literal TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'hypothesis',
    extraction_method TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(subject_entity_id, predicate, object_entity_id, object_literal),
    FOREIGN KEY(subject_entity_id) REFERENCES entities(id),
    FOREIGN KEY(object_entity_id) REFERENCES entities(id)
);

CREATE TABLE IF NOT EXISTS relation_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relation_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    chunk_id TEXT NOT NULL,
    quote TEXT NOT NULL,
    confidence REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(relation_id) REFERENCES relations(id),
    FOREIGN KEY(page_id) REFERENCES pages(id)
);

CREATE INDEX IF NOT EXISTS idx_entities_normalized_name ON entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_mentions_entity_id ON entity_mentions(entity_id);
CREATE INDEX IF NOT EXISTS idx_mentions_page_id ON entity_mentions(page_id);
CREATE INDEX IF NOT EXISTS idx_relations_subject_entity_id ON relations(subject_entity_id);
"""
