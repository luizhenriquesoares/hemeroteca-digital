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
    base_normalized_name TEXT NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS relation_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relation_id INTEGER NOT NULL,
    review_status TEXT NOT NULL,
    reviewer TEXT NOT NULL DEFAULT 'humano',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(relation_id) REFERENCES relations(id)
);

CREATE TABLE IF NOT EXISTS entity_identity_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    review_status TEXT NOT NULL,
    reviewer TEXT NOT NULL DEFAULT 'humano',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(entity_id) REFERENCES entities(id)
);

CREATE TABLE IF NOT EXISTS entity_merge_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_entity_id INTEGER NOT NULL,
    target_entity_id INTEGER NOT NULL,
    review_status TEXT NOT NULL,
    reviewer TEXT NOT NULL DEFAULT 'humano',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(source_entity_id) REFERENCES entities(id),
    FOREIGN KEY(target_entity_id) REFERENCES entities(id)
);

CREATE INDEX IF NOT EXISTS idx_entities_normalized_name ON entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_entities_base_normalized_name ON entities(base_normalized_name);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_pages_bib ON pages(bib);
CREATE INDEX IF NOT EXISTS idx_mentions_entity_id ON entity_mentions(entity_id);
CREATE INDEX IF NOT EXISTS idx_mentions_page_id ON entity_mentions(page_id);
CREATE INDEX IF NOT EXISTS idx_mentions_page_entity_id ON entity_mentions(page_id, entity_id);
CREATE INDEX IF NOT EXISTS idx_relations_subject_entity_id ON relations(subject_entity_id);
CREATE INDEX IF NOT EXISTS idx_relations_predicate ON relations(predicate);
CREATE INDEX IF NOT EXISTS idx_relation_evidence_relation_id ON relation_evidence(relation_id);
CREATE INDEX IF NOT EXISTS idx_relation_evidence_page_id ON relation_evidence(page_id);
CREATE INDEX IF NOT EXISTS idx_relation_reviews_relation_id ON relation_reviews(relation_id);
CREATE INDEX IF NOT EXISTS idx_entity_identity_reviews_entity_id ON entity_identity_reviews(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_merge_reviews_pair ON entity_merge_reviews(source_entity_id, target_entity_id);

-- Tabela de cache pré-computada para discovery
CREATE TABLE IF NOT EXISTS entity_stats_cache (
    entity_id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    mentions INTEGER NOT NULL DEFAULT 0,
    strong_mentions INTEGER NOT NULL DEFAULT 0,
    first_year INTEGER,
    last_year INTEGER,
    relation_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_entity_stats_type_mentions ON entity_stats_cache(entity_type, mentions DESC);

-- Índices de performance para discovery overview
CREATE INDEX IF NOT EXISTS idx_mentions_entity_page ON entity_mentions(entity_id, page_id);
CREATE INDEX IF NOT EXISTS idx_entities_type_name ON entities(type, canonical_name);
CREATE INDEX IF NOT EXISTS idx_pages_ano ON pages(ano);
CREATE INDEX IF NOT EXISTS idx_relations_object_entity_id ON relations(object_entity_id);
CREATE INDEX IF NOT EXISTS idx_relation_evidence_page_relation ON relation_evidence(page_id, relation_id);
"""
