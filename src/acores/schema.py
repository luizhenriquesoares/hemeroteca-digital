"""Schema SQLite para registros paroquiais dos Açores."""

PARISH_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS parish_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_type TEXT NOT NULL,
    event_date TEXT,
    birth_date TEXT,
    person_name TEXT NOT NULL,
    father_name TEXT,
    mother_name TEXT,
    paternal_grandfather TEXT,
    paternal_grandmother TEXT,
    maternal_grandfather TEXT,
    maternal_grandmother TEXT,
    spouse_name TEXT,
    godparents_json TEXT DEFAULT '[]',
    priest TEXT,
    parish TEXT NOT NULL,
    island TEXT NOT NULL,
    place TEXT,
    notes TEXT,
    raw_text TEXT,
    source_collection TEXT,
    source_page TEXT,
    source_image TEXT,
    confidence REAL DEFAULT 0.8,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS genealogy_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parish_record_id INTEGER REFERENCES parish_records(id),
    entity_id INTEGER,
    link_type TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    evidence TEXT,
    reviewer TEXT DEFAULT 'auto',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_parish_person ON parish_records(person_name);
CREATE INDEX IF NOT EXISTS idx_parish_father ON parish_records(father_name);
CREATE INDEX IF NOT EXISTS idx_parish_mother ON parish_records(mother_name);
CREATE INDEX IF NOT EXISTS idx_parish_type ON parish_records(record_type);
CREATE INDEX IF NOT EXISTS idx_parish_parish ON parish_records(parish);
CREATE INDEX IF NOT EXISTS idx_parish_collection ON parish_records(source_collection);
CREATE INDEX IF NOT EXISTS idx_genealogy_parish ON genealogy_links(parish_record_id);
CREATE INDEX IF NOT EXISTS idx_genealogy_entity ON genealogy_links(entity_id);
"""
