"""Repositório SQLite para registros paroquiais dos Açores."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.acores.schema import PARISH_SCHEMA_SQL
from src.config import DATA_DIR

PARISH_DB = DATA_DIR / "acores" / "parish_records.db"


class ParishRepository:
    def __init__(self, db_path: Path = PARISH_DB):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(PARISH_SCHEMA_SQL)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def upsert_record(self, record: dict, *, source_collection: str, source_page: str, raw_text: str = "") -> int:
        """Insere ou atualiza um registro paroquial."""
        with self.connect() as conn:
            # Verificar se já existe
            existing = conn.execute(
                """SELECT id FROM parish_records
                   WHERE source_collection = ? AND source_page = ? AND person_name = ?""",
                (source_collection, source_page, record.get("person_name", "")),
            ).fetchone()
            if existing:
                return existing["id"]

            godparents = record.get("godparents", [])
            if isinstance(godparents, str):
                godparents = [godparents]

            cursor = conn.execute(
                """INSERT INTO parish_records
                   (record_type, event_date, birth_date, person_name,
                    father_name, mother_name,
                    paternal_grandfather, paternal_grandmother,
                    maternal_grandfather, maternal_grandmother,
                    spouse_name, godparents_json, priest,
                    parish, island, place, notes, raw_text,
                    source_collection, source_page, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.get("record_type", "baptism"),
                    record.get("event_date", ""),
                    record.get("birth_date", ""),
                    record.get("person_name", ""),
                    record.get("father_name", ""),
                    record.get("mother_name", ""),
                    record.get("paternal_grandfather", ""),
                    record.get("paternal_grandmother", ""),
                    record.get("maternal_grandfather", ""),
                    record.get("maternal_grandmother", ""),
                    record.get("spouse_name", ""),
                    json.dumps(godparents, ensure_ascii=False),
                    record.get("priest", ""),
                    record.get("parish", "") or "São Pedro",
                    record.get("island", "") or "São Miguel",
                    record.get("place", ""),
                    record.get("notes", ""),
                    raw_text,
                    source_collection,
                    source_page,
                    record.get("confidence", 0.8),
                ),
            )
            return cursor.lastrowid

    def import_collection_results(self, results_dir: Path, collection_id: str) -> dict:
        """Importa resultados de OCR de uma coleção para o banco."""
        imported = 0
        skipped = 0

        for json_file in sorted(results_dir.glob("*.json")):
            if json_file.name == "all_records.json":
                continue
            data = json.loads(json_file.read_text(encoding="utf-8"))
            raw_text = data.get("raw_text", "")
            page_id = data.get("page_id", json_file.stem)

            for record in data.get("records", []):
                if not record.get("person_name"):
                    skipped += 1
                    continue
                self.upsert_record(
                    record,
                    source_collection=collection_id,
                    source_page=page_id,
                    raw_text=raw_text,
                )
                imported += 1

        return {"imported": imported, "skipped": skipped}

    def search_by_surname(self, surname: str, limit: int = 20) -> list[dict]:
        """Busca registros por sobrenome."""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM parish_records
                   WHERE person_name LIKE ? OR father_name LIKE ? OR mother_name LIKE ?
                   ORDER BY event_date ASC
                   LIMIT ?""",
                (f"%{surname}%", f"%{surname}%", f"%{surname}%", limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        """Estatísticas do acervo paroquial."""
        with self.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM parish_records").fetchone()[0]
            by_type = conn.execute(
                "SELECT record_type, COUNT(*) as c FROM parish_records GROUP BY record_type"
            ).fetchall()
            parishes = conn.execute(
                "SELECT DISTINCT parish, island FROM parish_records"
            ).fetchall()
            return {
                "total_records": total,
                "by_type": {r["record_type"]: r["c"] for r in by_type},
                "parishes": [{"parish": r["parish"], "island": r["island"]} for r in parishes],
            }
