"""Persistência SQLite da camada estruturada orientada a evidências."""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.config import STRUCTURED_DB
from src.structured.entities import normalize_name
from src.structured.models import PageReference
from src.structured.quality import assess_discovery_noise, assess_entity_noise
from src.structured.repository_mappers import (
    build_entity_payload,
    dump_aliases,
    dump_attributes,
    rows_to_dicts,
)
from src.structured.repository_queries import (
    GET_ENTITY_IDENTITY_REVIEW_SQL,
    GET_ENTITY_MERGE_REVIEW_SQL,
    GET_ENTITY_EVIDENCES_SQL,
    GET_ENTITY_MENTIONS_SQL,
    GET_ENTITY_RELATIONS_SQL,
    GET_RELATION_REVIEW_SQL,
    GET_ENTITY_SQL,
    GET_PAGE_SQL,
    INSERT_ENTITY_IDENTITY_REVIEW_SQL,
    INSERT_ENTITY_MERGE_REVIEW_SQL,
    INSERT_RELATION_REVIEW_SQL,
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

_YEAR_RE = re.compile(r"(1[0-9]{3}|20[0-9]{2})")


def _parse_year(value: str | int | None) -> int | None:
    if value is None:
        return None
    match = _YEAR_RE.search(str(value))
    return int(match.group(1)) if match else None


def _year_sql(alias: str = "p") -> str:
    return (
        f"CASE "
        f"WHEN TRIM(REPLACE({alias}.ano, 'Ano ', '')) GLOB '[0-9][0-9][0-9][0-9]' "
        f"THEN CAST(TRIM(REPLACE({alias}.ano, 'Ano ', '')) AS INTEGER) "
        f"ELSE NULL END"
    )


def _build_page_scope_sql(
    *,
    page_alias: str = "p",
    bib: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> tuple[str, list]:
    conditions: list[str] = []
    params: list = []
    year_expr = _year_sql(page_alias)

    if bib:
        conditions.append(f"{page_alias}.bib = ?")
        params.append(bib)
    if year_from is not None:
        conditions.append(f"{year_expr} >= ?")
        params.append(int(year_from))
    if year_to is not None:
        conditions.append(f"{year_expr} <= ?")
        params.append(int(year_to))

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where_sql, params


def _extend_where(where_sql: str, condition: str) -> str:
    if where_sql:
        return f"{where_sql} AND {condition}"
    return f"WHERE {condition}"


def _loads_json(value: str | None, fallback):
    try:
        return json.loads(value or "")
    except Exception:
        return fallback


def _compact_whitespace(value: str | None) -> str:
    return " ".join(str(value or "").split())


def _format_count(value: int | float | None) -> str:
    try:
        number = int(value or 0)
    except Exception:
        number = 0
    return f"{number:,}".replace(",", ".")


def _name_token_count(value: str | None) -> int:
    return len([token for token in normalize_name(str(value or "")).split() if token])


_VOWEL_RE = re.compile(r"[aeiouáàâãéêíóôõú]", re.IGNORECASE)
_REPEATED_CHARS = re.compile(r"(.)\1{2,}")
_OCR_GARBAGE = re.compile(r"[^a-záàâãéêíóôõúçA-ZÁÀÂÃÉÊÍÓÔÕÚÇ\s'\-]")
_PREP_SET = {"de", "da", "do", "das", "dos", "e", "d'", "d'"}
_NON_PORTUGUESE = re.compile(
    r"\b(?:sheep|skins|french|the|and|with|from|king|queen)\b", re.IGNORECASE
)
_NOISE_FORMULAS = re.compile(
    r"\b(?:nosso\s+senhor|nossa\s+se|reino\s+d|rainha\s+nossa|"
    r"legitimidade|soberan[cç]|nussimento|divisaô|"
    r"successaõ|successora|santo\s+antão|inspecção|"
    r"juiz\s+paz|brejo\s+d|massa\s+ma)\b",
    re.IGNORECASE,
)


def _strip_accents(text: str) -> str:
    import unicodedata as _ud
    return "".join(c for c in _ud.normalize("NFKD", text) if not _ud.combining(c))


def _is_legible_name(name: str) -> bool:
    """Verifica se um nome parece legível em português (não corrompido por OCR)."""
    if not name or len(name) < 5:
        return False
    if _NOISE_FORMULAS.search(name):
        return False
    if _NON_PORTUGUESE.search(name):
        return False
    tokens = name.split()
    substantive = [t for t in tokens if t.lower() not in _PREP_SET]
    if not substantive:
        return False
    short_tokens = 0
    for token in substantive:
        if len(token) <= 1:
            return False
        if len(token) == 2:
            short_tokens += 1
            # Token de 2 chars sem vogal = lixo (Th, Ae, etc.)
            if not _VOWEL_RE.search(token):
                return False
        if len(token) >= 3 and not _VOWEL_RE.search(token):
            return False
    if len(substantive) >= 3 and short_tokens >= 2:
        return False
    # Letras repetidas 3+ vezes após remover acentos (Copaaá→Copaaa)
    stripped = _strip_accents(name)
    if _REPEATED_CHARS.search(stripped):
        return False
    # Último token substantivo muito curto = provável truncamento
    last = substantive[-1] if substantive else ""
    if len(last) <= 3:
        return False
    clean = re.sub(r"\s+", " ", _OCR_GARBAGE.sub("", name)).strip()
    if len(clean) < len(name.strip()) * 0.85:
        return False
    return True


def _format_period(first_year: str | int | None, last_year: str | int | None) -> str | None:
    start = _parse_year(first_year)
    end = _parse_year(last_year)
    if start is not None and end is not None:
        if start == end:
            return str(start)
        return f"{start} a {end}"
    if start is not None:
        return f"desde {start}"
    if end is not None:
        return f"até {end}"
    return None


def _append_question(questions: list[str], text: str | None, *, limit: int) -> None:
    question = _compact_whitespace(text)
    if not question or question in questions or len(questions) >= limit:
        return
    questions.append(question)


def _journal_display_name(jornal: str | None, bib: str | None) -> str:
    label = str(jornal or "").strip()
    if not label or label == "?":
        return f"Acervo {bib or '?'}"
    return label


def _review_cluster_key(item: dict) -> str:
    key = str(item.get("base_normalized_name") or "").strip()
    if key:
        return key
    return normalize_name(str(item.get("canonical_name") or ""))


def _dedupe_merge_candidates(candidates: list[dict], *, limit: int = 3) -> list[dict]:
    seen_ids: set[int] = set()
    deduped: list[dict] = []
    for candidate in sorted(
        candidates,
        key=lambda item: (
            float(item.get("score") or 0),
            int(item.get("mentions") or 0),
            str(item.get("canonical_name") or ""),
        ),
        reverse=True,
    ):
        candidate_id = int(candidate.get("id") or 0)
        if candidate_id and candidate_id in seen_ids:
            continue
        if candidate_id:
            seen_ids.add(candidate_id)
        deduped.append(candidate)
        if len(deduped) >= limit:
            break
    return deduped


def _collapse_review_entities(items: list[dict], *, limit: int) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for item in items:
        buckets.setdefault(_review_cluster_key(item), []).append(item)

    collapsed: list[dict] = []
    for cluster_items in buckets.values():
        best = max(
            cluster_items,
            key=lambda item: (
                int(item.get("mentions") or 0),
                int(_parse_year(item.get("last_year")) or 0),
                len(str(item.get("canonical_name") or "")),
            ),
        )
        merged = dict(best)
        merged["variant_count"] = len(cluster_items)
        merged["total_mentions"] = sum(int(item.get("mentions") or 0) for item in cluster_items)
        merged["variant_names"] = sorted({str(item.get("canonical_name") or "") for item in cluster_items if item.get("canonical_name")})
        merged["merge_candidates"] = _dedupe_merge_candidates(
            [candidate for item in cluster_items for candidate in (item.get("merge_candidates") or [])],
            limit=3,
        )
        collapsed.append(merged)

    collapsed.sort(
        key=lambda item: (
            int(item.get("total_mentions") or 0),
            int(item.get("variant_count") or 0),
            str(item.get("canonical_name") or ""),
        ),
        reverse=True,
    )
    return collapsed[:limit]


def _merge_aliases(entity: dict) -> set[str]:
    aliases = {
        str(item).strip()
        for item in _loads_json(entity.get("aliases_json"), [])
        if str(item).strip()
    }
    canonical_name = str(entity.get("canonical_name") or "").strip()
    if canonical_name:
        aliases.add(canonical_name)
    return aliases


def _merge_title_hints(entity: dict) -> set[str]:
    attrs = _loads_json(entity.get("attributes_json"), {})
    return {
        str(item)
        for item in attrs.get("identity_hints", [])
        if str(item).startswith(("title:", "role:"))
    }


def _rank_merge_candidate_from_rows(entity: dict, candidate: dict) -> dict | None:
    if not entity or not candidate:
        return None
    if int(entity.get("id") or 0) == int(candidate.get("id") or 0):
        return None
    if entity.get("type") != candidate.get("type"):
        return None

    entity_aliases = _merge_aliases(entity)
    candidate_aliases = _merge_aliases(candidate)
    normalized_aliases = {normalize_name(item) for item in entity_aliases if item}
    candidate_normalized_aliases = {normalize_name(item) for item in candidate_aliases if item}
    base_name = entity.get("base_normalized_name") or normalize_name(entity.get("canonical_name", ""))
    candidate_base = candidate.get("base_normalized_name") or normalize_name(candidate.get("canonical_name", ""))
    canonical_name = str(entity.get("canonical_name") or "")
    candidate_name = str(candidate.get("canonical_name") or "")

    shared_aliases = sorted(
        alias for alias in (normalized_aliases & candidate_normalized_aliases)
        if alias and alias != base_name
    )
    base_overlap = bool(base_name and candidate_base) and (
        base_name == candidate_base
        or base_name in candidate_base
        or candidate_base in base_name
    )
    name_overlap = bool(canonical_name and candidate_name) and (
        canonical_name in candidate_name
        or candidate_name in canonical_name
    )

    if not (shared_aliases or base_overlap or name_overlap):
        return None

    reasons = []
    score = 0.0
    entity_title_hints = _merge_title_hints(entity)
    candidate_title_hints = _merge_title_hints(candidate)

    if candidate_base == base_name and base_name:
        reasons.append("mesma base nominal")
        score += 5
    if shared_aliases:
        reasons.append("alias compartilhado")
        score += 3
    if base_name and candidate_base and base_name != candidate_base and (base_name in candidate_base or candidate_base in base_name):
        reasons.append("variação curta/longa do mesmo nome")
        score += 2
    if entity_title_hints and (entity_title_hints & candidate_title_hints):
        reasons.append("mesmo contexto de cargo ou título")
        score += 1.5
    score += min(float(candidate.get("mentions") or 0), 25.0) / 25.0

    if score <= 0:
        return None

    return {
        "id": int(candidate["id"]),
        "canonical_name": candidate_name,
        "type": candidate.get("type"),
        "mentions": int(candidate.get("mentions") or 0),
        "last_year": candidate.get("last_year"),
        "reasons": reasons,
        "score": round(score, 2),
    }


def _entity_effective_status_sql(alias: str = "e") -> str:
    return (
        "COALESCE("
        f"(SELECT er.review_status FROM entity_identity_reviews er WHERE er.entity_id = {alias}.id ORDER BY er.id DESC LIMIT 1), "
        f"json_extract({alias}.attributes_json, '$.effective_identity_status'), "
        f"json_extract({alias}.attributes_json, '$.identity_status'), "
        "'resolved'"
        ")"
    )


def _active_entity_condition_sql(alias: str = "e") -> str:
    return f"{_entity_effective_status_sql(alias)} NOT IN ('rejected', 'merged')"


def _entity_effective_status_from_row(row: dict) -> str:
    attrs = _loads_json(row.get("attributes_json"), {})
    return (
        row.get("identity_review_status")
        or attrs.get("effective_identity_status")
        or attrs.get("identity_status")
        or "resolved"
    )


def _identity_status_rank(status: str) -> int:
    ranks = {
        "resolved": 3,
        "contextual": 2,
        "ambiguous": 1,
        "rejected": 0,
        "merged": -1,
    }
    return ranks.get(status or "", 0)


def _pair_score(entity: dict) -> tuple:
    return (
        int(entity.get("mentions") or 0),
        _identity_status_rank(_entity_effective_status_from_row(entity)),
        len(str(entity.get("canonical_name") or "")),
        str(entity.get("canonical_name") or ""),
    )


def _choose_merge_direction(left: dict, right: dict) -> tuple[dict, dict]:
    if _pair_score(right) > _pair_score(left):
        return left, right
    return right, left


def _merge_year_text(*values: str | None, prefer: str = "min") -> str | None:
    parsed = []
    fallback = None
    for value in values:
        if value in (None, "", "?"):
            continue
        fallback = fallback or str(value)
        year = _parse_year(value)
        if year is not None:
            parsed.append(year)
    if parsed:
        return str(min(parsed) if prefer == "min" else max(parsed))
    return fallback


def _dedupe_strings(*groups: list[str] | tuple[str, ...]) -> list[str]:
    seen = set()
    values: list[str] = []
    for group in groups:
        for item in group or []:
            if not item:
                continue
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
    return values


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
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(entities)").fetchall()}
            if "base_normalized_name" not in columns:
                conn.execute("ALTER TABLE entities ADD COLUMN base_normalized_name TEXT NOT NULL DEFAULT ''")
                conn.execute("UPDATE entities SET base_normalized_name = normalized_name WHERE base_normalized_name = ''")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_base_normalized_name ON entities(base_normalized_name)")

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
        base_normalized_name: str,
        aliases: tuple[str, ...],
        attributes: dict,
        year: str = "",
    ) -> int:
        aliases_json = dump_aliases(aliases)
        attributes_json = dump_attributes(attributes)
        with self.connect() as conn:
            conn.execute(
                UPSERT_ENTITY_SQL,
                (
                    entity_type,
                    canonical_name,
                    normalized_name,
                    base_normalized_name,
                    aliases_json,
                    attributes_json,
                    year or None,
                    year or None,
                ),
            )
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

    def get_surname_cloud(self, limit: int = 40) -> list[dict]:
        """Retorna os sobrenomes mais frequentes para nuvem de palavras."""
        with self.connect() as conn:
            rows = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        SUBSTR(e.canonical_name,
                               INSTR(e.canonical_name, ' ') + 1) AS surname_raw,
                        COUNT(DISTINCT m.id) AS mentions,
                        COUNT(DISTINCT e.id) AS people
                    FROM entities e
                    JOIN entity_mentions m ON m.entity_id = e.id
                    WHERE e.type = 'person'
                      AND {_active_entity_condition_sql('e')}
                      AND INSTR(e.canonical_name, ' ') > 0
                    GROUP BY surname_raw
                    HAVING mentions >= 3 AND people >= 1
                    ORDER BY mentions DESC
                    LIMIT ?
                    """,
                    (limit * 3,),
                ).fetchall()
            )

        _surname_stopwords = {
            "de", "da", "do", "das", "dos", "e",
            # Fórmulas editoriais e lugares
            "guarde", "paz", "janeiro", "recife", "olinda", "pernambuco",
            "boa", "provincia", "geral", "santo", "diario", "snr", "publica",
            "deos", "deus", "brasil", "rio", "porto", "bahia", "lisboa",
            "comp", "art", "dito", "dita", "anno", "sessao", "nos",
            "villa", "vila", "cidade", "rua", "camara", "governo",
            "norte", "sul", "leste", "oeste", "grande", "pequeno",
            # Títulos e termos institucionais
            "exm", "snr", "illm", "publico", "publica", "nacional",
            "presidente", "imperio", "provincial", "imperial", "general",
            "pro", "armas", "guerra", "municipal", "real", "lei",
            "secretario", "inspector", "commandante", "administrador",
            # Primeiros nomes que não são sobrenomes
            "antonio", "joze", "jose", "joaquim", "francisco", "manoel",
            "joao", "luiz", "pedro", "maria",
            # Substantivos comuns confundidos com sobrenomes
            "direito", "uniao", "união", "particulares", "marinha",
            "imperador", "brazil", "fazenda", "justica", "justiça",
            "commercio", "thesoureiro", "thesouro", "alfandega",
            "exercito", "policia", "correio", "instrucção",
        }
        # Pegar último token como sobrenome real e agrupar
        surname_map: dict[str, dict] = {}
        for row in rows:
            raw = str(row.get("surname_raw") or "").strip()
            if not raw:
                continue
            parts = raw.split()
            # Pegar o último token substantivo
            surname = parts[-1] if parts else raw
            if len(surname) < 3:
                continue
            normalized = normalize_name(surname)
            if normalized in _surname_stopwords:
                continue
            if normalized not in surname_map:
                surname_map[normalized] = {
                    "surname": surname,
                    "mentions": 0,
                    "people": 0,
                }
            surname_map[normalized]["mentions"] += int(row.get("mentions") or 0)
            surname_map[normalized]["people"] += int(row.get("people") or 0)

        cloud = sorted(surname_map.values(), key=lambda x: x["mentions"], reverse=True)[:limit]
        if cloud:
            max_m = cloud[0]["mentions"]
            for item in cloud:
                item["weight"] = round(item["mentions"] / max_m, 3)
        return cloud

    def search_entities(self, query: str, limit: int = 10) -> list[dict]:
        terms = normalize_name(query)
        with self.connect() as conn:
            rows = rows_to_dicts(conn.execute(
                SEARCH_ENTITIES_SQL,
                (f"%{terms}%", f"%{query.strip()}%", f"%{query.strip()}%", limit),
            ).fetchall())
        return [
            row for row in rows
            if _entity_effective_status_from_row(row) not in {"rejected", "merged"}
        ]

    def search_by_surname(self, surname: str, limit: int = 10) -> list[dict]:
        normalized = normalize_name(surname)
        tokens = [token for token in normalized.split() if token]
        if not tokens:
            return []

        search_term = tokens[-1]
        raw_term = str(surname or "").strip()
        fetch_limit = max(limit * 10, 40)

        with self.connect() as conn:
            year_expr = _year_sql("p")
            rows = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        e.id,
                        e.canonical_name,
                        e.normalized_name,
                        e.base_normalized_name,
                        e.aliases_json,
                        e.attributes_json,
                        {_entity_effective_status_sql('e')} AS identity_review_status,
                        COUNT(DISTINCT m.id) AS mentions,
                        MIN({year_expr}) AS first_year,
                        MAX({year_expr}) AS last_year
                    FROM entities e
                    LEFT JOIN entity_mentions m ON m.entity_id = e.id
                    LEFT JOIN pages p ON p.id = m.page_id
                    WHERE e.type = 'person'
                      AND {_active_entity_condition_sql('e')}
                      AND (
                        e.base_normalized_name LIKE ?
                        OR e.normalized_name LIKE ?
                        OR e.canonical_name LIKE ?
                      )
                    GROUP BY e.id
                    HAVING mentions > 0
                    ORDER BY mentions DESC, e.canonical_name ASC
                    LIMIT ?
                    """,
                    (f"%{search_term}%", f"%{search_term}%", f"%{raw_term}%", fetch_limit),
                ).fetchall()
            )

            filtered_rows = []
            for row in rows:
                if _name_token_count(row.get("canonical_name")) < 2:
                    continue
                noise = assess_discovery_noise(
                    entity_type="person",
                    canonical_name=row.get("canonical_name", ""),
                    attributes=_loads_json(row.get("attributes_json"), {}),
                )
                if noise["is_probable_noise"]:
                    continue
                filtered_rows.append(row)

            if not filtered_rows:
                return []

            role_map = self._get_primary_roles_for_entities(
                conn,
                [int(row["id"]) for row in filtered_rows],
            )

        buckets: dict[str, list[dict]] = {}
        for row in filtered_rows:
            key = str(row.get("base_normalized_name") or "").strip() or normalize_name(row.get("canonical_name", ""))
            buckets.setdefault(key, []).append(row)

        groups: list[dict] = []
        for key, items in buckets.items():
            ordered = sorted(
                items,
                key=lambda item: (
                    int(item.get("mentions") or 0),
                    int(_parse_year(item.get("last_year")) or 0),
                    len(str(item.get("canonical_name") or "")),
                    str(item.get("canonical_name") or ""),
                ),
                reverse=True,
            )
            best = ordered[0]
            first_years = [_parse_year(item.get("first_year")) for item in ordered]
            last_years = [_parse_year(item.get("last_year")) for item in ordered]
            valid_first_years = [year for year in first_years if year is not None]
            valid_last_years = [year for year in last_years if year is not None]

            variants = []
            seen_variants = set()
            for item in ordered:
                for candidate in {str(item.get("canonical_name") or "").strip(), *[
                    str(alias).strip()
                    for alias in _loads_json(item.get("aliases_json"), [])
                ]}:
                    if not candidate or candidate in seen_variants:
                        continue
                    seen_variants.add(candidate)
                    variants.append(candidate)

            roles = []
            seen_roles = set()
            for item in ordered:
                role = _compact_whitespace(role_map.get(int(item["id"]), {}).get("role"))
                if role and role not in seen_roles:
                    seen_roles.add(role)
                    roles.append(role)

            members = []
            for item in ordered[:6]:
                item_id = int(item["id"])
                role_info = role_map.get(item_id, {})
                members.append(
                    {
                        "id": item_id,
                        "canonical_name": item.get("canonical_name"),
                        "mentions": int(item.get("mentions") or 0),
                        "first_year": _parse_year(item.get("first_year")),
                        "last_year": _parse_year(item.get("last_year")),
                        "role": role_info.get("role"),
                    }
                )

            groups.append(
                {
                    "group_key": key,
                    "entity_id": int(best["id"]),
                    "canonical_name": best.get("canonical_name"),
                    "surname": search_term,
                    "total_mentions": sum(int(item.get("mentions") or 0) for item in ordered),
                    "first_year": min(valid_first_years) if valid_first_years else None,
                    "last_year": max(valid_last_years) if valid_last_years else None,
                    "variant_count": len(ordered),
                    "variants": variants[:8],
                    "roles": roles[:3],
                    "members": members,
                }
            )

        groups.sort(
            key=lambda item: (
                int(item.get("total_mentions") or 0),
                int(item.get("variant_count") or 0),
                str(item.get("canonical_name") or ""),
            ),
            reverse=True,
        )
        return groups[:limit]

    def rebuild_entity_stats_cache(self) -> int:
        """Pré-computa estatísticas de entidades para queries rápidas."""
        with self.connect() as conn:
            year_expr = _year_sql("p")
            conn.execute("DELETE FROM entity_stats_cache")
            conn.execute(f"""
                INSERT INTO entity_stats_cache
                    (entity_id, entity_type, canonical_name, mentions, strong_mentions,
                     first_year, last_year, relation_count)
                SELECT
                    e.id,
                    e.type,
                    e.canonical_name,
                    COUNT(m.id) AS mentions,
                    SUM(CASE WHEN m.confidence >= 0.6 THEN 1 ELSE 0 END) AS strong_mentions,
                    MIN({year_expr}) AS first_year,
                    MAX({year_expr}) AS last_year,
                    (
                        SELECT COUNT(DISTINCT r.id)
                        FROM relations r
                        WHERE (r.subject_entity_id = e.id OR r.object_entity_id = e.id)
                          AND r.predicate != 'mentioned_with'
                    ) AS relation_count
                FROM entities e
                JOIN entity_mentions m ON m.entity_id = e.id
                JOIN pages p ON p.id = m.page_id
                WHERE {_active_entity_condition_sql('e')}
                GROUP BY e.id
                HAVING mentions >= 2
            """)
            count = conn.execute("SELECT COUNT(*) FROM entity_stats_cache").fetchone()[0]
        return count

    def _ensure_stats_cache(self, conn) -> bool:
        """Garante que o cache existe, cria se necessário."""
        # Criar tabela se não existir
        conn.execute("""
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
            )
        """)
        count = conn.execute("SELECT COUNT(*) FROM entity_stats_cache").fetchone()[0]
        if count == 0:
            # Rebuild na primeira chamada
            self.rebuild_entity_stats_cache()
            return True
        return False

    def get_featured_entity(self, seed: int) -> dict | None:
        with self.connect() as conn:
            self._ensure_stats_cache(conn)
            year_expr = _year_sql("p")
            candidate_rows = rows_to_dicts(
                conn.execute(
                    """
                    SELECT
                        sc.entity_id AS id,
                        sc.canonical_name,
                        e.aliases_json,
                        e.attributes_json,
                        sc.mentions,
                        sc.strong_mentions,
                        sc.first_year,
                        sc.last_year,
                        sc.relation_count
                    FROM entity_stats_cache sc
                    JOIN entities e ON e.id = sc.entity_id
                    WHERE sc.entity_type = 'person'
                      AND sc.mentions >= 10
                      AND sc.strong_mentions >= 2
                    ORDER BY sc.relation_count DESC, sc.mentions DESC, sc.canonical_name ASC
                    LIMIT 500
                    """
                ).fetchall()
            )

            candidates = []
            for row in candidate_rows:
                name = row.get("canonical_name", "")
                if _name_token_count(name) < 3:
                    continue
                if not _is_legible_name(name):
                    continue
                attrs = _loads_json(row.get("attributes_json"), {})
                noise = assess_discovery_noise(
                    entity_type="person",
                    canonical_name=name,
                    attributes=attrs,
                )
                if noise["is_probable_noise"]:
                    continue
                candidates.append(row)

            if not candidates:
                return None

            selected = candidates[seed % len(candidates)]
            entity_id = int(selected["id"])
            role_map = self._get_primary_roles_for_entities(conn, [entity_id])

            top_snippet = conn.execute(
                f"""
                SELECT
                    m.surface_form,
                    m.snippet,
                    m.confidence,
                    p.bib,
                    p.pagina,
                    p.jornal,
                    p.ano,
                    p.edicao,
                    {year_expr} AS year
                FROM entity_mentions m
                JOIN pages p ON p.id = m.page_id
                WHERE m.entity_id = ?
                ORDER BY (m.confidence >= 0.6) DESC, m.confidence DESC, LENGTH(m.snippet) DESC, p.id ASC
                LIMIT 1
                """,
                (entity_id,),
            ).fetchone()

            relation_highlights = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        r.id,
                        r.predicate,
                        CASE
                            WHEN r.subject_entity_id = ? THEN COALESCE(object_entity.canonical_name, r.object_literal)
                            ELSE subject.canonical_name
                        END AS counterpart_name,
                        COUNT(re.id) AS evidences,
                        AVG(COALESCE(re.confidence, r.confidence)) AS confidence,
                        MAX({year_expr}) AS last_year
                    FROM relations r
                    JOIN entities subject ON subject.id = r.subject_entity_id
                    LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                    LEFT JOIN relation_evidence re ON re.relation_id = r.id
                    LEFT JOIN pages p ON p.id = re.page_id
                    WHERE (r.subject_entity_id = ? OR r.object_entity_id = ?)
                      AND r.predicate != 'mentioned_with'
                    GROUP BY r.id
                    ORDER BY evidences DESC, confidence DESC, r.id ASC
                    LIMIT 4
                    """,
                    (entity_id, entity_id, entity_id),
                ).fetchall()
            )

        aliases = sorted(
            alias for alias in {
                str(selected.get("canonical_name") or "").strip(),
                *[str(item).strip() for item in _loads_json(selected.get("aliases_json"), [])],
            }
            if alias
        )
        primary_role = role_map.get(entity_id, {})
        period_label = _format_period(selected.get("first_year"), selected.get("last_year"))
        role_fragment = f" como {primary_role.get('role')}" if primary_role.get("role") else ""
        period_fragment = f" entre {period_label}" if period_label else ""

        return {
            "id": entity_id,
            "canonical_name": selected.get("canonical_name"),
            "aliases": aliases[:6],
            "mentions": int(selected.get("mentions") or 0),
            "relation_count": int(selected.get("relation_count") or 0),
            "first_year": _parse_year(selected.get("first_year")),
            "last_year": _parse_year(selected.get("last_year")),
            "period_label": period_label,
            "role": primary_role.get("role"),
            "role_evidences": int(primary_role.get("evidences") or 0),
            "top_snippet": dict(top_snippet) if top_snippet else None,
            "relation_highlights": [
                {
                    **item,
                    "evidences": int(item.get("evidences") or 0),
                    "confidence": round(float(item.get("confidence") or 0.0), 2),
                    "last_year": _parse_year(item.get("last_year")),
                }
                for item in relation_highlights
            ],
            "discovery_question": (
                f"Quem era {selected.get('canonical_name')}, "
                f"citado {_format_count(selected.get('mentions'))} vezes"
                f"{role_fragment}{period_fragment}?"
            ),
        }

    def get_entity(self, entity_id: int) -> dict | None:
        with self.connect() as conn:
            entity = conn.execute(GET_ENTITY_SQL, (entity_id,)).fetchone()
            if not entity:
                return None

            mentions = conn.execute(GET_ENTITY_MENTIONS_SQL, (entity_id,)).fetchall()
            relation_rows = conn.execute(GET_ENTITY_RELATIONS_SQL, (entity_id, entity_id)).fetchall()
            evidences = conn.execute(GET_ENTITY_EVIDENCES_SQL, (entity_id, entity_id)).fetchall()
            relations = []
            for relation in relation_rows:
                row = dict(relation)
                review = conn.execute(GET_RELATION_REVIEW_SQL, (row["id"],)).fetchone()
                if review:
                    row["review_status"] = review["review_status"]
                    row["reviewer"] = review["reviewer"]
                    row["review_note"] = review["note"]
                    row["reviewed_at"] = review["created_at"]
                    row["effective_status"] = review["review_status"]
                else:
                    row["effective_status"] = row["status"]
                relations.append(row)
            identity_review = conn.execute(GET_ENTITY_IDENTITY_REVIEW_SQL, (entity_id,)).fetchone()

        payload = build_entity_payload(entity, mentions, relations, evidences)
        if identity_review:
            payload["identity_review"] = dict(identity_review)
            try:
                attrs = json.loads(payload.get("attributes_json", "{}"))
            except Exception:
                attrs = {}
            attrs["effective_identity_status"] = identity_review["review_status"]
            attrs["identity_review_note"] = identity_review["note"]
            payload["attributes_json"] = json.dumps(attrs, ensure_ascii=False)
        else:
            payload["identity_review"] = None

        payload["story"] = self.get_entity_story(entity_id)
        payload["merge_candidates"] = self.get_entity_merge_candidates(entity_id, limit=5)
        payload["noise_assessment"] = self.get_entity_noise_assessment(entity_id)
        return payload

    def get_page(self, bib: str, pagina: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(GET_PAGE_SQL, (bib, pagina)).fetchone()
        return dict(row) if row else None

    def _get_entity_basic(self, conn: sqlite3.Connection, entity_id: int) -> dict | None:
        row = conn.execute(
            f"""
            SELECT
                e.id,
                e.type,
                e.canonical_name,
                e.normalized_name,
                e.base_normalized_name,
                e.aliases_json,
                e.attributes_json,
                e.first_seen_year,
                e.last_seen_year,
                {_entity_effective_status_sql('e')} AS identity_review_status,
                COUNT(DISTINCT m.id) AS mentions,
                MAX(p.ano) AS last_year
            FROM entities e
            LEFT JOIN entity_mentions m ON m.entity_id = e.id
            LEFT JOIN pages p ON p.id = m.page_id
            WHERE e.id = ?
            GROUP BY e.id
            """,
            (entity_id,),
        ).fetchone()
        return dict(row) if row else None

    def _get_primary_roles_for_entities(self, conn: sqlite3.Connection, entity_ids: list[int]) -> dict[int, dict]:
        if not entity_ids:
            return {}

        placeholders = ",".join("?" for _ in entity_ids)
        rows = rows_to_dicts(
            conn.execute(
                f"""
                SELECT
                    r.subject_entity_id AS entity_id,
                    COALESCE(object_entity.canonical_name, r.object_literal) AS role,
                    COUNT(re.id) AS evidences,
                    AVG(COALESCE(re.confidence, r.confidence)) AS confidence
                FROM relations r
                LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                LEFT JOIN relation_evidence re ON re.relation_id = r.id
                WHERE r.predicate = 'holds_role'
                  AND r.subject_entity_id IN ({placeholders})
                GROUP BY r.subject_entity_id, role
                ORDER BY r.subject_entity_id ASC, evidences DESC, confidence DESC, role ASC
                """,
                entity_ids,
            ).fetchall()
        )

        best_roles: dict[int, dict] = {}
        for row in rows:
            entity_id = int(row.get("entity_id") or 0)
            role = _compact_whitespace(row.get("role"))
            if not entity_id or not role or entity_id in best_roles:
                continue
            best_roles[entity_id] = {
                "role": role,
                "evidences": int(row.get("evidences") or 0),
                "confidence": round(float(row.get("confidence") or 0.0), 2),
            }
        return best_roles

    def _get_entity_merge_candidates(
        self,
        conn: sqlite3.Connection,
        entity_id: int,
        *,
        limit: int = 5,
    ) -> list[dict]:
        entity = self._get_entity_basic(conn, entity_id)
        if not entity:
            return []

        base_name = entity.get("base_normalized_name") or normalize_name(entity.get("canonical_name", ""))
        canonical_name = entity.get("canonical_name", "")

        rows = rows_to_dicts(
            conn.execute(
                f"""
                SELECT
                    e.id,
                    e.type,
                    e.canonical_name,
                    e.base_normalized_name,
                    e.aliases_json,
                    e.attributes_json,
                    {_entity_effective_status_sql('e')} AS identity_review_status,
                    COUNT(DISTINCT m.id) AS mentions,
                    MAX(p.ano) AS last_year
                FROM entities e
                LEFT JOIN entity_mentions m ON m.entity_id = e.id
                LEFT JOIN pages p ON p.id = m.page_id
                WHERE e.id != ?
                  AND e.type = ?
                  AND {_active_entity_condition_sql('e')}
                  AND (
                    e.base_normalized_name = ?
                    OR e.base_normalized_name LIKE ?
                    OR ? LIKE '%' || e.base_normalized_name || '%'
                    OR e.canonical_name LIKE ?
                    OR e.aliases_json LIKE ?
                  )
                GROUP BY e.id
                ORDER BY mentions DESC, e.canonical_name ASC
                LIMIT ?
                """,
                (
                    entity_id,
                    entity["type"],
                    base_name,
                    f"%{base_name}%",
                    base_name,
                    f"%{canonical_name}%",
                    f"%{canonical_name}%",
                    limit * 6,
                ),
            ).fetchall()
        )

        ranked = []
        for row in rows:
            scored = _rank_merge_candidate_from_rows(entity, row)
            if scored:
                ranked.append(scored)

        ranked.sort(key=lambda item: (item["score"], item["mentions"], item["canonical_name"]), reverse=True)
        return ranked[:limit]

    def get_entity_merge_candidates(self, entity_id: int, limit: int = 5) -> list[dict]:
        with self.connect() as conn:
            return self._get_entity_merge_candidates(conn, entity_id, limit=limit)

    def get_entity_noise_assessment(self, entity_id: int) -> dict:
        with self.connect() as conn:
            entity = self._get_entity_basic(conn, entity_id)
        if not entity:
            return {"score": 0.0, "reasons": [], "is_probable_noise": False}
        attrs = _loads_json(entity.get("attributes_json"), {})
        return assess_entity_noise(
            entity_type=entity.get("type", ""),
            canonical_name=entity.get("canonical_name", ""),
            attributes=attrs,
        )

    def _get_entity_merge_review(
        self,
        conn: sqlite3.Connection,
        source_entity_id: int,
        target_entity_id: int,
    ) -> dict | None:
        row = conn.execute(
            GET_ENTITY_MERGE_REVIEW_SQL,
            (source_entity_id, target_entity_id),
        ).fetchone()
        return dict(row) if row else None

    def review_entity_merge_suggestion(
        self,
        source_entity_id: int,
        target_entity_id: int,
        *,
        review_status: str,
        reviewer: str = "humano",
        note: str = "",
    ) -> dict | None:
        if source_entity_id == target_entity_id:
            return None

        with self.connect() as conn:
            source = self._get_entity_basic(conn, source_entity_id)
            target = self._get_entity_basic(conn, target_entity_id)
            if not source or not target:
                return None
            if source["type"] != target["type"]:
                return None
            conn.execute(
                INSERT_ENTITY_MERGE_REVIEW_SQL,
                (source_entity_id, target_entity_id, review_status, reviewer, note),
            )
            return self._get_entity_merge_review(conn, source_entity_id, target_entity_id)

    def _build_merge_review_queue(
        self,
        conn: sqlite3.Connection,
        *,
        source_rows: list[dict],
        limit: int = 12,
    ) -> list[dict]:
        if not source_rows:
            return []

        entity_rows = list(source_rows)
        missing_entity_ids = []
        seen_entity_ids = set()
        for item in entity_rows:
            if item and item.get("id") is not None:
                seen_entity_ids.add(int(item["id"]))
        for item in entity_rows:
            if item.get("id") is None:
                continue
            if int(item["id"]) not in seen_entity_ids:
                missing_entity_ids.append(int(item["id"]))
        for entity_id in missing_entity_ids:
            entity = self._get_entity_basic(conn, entity_id)
            if entity:
                entity_rows.append(entity)

        # Precisamos também de possíveis alvos fortes, mesmo se não estiverem na fila.
        universe_rows = rows_to_dicts(
            conn.execute(
                f"""
                SELECT
                    e.id,
                    e.type,
                    e.canonical_name,
                    e.aliases_json,
                    e.attributes_json,
                    {_entity_effective_status_sql('e')} AS identity_review_status,
                    COUNT(DISTINCT m.id) AS mentions,
                    MAX(p.ano) AS last_year
                FROM entities e
                LEFT JOIN entity_mentions m ON m.entity_id = e.id
                LEFT JOIN pages p ON p.id = m.page_id
                WHERE e.type = 'person'
                  AND {_active_entity_condition_sql('e')}
                GROUP BY e.id
                HAVING mentions > 0
                ORDER BY mentions DESC, e.canonical_name ASC
                LIMIT ?
                """,
                (max(limit * 12, 80),),
            ).fetchall()
        )
        entity_by_id = {int(item["id"]): item for item in universe_rows}
        for item in entity_rows:
            entity_by_id.setdefault(int(item["id"]), item)

        proposals: list[dict] = []
        seen_pairs: set[tuple[int, int]] = set()

        for entity in entity_rows:
            entity_id = int(entity["id"])
            ranked_candidates = []
            for candidate_row in universe_rows:
                scored = _rank_merge_candidate_from_rows(entity, candidate_row)
                if scored:
                    ranked_candidates.append(scored)
            ranked_candidates.sort(
                key=lambda item: (item["score"], item["mentions"], item["canonical_name"]),
                reverse=True,
            )
            for candidate in ranked_candidates[:3]:
                candidate_full = entity_by_id.get(int(candidate["id"])) or self._get_entity_basic(conn, int(candidate["id"]))
                if not candidate_full:
                    continue
                source, target = _choose_merge_direction(entity, candidate_full)
                source_id = int(source["id"])
                target_id = int(target["id"])
                if source_id == target_id:
                    continue
                pair = (source_id, target_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                score = float(candidate.get("score") or 0.0)
                if score < 5.0:
                    continue

                latest_review = self._get_entity_merge_review(conn, source_id, target_id)
                if latest_review and latest_review.get("review_status") in {"rejected", "approved"}:
                    continue

                source_noise = assess_entity_noise(
                    entity_type=source.get("type", ""),
                    canonical_name=source.get("canonical_name", ""),
                    attributes=_loads_json(source.get("attributes_json"), {}),
                )

                proposals.append(
                    {
                        "source_id": source_id,
                        "target_id": target_id,
                        "source_name": source["canonical_name"],
                        "target_name": target["canonical_name"],
                        "source_mentions": int(source.get("mentions") or 0),
                        "target_mentions": int(target.get("mentions") or 0),
                        "source_last_year": source.get("last_year"),
                        "target_last_year": target.get("last_year"),
                        "score": round(score, 2),
                        "reasons": candidate.get("reasons", []),
                        "source_noise_assessment": source_noise,
                        "review": latest_review,
                    }
                )

        proposals.sort(
            key=lambda item: (
                item["score"],
                item["source_noise_assessment"]["score"],
                item["target_mentions"],
                item["source_mentions"],
            ),
            reverse=True,
        )
        return proposals[:limit]

    def get_merge_review_queue(self, limit: int = 12) -> list[dict]:
        with self.connect() as conn:
            source_rows = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        e.id,
                        e.type,
                        e.canonical_name,
                        e.aliases_json,
                        e.attributes_json,
                        {_entity_effective_status_sql('e')} AS identity_review_status,
                        COUNT(DISTINCT m.id) AS mentions,
                        MAX(p.ano) AS last_year
                    FROM entities e
                    LEFT JOIN entity_mentions m ON m.entity_id = e.id
                    LEFT JOIN pages p ON p.id = m.page_id
                    WHERE e.type = 'person'
                      AND {_active_entity_condition_sql('e')}
                      AND (
                        e.attributes_json LIKE '%"identity_status": "ambiguous"%'
                        OR e.attributes_json LIKE '%"identity_status": "contextual"%'
                      )
                    GROUP BY e.id
                    HAVING mentions > 0
                    ORDER BY mentions DESC, e.canonical_name ASC
                    LIMIT ?
                    """,
                    (max(limit * 6, 36),),
                ).fetchall()
            )
            return self._build_merge_review_queue(conn, source_rows=source_rows, limit=limit)

    def get_discovery_overview(
        self,
        *,
        bib: str | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        limit: int = 8,
    ) -> dict:
        year_from = _parse_year(year_from)
        year_to = _parse_year(year_to)

        with self.connect() as conn:
            page_where_sql, page_params = _build_page_scope_sql(
                page_alias="p",
                bib=bib,
                year_from=year_from,
                year_to=year_to,
            )
            year_expr = _year_sql("p")

            scope_pages = conn.execute(
                f"SELECT COUNT(*) AS total FROM pages p {page_where_sql}",
                page_params,
            ).fetchone()["total"]
            scope_mentions = conn.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM entity_mentions m
                JOIN pages p ON p.id = m.page_id
                {page_where_sql}
                """,
                page_params,
            ).fetchone()["total"]

            timeline_rows = conn.execute(
                f"""
                SELECT
                    {year_expr} AS year,
                    COUNT(DISTINCT p.id) AS pages,
                    COUNT(m.id) AS mentions
                FROM pages p
                LEFT JOIN entity_mentions m ON m.page_id = p.id
                {page_where_sql}
                GROUP BY year
                HAVING year IS NOT NULL
                ORDER BY year ASC
                """,
                page_params,
            ).fetchall()

            journals = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        p.bib,
                        p.jornal,
                        COUNT(DISTINCT p.id) AS pages,
                        COUNT(m.id) AS mentions
                    FROM pages p
                    LEFT JOIN entity_mentions m ON m.page_id = p.id
                    {page_where_sql}
                    GROUP BY p.bib, p.jornal
                    ORDER BY mentions DESC, pages DESC, p.jornal ASC
                    LIMIT ?
                    """,
                    [*page_params, limit],
                ).fetchall()
            )
            journals = [
                {
                    **item,
                    "jornal": _journal_display_name(item.get("jornal"), item.get("bib")),
                }
                for item in journals
            ]

            def _top_entities(
                entity_type: str,
                *,
                custom_page_where_sql: str | None = None,
                custom_page_params: list | None = None,
            ) -> list[dict]:
                fetch_limit = limit * 6
                scoped_where_sql = custom_page_where_sql if custom_page_where_sql is not None else page_where_sql
                scoped_page_params = custom_page_params if custom_page_params is not None else page_params

                # Usar cache quando não há filtros de escopo
                use_cache = (not scoped_where_sql and not scoped_page_params)
                if use_cache:
                    self._ensure_stats_cache(conn)
                    rows = rows_to_dicts(
                        conn.execute(
                            """
                            SELECT sc.entity_id AS id, sc.canonical_name, sc.entity_type AS type,
                                   e.attributes_json, sc.mentions, sc.first_year, sc.last_year
                            FROM entity_stats_cache sc
                            JOIN entities e ON e.id = sc.entity_id
                            WHERE sc.entity_type = ?
                            ORDER BY sc.mentions DESC, sc.canonical_name ASC
                            LIMIT ?
                            """,
                            (entity_type, fetch_limit),
                        ).fetchall()
                    )
                else:
                    rows = rows_to_dicts(
                        conn.execute(
                            f"""
                            SELECT e.id, e.canonical_name, e.type, e.attributes_json,
                                   COUNT(m.id) AS mentions,
                                   MIN({year_expr}) AS first_year, MAX({year_expr}) AS last_year
                            FROM entity_mentions m
                            JOIN entities e ON e.id = m.entity_id
                            JOIN pages p ON p.id = m.page_id
                            {_extend_where(scoped_where_sql, f"e.type = ? AND {_active_entity_condition_sql('e')}")}
                            GROUP BY e.id
                            ORDER BY mentions DESC, e.canonical_name ASC
                            LIMIT ?
                            """,
                            [*scoped_page_params, entity_type, fetch_limit],
                        ).fetchall()
                    )
                filtered = []
                for row in rows:
                    attrs = _loads_json(row.get("attributes_json"), {})
                    noise = assess_discovery_noise(
                        entity_type=row.get("type", ""),
                        canonical_name=row.get("canonical_name", ""),
                        attributes=attrs,
                    )
                    if noise["is_probable_noise"]:
                        continue
                    row.pop("attributes_json", None)
                    filtered.append(row)
                    if len(filtered) >= limit:
                        break

                if entity_type == "person" and filtered:
                    role_map = self._get_primary_roles_for_entities(conn, [int(item["id"]) for item in filtered])
                    for item in filtered:
                        role_info = role_map.get(int(item["id"]), {})
                        if role_info.get("role"):
                            item["role"] = role_info["role"]
                            item["role_evidences"] = int(role_info.get("evidences") or 0)
                return filtered

            roles = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        COALESCE(object_entity.canonical_name, r.object_literal) AS role,
                        COUNT(re.id) AS evidences,
                        MAX({year_expr}) AS last_year
                    FROM relations r
                    LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                    JOIN relation_evidence re ON re.relation_id = r.id
                    JOIN pages p ON p.id = re.page_id
                    {_extend_where(page_where_sql, "r.predicate = 'holds_role'")}
                    GROUP BY role
                    ORDER BY evidences DESC, role ASC
                    LIMIT ?
                    """,
                    [*page_params, limit],
                ).fetchall()
            )

            family_relation = conn.execute(
                f"""
                SELECT
                    subject.canonical_name AS left_name,
                    COALESCE(object_entity.canonical_name, r.object_literal) AS right_name,
                    r.predicate,
                    COUNT(re.id) AS evidences,
                    MIN({year_expr}) AS first_year,
                    MAX({year_expr}) AS last_year
                FROM relations r
                JOIN entities subject ON subject.id = r.subject_entity_id
                LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                LEFT JOIN relation_evidence re ON re.relation_id = r.id
                LEFT JOIN pages p ON p.id = re.page_id
                {_extend_where(page_where_sql, "r.predicate IN ('child_of', 'spouse_of', 'parent_of', 'widow_of')")}
                GROUP BY r.id
                ORDER BY evidences DESC, r.id ASC
                LIMIT 1
                """,
                page_params,
            ).fetchone()

            co_mention_candidates = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        subject.canonical_name AS left_name,
                        object_entity.canonical_name AS right_name,
                        COUNT(DISTINCT re.page_id) AS shared_pages,
                        COUNT(re.id) AS evidences,
                        MIN({year_expr}) AS first_year,
                        MAX({year_expr}) AS last_year
                    FROM relations r
                    JOIN entities subject ON subject.id = r.subject_entity_id
                    JOIN entities object_entity ON object_entity.id = r.object_entity_id
                    JOIN relation_evidence re ON re.relation_id = r.id
                    JOIN pages p ON p.id = re.page_id
                    {_extend_where(page_where_sql, "r.predicate = 'mentioned_with' AND r.object_entity_id IS NOT NULL AND r.subject_entity_id < r.object_entity_id")}
                    GROUP BY subject.id, object_entity.id
                    ORDER BY shared_pages DESC, evidences DESC, subject.canonical_name ASC, object_entity.canonical_name ASC
                    LIMIT ?
                    """,
                    [*page_params, max(limit * 3, 12)],
                ).fetchall()
            )
            co_mention_pair = None
            for item in co_mention_candidates:
                if assess_discovery_noise(entity_type="person", canonical_name=item.get("left_name", ""))["is_probable_noise"]:
                    continue
                if assess_discovery_noise(entity_type="person", canonical_name=item.get("right_name", ""))["is_probable_noise"]:
                    continue
                co_mention_pair = item
                break

            hotspot_rows = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        p.id AS page_id,
                        p.bib,
                        p.pagina,
                        p.jornal,
                        p.ano,
                        p.edicao,
                        (
                            SELECT COUNT(*)
                            FROM entity_mentions m
                            WHERE m.page_id = p.id
                        ) AS mention_count,
                        (
                            SELECT COUNT(DISTINCT m.entity_id)
                            FROM entity_mentions m
                            WHERE m.page_id = p.id
                        ) AS distinct_entities,
                        (
                            SELECT COUNT(DISTINCT re.relation_id)
                            FROM relation_evidence re
                            WHERE re.page_id = p.id
                        ) AS relation_count
                    FROM pages p
                    {page_where_sql}
                    ORDER BY distinct_entities DESC, mention_count DESC, relation_count DESC, p.id ASC
                    LIMIT ?
                    """,
                    [*page_params, limit * 4],
                ).fetchall()
            )

            hotspots = []
            for row in hotspot_rows:
                focus_entities = rows_to_dicts(
                    conn.execute(
                        f"""
                        SELECT
                            e.id,
                            e.canonical_name,
                            e.type,
                            COUNT(*) AS mentions
                        FROM entity_mentions m
                        JOIN entities e ON e.id = m.entity_id
                        WHERE m.page_id = ? AND {_active_entity_condition_sql('e')}
                        GROUP BY e.id
                        ORDER BY mentions DESC, e.canonical_name ASC
                        LIMIT 6
                        """,
                        (row["page_id"],),
                    ).fetchall()
                )
                spotlight = conn.execute(
                    """
                    SELECT snippet
                    FROM entity_mentions
                    WHERE page_id = ? AND LENGTH(snippet) > 20
                    ORDER BY confidence DESC, id ASC
                    LIMIT 1
                    """,
                    (row["page_id"],),
                ).fetchone()
                spotlight_snippet = spotlight["snippet"] if spotlight else ""
                if len((spotlight_snippet or "").strip()) < 40:
                    continue
                hotspot = dict(row)
                # Filtrar entidades ruidosas dos hotspots.
                clean_focus = [
                    e for e in focus_entities
                    if not assess_discovery_noise(
                        entity_type=e.get("type", ""),
                        canonical_name=e.get("canonical_name", ""),
                    )["is_probable_noise"]
                ]
                if len(clean_focus) < 2:
                    continue
                hotspot["focus_entities"] = clean_focus[:4]
                hotspot["headline"] = ", ".join(item["canonical_name"] for item in clean_focus[:3]) or f"{row['bib']} / {row['pagina']}"
                hotspot["snippet"] = spotlight_snippet
                hotspots.append(hotspot)
                if len(hotspots) >= limit:
                    break

            top_people = _top_entities("person")
            top_places = _top_entities("place")
            top_institutions = _top_entities("institution")
            timeline = [
                {
                    "year": int(row["year"]),
                    "pages": int(row["pages"]),
                    "mentions": int(row["mentions"]),
                }
                for row in rows_to_dicts(timeline_rows)
            ]
            peak_years = sorted(
                timeline,
                key=lambda item: (item["mentions"], item["pages"]),
                reverse=True,
            )[: min(5, limit)]

            temporal_spike = None
            for previous, current in zip(timeline, timeline[1:]):
                if previous["mentions"] <= 0:
                    continue
                ratio = current["mentions"] / previous["mentions"]
                if ratio < 1.5:
                    continue
                candidate = {
                    "year": current["year"],
                    "mentions": current["mentions"],
                    "previous_mentions": previous["mentions"],
                    "ratio": ratio,
                }
                if (
                    temporal_spike is None
                    or candidate["ratio"] > temporal_spike["ratio"]
                    or (
                        candidate["ratio"] == temporal_spike["ratio"]
                        and candidate["mentions"] > temporal_spike["mentions"]
                    )
                ):
                    temporal_spike = candidate

            target_year = None
            if year_from is not None and year_to is not None and year_from == year_to:
                target_year = year_from
            elif year_from is not None and year_to is None:
                target_year = year_from
            elif year_to is not None and year_from is None:
                target_year = year_to
            elif peak_years:
                target_year = int(peak_years[0]["year"])
            elif timeline:
                target_year = int(timeline[0]["year"])

            period_snapshot = None
            if target_year is not None:
                year_scope_sql, year_scope_params = _build_page_scope_sql(
                    page_alias="p",
                    bib=bib,
                    year_from=target_year,
                    year_to=target_year,
                )
                period_pages = conn.execute(
                    f"SELECT COUNT(*) AS total FROM pages p {year_scope_sql}",
                    year_scope_params,
                ).fetchone()["total"]
                period_mentions = conn.execute(
                    f"""
                    SELECT COUNT(*) AS total
                    FROM entity_mentions m
                    JOIN pages p ON p.id = m.page_id
                    {year_scope_sql}
                    """,
                    year_scope_params,
                ).fetchone()["total"]
                period_people = _top_entities(
                    "person",
                    custom_page_where_sql=year_scope_sql,
                    custom_page_params=year_scope_params,
                )
                period_institutions = _top_entities(
                    "institution",
                    custom_page_where_sql=year_scope_sql,
                    custom_page_params=year_scope_params,
                )
                period_snapshot = {
                    "year": int(target_year),
                    "title": f"Pernambuco, {target_year}",
                    "pages": int(period_pages),
                    "mentions": int(period_mentions),
                    "top_people": period_people[:3],
                    "top_institutions": period_institutions[:3],
                }

            lead_person = top_people[0] if top_people else None
            lead_place = top_places[0] if top_places else None
            lead_institution = top_institutions[0] if top_institutions else None
            family_relation_payload = dict(family_relation) if family_relation else None

            research_questions: list[str] = []
            if lead_person:
                period_label = _format_period(lead_person.get("first_year"), lead_person.get("last_year"))
                role_fragment = f" como {lead_person['role']}" if lead_person.get("role") else ""
                period_fragment = f" entre {period_label}" if period_label else ""
                _append_question(
                    research_questions,
                    (
                        f"Quem era {lead_person['canonical_name']}, citado {_format_count(lead_person['mentions'])} vezes"
                        f"{role_fragment}{period_fragment}?"
                    ),
                    limit=6,
                )
            if family_relation_payload:
                family_period = _format_period(
                    family_relation_payload.get("first_year"),
                    family_relation_payload.get("last_year"),
                )
                _append_question(
                    research_questions,
                    (
                        f"Qual a relação entre {family_relation_payload['left_name']} e "
                        f"{family_relation_payload['right_name']}"
                        f"{f' nos jornais de {family_period}' if family_period else ' nos jornais deste recorte'}?"
                    ),
                    limit=6,
                )
            if lead_institution:
                institution_year = (
                    target_year
                    or _parse_year(lead_institution.get("last_year"))
                    or _parse_year(lead_institution.get("first_year"))
                )
                _append_question(
                    research_questions,
                    (
                        f"O que {lead_institution['canonical_name']} representava em {institution_year}?"
                        if institution_year is not None
                        else f"O que {lead_institution['canonical_name']} representava neste recorte?"
                    ),
                    limit=6,
                )
            if temporal_spike:
                _append_question(
                    research_questions,
                    (
                        f"O que mudou em {temporal_spike['year']} para gerar {_format_count(temporal_spike['mentions'])} "
                        f"menções, contra {_format_count(temporal_spike['previous_mentions'])} no ano anterior?"
                    ),
                    limit=6,
                )
            elif peak_years:
                _append_question(
                    research_questions,
                    (
                        f"O que aconteceu em {peak_years[0]['year']} para concentrar "
                        f"{_format_count(peak_years[0]['mentions'])} menções?"
                    ),
                    limit=6,
                )
            if co_mention_pair:
                _append_question(
                    research_questions,
                    (
                        f"{co_mention_pair['left_name']} e {co_mention_pair['right_name']} aparecem juntos em "
                        f"{_format_count(co_mention_pair['shared_pages'])} páginas. Qual a conexão?"
                    ),
                    limit=6,
                )
            if hotspots:
                hotspot = hotspots[0]
                _append_question(
                    research_questions,
                    (
                        f"Que episódio documental reúne {hotspot['headline']} na página "
                        f"{hotspot['bib']} / {hotspot['pagina']}?"
                    ),
                    limit=6,
                )
            if lead_person and lead_place:
                _append_question(
                    research_questions,
                    (
                        f"Como {lead_person['canonical_name']} aparece ligado a {lead_place['canonical_name']} "
                        f"nos jornais deste recorte?"
                    ),
                    limit=6,
                )

            top_roles = [
                {
                    **item,
                    "evidences": int(item.get("evidences") or 0),
                    "last_year": _parse_year(item.get("last_year")),
                }
                for item in rows_to_dicts(roles)
            ]

            return {
                "scope": {
                    "bib": bib,
                    "year_from": year_from,
                    "year_to": year_to,
                    "pages": int(scope_pages),
                    "mentions": int(scope_mentions),
                },
                "timeline": timeline,
                "peak_years": peak_years,
                "top_people": top_people,
                "top_places": top_places,
                "top_institutions": top_institutions,
                "top_roles": top_roles,
                "top_journals": journals,
                "period_snapshot": period_snapshot,
                "document_hotspots": hotspots,
                "research_questions": research_questions[:6],
            }

    # -- Trilhas temáticas -------------------------------------------------

    _TRAIL_DEFINITIONS: dict[str, dict] = {
        "governo": {
            "title": "Poder e Governo",
            "subtitle": "Autoridades provinciais, câmaras e cargos públicos no Pernambuco imperial.",
            "institution_filter": ["governo", "camara", "câmara", "presidencia", "presidência", "secretaria"],
            "role_filter": ["Coronel", "Major", "Tenente", "Capitão", "Conselheiro", "Desembargador"],
            "icon": "governo",
        },
        "comercio": {
            "title": "Comércio e Porto",
            "subtitle": "Alfândega, companhias comerciais e embarcações no cotidiano portuário.",
            "institution_filter": ["alfandega", "alfândega", "companhia", "comp", "consulado"],
            "role_filter": [],
            "entity_keywords": ["porto", "alfandega", "alfândega", "sumaca", "escuna", "navio"],
            "icon": "comercio",
        },
        "igreja": {
            "title": "Igreja e Clero",
            "subtitle": "Padres, vigários e a presença da Igreja nos registros de época.",
            "institution_filter": ["igreja"],
            "role_filter": ["Padre", "Vigário"],
            "icon": "igreja",
        },
        "geografia": {
            "title": "Geografia do Poder",
            "subtitle": "Lugares, vilas e comarcas que ancoram eventos e pessoas.",
            "institution_filter": [],
            "role_filter": [],
            "entity_type_focus": "place",
            "icon": "geografia",
        },
    }

    def get_trail(self, trail_name: str, *, limit: int = 8) -> dict | None:
        defn = self._TRAIL_DEFINITIONS.get(trail_name)
        if not defn:
            return None

        with self.connect() as conn:
            year_expr = _year_sql("p")

            # Instituições do tema
            institutions: list[dict] = []
            if defn.get("institution_filter"):
                like_clauses = " OR ".join(
                    "LOWER(e.canonical_name) LIKE ?" for _ in defn["institution_filter"]
                )
                params = [f"%{kw}%" for kw in defn["institution_filter"]]
                raw_institutions = rows_to_dicts(
                    conn.execute(
                        f"""
                        SELECT e.id, e.canonical_name, e.type,
                               COUNT(m.id) AS mentions,
                               MIN({year_expr}) AS first_year,
                               MAX({year_expr}) AS last_year
                        FROM entities e
                        JOIN entity_mentions m ON m.entity_id = e.id
                        JOIN pages p ON p.id = m.page_id
                        WHERE e.type = 'institution'
                          AND ({like_clauses})
                          AND {_active_entity_condition_sql('e')}
                        GROUP BY e.id
                        ORDER BY mentions DESC
                        LIMIT ?
                        """,
                        [*params, limit * 3],
                    ).fetchall()
                )
                for row in raw_institutions:
                    noise = assess_discovery_noise(
                        entity_type="institution",
                        canonical_name=row.get("canonical_name", ""),
                    )
                    if not noise["is_probable_noise"]:
                        institutions.append(row)
                    if len(institutions) >= limit:
                        break

            # Pessoas associadas ao tema (por cargo ou co-menção com as instituições)
            people: list[dict] = []
            if defn.get("role_filter"):
                role_placeholders = ",".join("?" for _ in defn["role_filter"])
                raw_people = rows_to_dicts(
                    conn.execute(
                        f"""
                        SELECT e.id, e.canonical_name, e.type,
                               COALESCE(obj_e.canonical_name, r.object_literal) AS role,
                               COUNT(DISTINCT m.id) AS mentions,
                               MIN({year_expr}) AS first_year,
                               MAX({year_expr}) AS last_year
                        FROM relations r
                        JOIN entities e ON e.id = r.subject_entity_id
                        LEFT JOIN entities obj_e ON obj_e.id = r.object_entity_id
                        JOIN entity_mentions m ON m.entity_id = e.id
                        JOIN pages p ON p.id = m.page_id
                        WHERE r.predicate = 'holds_role'
                          AND COALESCE(obj_e.canonical_name, r.object_literal) IN ({role_placeholders})
                          AND {_active_entity_condition_sql('e')}
                        GROUP BY e.id
                        ORDER BY mentions DESC
                        LIMIT ?
                        """,
                        [*defn["role_filter"], limit * 3],
                    ).fetchall()
                )
                for row in raw_people:
                    if _is_legible_name(row.get("canonical_name", "")):
                        people.append(row)
                    if len(people) >= limit:
                        break
            elif defn.get("entity_type_focus") == "place":
                people_rows = rows_to_dicts(
                    conn.execute(
                        f"""
                        SELECT e.id, e.canonical_name, e.type,
                               COUNT(m.id) AS mentions,
                               MIN({year_expr}) AS first_year,
                               MAX({year_expr}) AS last_year
                        FROM entities e
                        JOIN entity_mentions m ON m.entity_id = e.id
                        JOIN pages p ON p.id = m.page_id
                        WHERE e.type = 'place'
                          AND {_active_entity_condition_sql('e')}
                        GROUP BY e.id
                        ORDER BY mentions DESC
                        LIMIT ?
                        """,
                        (limit * 3,),
                    ).fetchall()
                )
                for row in people_rows:
                    noise = assess_discovery_noise(
                        entity_type="place",
                        canonical_name=row.get("canonical_name", ""),
                    )
                    if not noise["is_probable_noise"]:
                        people.append(row)
                    if len(people) >= limit:
                        break

            # Páginas representativas do tema
            hotspots: list[dict] = []
            entity_ids = [int(e["id"]) for e in [*institutions[:3], *people[:3]] if e.get("id")]
            if entity_ids:
                id_placeholders = ",".join("?" for _ in entity_ids)
                hotspots = rows_to_dicts(
                    conn.execute(
                        f"""
                        SELECT DISTINCT p.bib, p.pagina, p.jornal, p.ano,
                               COUNT(DISTINCT m.entity_id) AS entity_count
                        FROM entity_mentions m
                        JOIN pages p ON p.id = m.page_id
                        WHERE m.entity_id IN ({id_placeholders})
                        GROUP BY p.id
                        ORDER BY entity_count DESC
                        LIMIT ?
                        """,
                        [*entity_ids, limit],
                    ).fetchall()
                )

            # Perguntas temáticas
            questions: list[str] = []
            is_geo = defn.get("entity_type_focus") == "place"
            if is_geo and people:
                questions.append(
                    f"Que eventos os jornais registram em {people[0]['canonical_name']}?"
                )
                if len(people) >= 2:
                    questions.append(
                        f"Qual a relação entre {people[0]['canonical_name']} e "
                        f"{people[1]['canonical_name']} nos registros de época?"
                    )
            elif people and institutions:
                role_frag = f" como {people[0]['role']}" if people[0].get("role") else ""
                questions.append(
                    f"Qual o papel de {people[0]['canonical_name']}{role_frag} "
                    f"na {institutions[0]['canonical_name']}?"
                )
            if institutions and len(institutions) >= 2:
                questions.append(
                    f"Como {institutions[0]['canonical_name']} e {institutions[1]['canonical_name']} "
                    f"se relacionam nos jornais?"
                )
            if not is_geo and people and len(people) >= 2:
                questions.append(
                    f"Quem eram {people[0]['canonical_name']} e {people[1]['canonical_name']}?"
                )

        return {
            "trail": trail_name,
            "title": defn["title"],
            "subtitle": defn["subtitle"],
            "icon": defn.get("icon", trail_name),
            "institutions": institutions,
            "people": people,
            "hotspot_pages": hotspots,
            "research_questions": questions,
        }

    def list_trails(self) -> list[dict]:
        return [
            {
                "key": key,
                "title": defn["title"],
                "subtitle": defn["subtitle"],
                "icon": defn.get("icon", key),
            }
            for key, defn in self._TRAIL_DEFINITIONS.items()
        ]

    def get_family_tree(self, entity_id: int) -> dict:
        """Retorna árvore genealógica centrada em uma entidade."""
        with self.connect() as conn:
            entity = self._get_entity_basic(conn, entity_id)
            if not entity:
                return {"nodes": [], "edges": [], "center_id": entity_id}

            nodes: dict[int, dict] = {}
            edges: list[dict] = []
            visited: set[int] = set()

            def _add_entity(eid: int) -> dict | None:
                if eid in nodes:
                    return nodes[eid]
                row = conn.execute(
                    "SELECT id, canonical_name, type FROM entities WHERE id = ?", (eid,)
                ).fetchone()
                if not row:
                    return None
                # Buscar cargo
                role_row = conn.execute(
                    """SELECT COALESCE(o.canonical_name, r.object_literal) as role
                       FROM relations r LEFT JOIN entities o ON o.id = r.object_entity_id
                       WHERE r.predicate = 'holds_role' AND r.subject_entity_id = ?
                       ORDER BY r.confidence DESC LIMIT 1""",
                    (eid,),
                ).fetchone()
                mention_count = conn.execute(
                    "SELECT COUNT(*) FROM entity_mentions WHERE entity_id = ?", (eid,)
                ).fetchone()[0]
                node = {
                    "id": int(row["id"]),
                    "name": row["canonical_name"],
                    "type": row["type"],
                    "role": role_row["role"] if role_row else "",
                    "mentions": int(mention_count),
                    "central": eid == entity_id,
                }
                nodes[eid] = node
                return node

            def _expand(eid: int, depth: int = 0):
                if eid in visited or depth > 3:
                    return
                visited.add(eid)
                _add_entity(eid)

                # Relações familiares de/para esta entidade
                rels = conn.execute(
                    """SELECT r.id, r.subject_entity_id, r.predicate, r.object_entity_id, r.confidence
                       FROM relations r
                       WHERE r.predicate IN ('child_of','spouse_of','widow_of','parent_of')
                         AND (r.subject_entity_id = ? OR r.object_entity_id = ?)""",
                    (eid, eid),
                ).fetchall()

                for rel in rels:
                    sid = int(rel["subject_entity_id"])
                    oid = int(rel["object_entity_id"]) if rel["object_entity_id"] else None
                    if not oid:
                        continue
                    _add_entity(sid)
                    _add_entity(oid)

                    label_map = {
                        "child_of": "filho(a) de",
                        "parent_of": "pai/m\u00e3e de",
                        "spouse_of": "c\u00f4njuge de",
                        "widow_of": "vi\u00fava de",
                    }
                    edges.append({
                        "source": sid,
                        "target": oid,
                        "predicate": rel["predicate"],
                        "label": label_map.get(rel["predicate"], rel["predicate"]),
                    })

                    # Expandir parentes
                    other = oid if sid == eid else sid
                    if other not in visited:
                        _expand(other, depth + 1)

            _expand(entity_id)

            return {
                "nodes": list(nodes.values()),
                "edges": edges,
                "center_id": entity_id,
                "center_name": entity.get("canonical_name", ""),
            }

    def get_period_narrative(self, year: int, *, limit: int = 8) -> dict:
        """Retorna dados narrativos de um período específico."""
        with self.connect() as conn:
            year_expr = _year_sql("p")
            page_where = f"WHERE {year_expr} = ?"
            params = [year]

            pages = conn.execute(
                f"SELECT COUNT(*) AS total FROM pages p {page_where}", params
            ).fetchone()["total"]

            mentions = conn.execute(
                f"SELECT COUNT(*) AS total FROM entity_mentions m JOIN pages p ON p.id = m.page_id {page_where}",
                params,
            ).fetchone()["total"]

            # Top entidades por tipo neste ano
            def _top(entity_type: str):
                rows = rows_to_dicts(conn.execute(
                    f"""SELECT e.id, e.canonical_name, e.type, COUNT(m.id) AS mentions
                        FROM entity_mentions m
                        JOIN entities e ON e.id = m.entity_id
                        JOIN pages p ON p.id = m.page_id
                        {_extend_where(page_where, f"e.type = ? AND {_active_entity_condition_sql('e')}")}
                        GROUP BY e.id ORDER BY mentions DESC LIMIT ?""",
                    [*params, entity_type, limit * 2],
                ).fetchall())
                filtered = []
                for r in rows:
                    noise = assess_discovery_noise(
                        entity_type=r.get("type", ""),
                        canonical_name=r.get("canonical_name", ""),
                    )
                    if not noise["is_probable_noise"]:
                        filtered.append(r)
                    if len(filtered) >= limit:
                        break
                return filtered

            people = _top("person")
            places = _top("place")
            institutions = _top("institution")

            # Eventos/relações importantes do período
            events = rows_to_dicts(conn.execute(
                f"""SELECT r.predicate,
                       s.canonical_name AS subject, s.id AS subject_id,
                       COALESCE(o.canonical_name, r.object_literal) AS object,
                       re.quote
                    FROM relations r
                    JOIN entities s ON s.id = r.subject_entity_id
                    LEFT JOIN entities o ON o.id = r.object_entity_id
                    JOIN relation_evidence re ON re.relation_id = r.id
                    JOIN pages p ON p.id = re.page_id
                    WHERE r.predicate NOT IN ('mentioned_with', 'holds_role')
                      AND {year_expr} = ?
                    ORDER BY r.confidence DESC
                    LIMIT ?""",
                [year, limit],
            ).fetchall())

            # Jornais ativos no período
            journals = rows_to_dicts(conn.execute(
                f"""SELECT p.bib, p.jornal, COUNT(DISTINCT p.id) AS pages, COUNT(m.id) AS mentions
                    FROM pages p LEFT JOIN entity_mentions m ON m.page_id = p.id
                    {page_where} GROUP BY p.bib, p.jornal
                    ORDER BY mentions DESC LIMIT ?""",
                [*params, limit],
            ).fetchall())

            return {
                "year": year,
                "pages": pages,
                "mentions": mentions,
                "people": people,
                "places": places,
                "institutions": institutions,
                "events": events,
                "journals": journals,
            }

    def get_entity_story(self, entity_id: int, *, limit: int = 10) -> dict:
        with self.connect() as conn:
            year_expr = _year_sql("p")
            entity_row = conn.execute(GET_ENTITY_SQL, (entity_id,)).fetchone()
            entity_name = entity_row["canonical_name"] if entity_row else "Esta entidade"
            primary_role = self._get_primary_roles_for_entities(conn, [entity_id]).get(entity_id, {}).get("role")

            timeline = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        {year_expr} AS year,
                        COUNT(*) AS mentions
                    FROM entity_mentions m
                    JOIN pages p ON p.id = m.page_id
                    WHERE m.entity_id = ?
                    GROUP BY year
                    HAVING year IS NOT NULL
                    ORDER BY year ASC
                    """,
                    (entity_id,),
                ).fetchall()
            )

            journals = rows_to_dicts(
                conn.execute(
                    """
                    SELECT
                        p.bib,
                        p.jornal,
                        COUNT(*) AS mentions
                    FROM entity_mentions m
                    JOIN pages p ON p.id = m.page_id
                    WHERE m.entity_id = ?
                    GROUP BY p.bib, p.jornal
                    ORDER BY mentions DESC, p.jornal ASC
                    LIMIT ?
                    """,
                    (entity_id, limit),
                ).fetchall()
            )

            co_mentions = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        e.id AS entity_id,
                        e.canonical_name,
                        e.type,
                        COUNT(DISTINCT other.page_id) AS shared_pages,
                        MIN({year_expr}) AS first_year,
                        MAX({year_expr}) AS last_year
                    FROM entity_mentions anchor
                    JOIN entity_mentions other
                      ON other.page_id = anchor.page_id
                     AND other.entity_id != anchor.entity_id
                    JOIN entities e ON e.id = other.entity_id
                    JOIN pages p ON p.id = other.page_id
                    WHERE anchor.entity_id = ?
                      AND {_active_entity_condition_sql('e')}
                    GROUP BY e.id
                    ORDER BY shared_pages DESC, e.canonical_name ASC
                    LIMIT ?
                    """,
                    (entity_id, limit * 4),
                ).fetchall()
            )

            relation_highlights = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        r.id,
                        r.predicate,
                        CASE
                            WHEN r.subject_entity_id = ? THEN COALESCE(object_entity.canonical_name, r.object_literal)
                            ELSE subject.canonical_name
                        END AS counterpart_name,
                        CASE
                            WHEN r.subject_entity_id = ? THEN COALESCE(object_entity.type, CASE WHEN r.predicate = 'holds_role' THEN 'role' ELSE 'literal' END)
                            ELSE subject.type
                        END AS counterpart_type,
                        COUNT(re.id) AS evidences,
                        AVG(re.confidence) AS confidence,
                        MAX({year_expr}) AS last_year
                    FROM relations r
                    JOIN entities subject ON subject.id = r.subject_entity_id
                    LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                    LEFT JOIN relation_evidence re ON re.relation_id = r.id
                    LEFT JOIN pages p ON p.id = re.page_id
                    WHERE r.subject_entity_id = ? OR r.object_entity_id = ?
                    GROUP BY r.id
                    ORDER BY evidences DESC, confidence DESC, r.id ASC
                    LIMIT ?
                    """,
                    (entity_id, entity_id, entity_id, entity_id, limit),
                ).fetchall()
            )

            def _milestone(order_by: str) -> dict | None:
                row = conn.execute(
                    f"""
                    SELECT
                        m.surface_form,
                        m.snippet,
                        m.confidence,
                        p.bib,
                        p.pagina,
                        p.jornal,
                        p.ano,
                        p.edicao,
                        p.image_path,
                        {year_expr} AS year
                    FROM entity_mentions m
                    JOIN pages p ON p.id = m.page_id
                    WHERE m.entity_id = ?
                    ORDER BY {order_by}
                    LIMIT 1
                    """,
                    (entity_id,),
                ).fetchone()
                return dict(row) if row else None

            milestones = []
            first_doc = _milestone(f"({year_expr} IS NULL), {year_expr} ASC, p.id ASC, m.confidence DESC")
            last_doc = _milestone(f"({year_expr} IS NULL), {year_expr} DESC, p.id DESC, m.confidence DESC")
            strongest_doc = _milestone("m.confidence DESC, p.id ASC")

            if first_doc:
                first_doc["label"] = "Primeira aparição"
                milestones.append(first_doc)
            if last_doc:
                last_doc["label"] = "Última aparição"
                milestones.append(last_doc)
            if strongest_doc:
                strongest_doc["label"] = "Menção mais forte"
                milestones.append(strongest_doc)

        timeline = [
            {"year": int(item["year"]), "mentions": int(item["mentions"])}
            for item in timeline
            if item.get("year") is not None
        ]
        peak_years = sorted(timeline, key=lambda item: item["mentions"], reverse=True)[:3]

        connections = {"people": [], "places": [], "institutions": [], "other": []}
        for item in co_mentions:
            payload = {
                "entity_id": int(item["entity_id"]),
                "canonical_name": item["canonical_name"],
                "type": item["type"],
                "shared_pages": int(item["shared_pages"]),
                "first_year": item["first_year"],
                "last_year": item["last_year"],
            }
            if item["type"] == "person" and len(connections["people"]) < limit:
                connections["people"].append(payload)
            elif item["type"] == "place" and len(connections["places"]) < limit:
                connections["places"].append(payload)
            elif item["type"] == "institution" and len(connections["institutions"]) < limit:
                connections["institutions"].append(payload)
            elif len(connections["other"]) < limit:
                connections["other"].append(payload)

        research_questions: list[str] = []
        if primary_role and peak_years:
            entity_period = _format_period(
                timeline[0]["year"] if timeline else None,
                timeline[-1]["year"] if timeline else None,
            )
            _append_question(
                research_questions,
                (
                    f"Que atos de {entity_name} como {primary_role} aparecem nos jornais "
                    f"entre {entity_period or peak_years[0]['year']}?"
                ),
                limit=5,
            )
        if peak_years:
            _append_question(
                research_questions,
                (
                    f"Por que {entity_name} concentra {_format_count(peak_years[0]['mentions'])} "
                    f"menções em {peak_years[0]['year']}?"
                ),
                limit=5,
            )
        if connections["people"]:
            _append_question(
                research_questions,
                (
                    f"Qual a relação entre {entity_name} e {connections['people'][0]['canonical_name']} "
                    f"nas {connections['people'][0]['shared_pages']} páginas em que surgem juntos?"
                ),
                limit=5,
            )
        if connections["places"]:
            _append_question(
                research_questions,
                (
                    f"O que a presença de {entity_name} em {connections['places'][0]['canonical_name']} "
                    f"revela sobre sua atuação documental?"
                ),
                limit=5,
            )
        if connections["institutions"]:
            _append_question(
                research_questions,
                (
                    f"Como {entity_name} aparece ligado a {connections['institutions'][0]['canonical_name']} "
                    f"nos jornais disponíveis?"
                ),
                limit=5,
            )
        if len(journals) >= 2:
            _append_question(
                research_questions,
                (
                    f"Por que {entity_name} aparece tanto no {journals[0]['jornal']} "
                    f"quanto no {journals[1]['jornal']}?"
                ),
                limit=5,
            )
        elif relation_highlights:
            _append_question(
                research_questions,
                f"Quais relações estruturadas de {entity_name} merecem revisão prioritária neste dossiê?",
                limit=5,
            )

        return {
            "timeline": timeline,
            "peak_years": peak_years,
            "journals": journals,
            "connections": connections,
            "relation_highlights": relation_highlights,
            "milestones": milestones,
            "research_questions": research_questions[:5],
        }

    def get_entity_comparison(
        self,
        left_entity_id: int,
        right_entity_id: int,
        *,
        limit: int = 8,
    ) -> dict | None:
        if left_entity_id == right_entity_id:
            return None

        with self.connect() as conn:
            year_expr = _year_sql("p")

            def _load_entity_basic(entity_id: int) -> dict | None:
                row = conn.execute(
                    f"""
                    SELECT
                        e.id,
                        e.type,
                        e.canonical_name,
                        COUNT(m.id) AS mentions,
                        MIN({year_expr}) AS first_year,
                        MAX({year_expr}) AS last_year
                    FROM entities e
                    LEFT JOIN entity_mentions m ON m.entity_id = e.id
                    LEFT JOIN pages p ON p.id = m.page_id
                    WHERE e.id = ?
                    GROUP BY e.id
                    """,
                    (entity_id,),
                ).fetchone()
                return dict(row) if row else None

            left = _load_entity_basic(left_entity_id)
            right = _load_entity_basic(right_entity_id)
            if not left or not right:
                return None

            timeline_rows = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        {year_expr} AS year,
                        SUM(CASE WHEN m.entity_id = ? THEN 1 ELSE 0 END) AS left_mentions,
                        SUM(CASE WHEN m.entity_id = ? THEN 1 ELSE 0 END) AS right_mentions
                    FROM entity_mentions m
                    JOIN pages p ON p.id = m.page_id
                    WHERE m.entity_id IN (?, ?)
                    GROUP BY year
                    HAVING year IS NOT NULL
                    ORDER BY year ASC
                    """,
                    (left_entity_id, right_entity_id, left_entity_id, right_entity_id),
                ).fetchall()
            )

            shared_page_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT p.id
                    FROM pages p
                    JOIN entity_mentions ml ON ml.page_id = p.id AND ml.entity_id = ?
                    JOIN entity_mentions mr ON mr.page_id = p.id AND mr.entity_id = ?
                    GROUP BY p.id
                ) shared
                """,
                (left_entity_id, right_entity_id),
            ).fetchone()[0]

            shared_pages = rows_to_dicts(
                conn.execute(
                    """
                    SELECT
                        p.id AS page_id,
                        p.bib,
                        p.pagina,
                        p.jornal,
                        p.ano,
                        p.edicao,
                        (
                            SELECT surface_form
                            FROM entity_mentions
                            WHERE page_id = p.id AND entity_id = ?
                            ORDER BY confidence DESC, id ASC
                            LIMIT 1
                        ) AS left_surface,
                        (
                            SELECT surface_form
                            FROM entity_mentions
                            WHERE page_id = p.id AND entity_id = ?
                            ORDER BY confidence DESC, id ASC
                            LIMIT 1
                        ) AS right_surface,
                        (
                            SELECT snippet
                            FROM entity_mentions
                            WHERE page_id = p.id AND entity_id = ?
                            ORDER BY confidence DESC, id ASC
                            LIMIT 1
                        ) AS left_snippet,
                        (
                            SELECT snippet
                            FROM entity_mentions
                            WHERE page_id = p.id AND entity_id = ?
                            ORDER BY confidence DESC, id ASC
                            LIMIT 1
                        ) AS right_snippet,
                        COUNT(DISTINCT bridge.entity_id) AS bridge_entities
                    FROM pages p
                    JOIN entity_mentions ml ON ml.page_id = p.id AND ml.entity_id = ?
                    JOIN entity_mentions mr ON mr.page_id = p.id AND mr.entity_id = ?
                    LEFT JOIN entity_mentions bridge ON bridge.page_id = p.id AND bridge.entity_id NOT IN (?, ?)
                    GROUP BY p.id
                    ORDER BY bridge_entities DESC, p.id ASC
                    LIMIT ?
                    """,
                    (
                        left_entity_id,
                        right_entity_id,
                        left_entity_id,
                        right_entity_id,
                        left_entity_id,
                        right_entity_id,
                        left_entity_id,
                        right_entity_id,
                        limit,
                    ),
                ).fetchall()
            )

            journal_overlap = rows_to_dicts(
                conn.execute(
                    """
                    SELECT
                        p.bib,
                        p.jornal,
                        COUNT(DISTINCT p.id) AS shared_pages
                    FROM pages p
                    JOIN entity_mentions ml ON ml.page_id = p.id AND ml.entity_id = ?
                    JOIN entity_mentions mr ON mr.page_id = p.id AND mr.entity_id = ?
                    GROUP BY p.bib, p.jornal
                    ORDER BY shared_pages DESC, p.jornal ASC
                    LIMIT ?
                    """,
                    (left_entity_id, right_entity_id, limit),
                ).fetchall()
            )

            bridge_rows = rows_to_dicts(
                    conn.execute(
                    f"""
                    SELECT
                        e.id AS entity_id,
                        e.canonical_name,
                        e.type,
                        COUNT(DISTINCT p.id) AS shared_pages
                    FROM pages p
                    JOIN entity_mentions ml ON ml.page_id = p.id AND ml.entity_id = ?
                    JOIN entity_mentions mr ON mr.page_id = p.id AND mr.entity_id = ?
                    JOIN entity_mentions bridge ON bridge.page_id = p.id AND bridge.entity_id NOT IN (?, ?)
                    JOIN entities e ON e.id = bridge.entity_id AND {_active_entity_condition_sql('e')}
                    GROUP BY e.id
                    ORDER BY shared_pages DESC, e.canonical_name ASC
                    LIMIT ?
                    """,
                    (left_entity_id, right_entity_id, left_entity_id, right_entity_id, limit * 6),
                ).fetchall()
            )

            direct_relations = rows_to_dicts(
                conn.execute(
                    f"""
                    SELECT
                        r.id,
                        r.predicate,
                        r.confidence,
                        COALESCE(
                            (
                                SELECT rr.review_status
                                FROM relation_reviews rr
                                WHERE rr.relation_id = r.id
                                ORDER BY rr.id DESC
                                LIMIT 1
                            ),
                            r.status
                        ) AS effective_status,
                        COUNT(re.id) AS evidences,
                        MAX({year_expr}) AS last_year
                    FROM relations r
                    LEFT JOIN relation_evidence re ON re.relation_id = r.id
                    LEFT JOIN pages p ON p.id = re.page_id
                    WHERE (r.subject_entity_id = ? AND r.object_entity_id = ?)
                       OR (r.subject_entity_id = ? AND r.object_entity_id = ?)
                    GROUP BY r.id
                    ORDER BY evidences DESC, r.confidence DESC, r.id ASC
                    LIMIT ?
                    """,
                    (left_entity_id, right_entity_id, right_entity_id, left_entity_id, limit),
                ).fetchall()
            )

        timeline = []
        shared_years = []
        for row in timeline_rows:
            entry = {
                "year": int(row["year"]),
                "left_mentions": int(row["left_mentions"] or 0),
                "right_mentions": int(row["right_mentions"] or 0),
            }
            entry["shared_presence"] = entry["left_mentions"] > 0 and entry["right_mentions"] > 0
            if entry["shared_presence"]:
                shared_years.append(entry["year"])
            timeline.append(entry)

        strongest_overlap = None
        if timeline:
            strongest_overlap = max(
                timeline,
                key=lambda item: (min(item["left_mentions"], item["right_mentions"]), item["left_mentions"] + item["right_mentions"]),
            )

        bridges = {"people": [], "places": [], "institutions": [], "other": []}
        for item in bridge_rows:
            payload = {
                "entity_id": int(item["entity_id"]),
                "canonical_name": item["canonical_name"],
                "type": item["type"],
                "shared_pages": int(item["shared_pages"]),
            }
            if item["type"] == "person" and len(bridges["people"]) < limit:
                bridges["people"].append(payload)
            elif item["type"] == "place" and len(bridges["places"]) < limit:
                bridges["places"].append(payload)
            elif item["type"] == "institution" and len(bridges["institutions"]) < limit:
                bridges["institutions"].append(payload)
            elif len(bridges["other"]) < limit:
                bridges["other"].append(payload)

        research_questions: list[str] = []
        if shared_page_count:
            research_questions.append(
                f"Em quais episódios documentais {left['canonical_name']} e {right['canonical_name']} aparecem juntos?"
            )
        if shared_years:
            research_questions.append(
                f"O que estava acontecendo em {shared_years[0]} quando essas duas entidades coexistem no jornal?"
            )
        if bridges["institutions"]:
            research_questions.append(
                f"Qual o papel de {bridges['institutions'][0]['canonical_name']} na aproximação entre essas entidades?"
            )
        if bridges["places"]:
            research_questions.append(
                f"Como o espaço {bridges['places'][0]['canonical_name']} ajuda a explicar essa conexão?"
            )
        if direct_relations:
            research_questions.append(
                f"A relação direta extraída entre elas é sólida ou ainda precisa de revisão humana?"
            )
        if not shared_years and timeline:
            research_questions.append(
                f"Essas entidades ocupam momentos diferentes do acervo ou fazem parte de ciclos sucessivos?"
            )

        return {
            "left": left,
            "right": right,
            "timeline": timeline,
            "overlap": {
                "shared_pages": int(shared_page_count),
                "shared_years": shared_years,
                "shared_journals": journal_overlap,
                "strongest_overlap_year": strongest_overlap["year"] if strongest_overlap else None,
            },
            "shared_pages": shared_pages,
            "bridges": bridges,
            "direct_relations": direct_relations,
            "research_questions": research_questions[:6],
        }

    def review_relation(
        self,
        relation_id: int,
        *,
        review_status: str,
        reviewer: str = "humano",
        note: str = "",
    ) -> dict | None:
        with self.connect() as conn:
            relation = conn.execute("SELECT id FROM relations WHERE id = ?", (relation_id,)).fetchone()
            if not relation:
                return None
            conn.execute(INSERT_RELATION_REVIEW_SQL, (relation_id, review_status, reviewer, note))
            review = conn.execute(GET_RELATION_REVIEW_SQL, (relation_id,)).fetchone()
        return dict(review) if review else None

    def review_entity_identity(
        self,
        entity_id: int,
        *,
        review_status: str,
        reviewer: str = "humano",
        note: str = "",
    ) -> dict | None:
        with self.connect() as conn:
            entity = conn.execute("SELECT id FROM entities WHERE id = ?", (entity_id,)).fetchone()
            if not entity:
                return None
            conn.execute(INSERT_ENTITY_IDENTITY_REVIEW_SQL, (entity_id, review_status, reviewer, note))
            review = conn.execute(GET_ENTITY_IDENTITY_REVIEW_SQL, (entity_id,)).fetchone()
        return dict(review) if review else None

    def _dedupe_mentions_for_entity(self, conn: sqlite3.Connection, entity_id: int) -> None:
        conn.execute(
            """
            DELETE FROM entity_mentions
            WHERE entity_id = ?
              AND id NOT IN (
                SELECT MIN(id)
                FROM entity_mentions
                WHERE entity_id = ?
                GROUP BY page_id, chunk_id, surface_form
              )
            """,
            (entity_id, entity_id),
        )

    def _dedupe_relation_evidence(self, conn: sqlite3.Connection, relation_id: int) -> None:
        conn.execute(
            """
            DELETE FROM relation_evidence
            WHERE relation_id = ?
              AND id NOT IN (
                SELECT MIN(id)
                FROM relation_evidence
                WHERE relation_id = ?
                GROUP BY page_id, chunk_id, quote
              )
            """,
            (relation_id, relation_id),
        )

    def merge_entities(
        self,
        source_entity_id: int,
        target_entity_id: int,
        *,
        reviewer: str = "humano",
        note: str = "",
    ) -> dict | None:
        if source_entity_id == target_entity_id:
            return None

        with self.connect() as conn:
            source = self._get_entity_basic(conn, source_entity_id)
            target = self._get_entity_basic(conn, target_entity_id)
            if not source or not target:
                return None
            if source["type"] != target["type"]:
                return None
            if _entity_effective_status_from_row(target) in {"rejected", "merged"}:
                return None

            moved_mentions = int(
                conn.execute(
                    "SELECT COUNT(*) FROM entity_mentions WHERE entity_id = ?",
                    (source_entity_id,),
                ).fetchone()[0]
            )

            source_aliases = _loads_json(source.get("aliases_json"), [])
            target_aliases = _loads_json(target.get("aliases_json"), [])
            source_attrs = _loads_json(source.get("attributes_json"), {})
            target_attrs = _loads_json(target.get("attributes_json"), {})

            merged_aliases = _dedupe_strings(
                [target.get("canonical_name", "")],
                target_aliases,
                [source.get("canonical_name", "")],
                source_aliases,
            )
            merged_hints = _dedupe_strings(
                target_attrs.get("identity_hints", []),
                source_attrs.get("identity_hints", []),
            )
            merged_from = _dedupe_strings(
                target_attrs.get("merged_from", []),
                [source.get("canonical_name", "")],
                source_attrs.get("merged_from", []),
            )
            merged_entity_ids = _dedupe_strings(
                [str(item) for item in target_attrs.get("merged_entity_ids", [])],
                [str(source_entity_id)],
                [str(item) for item in source_attrs.get("merged_entity_ids", [])],
            )

            target_attrs["identity_status"] = "resolved"
            target_attrs["effective_identity_status"] = "resolved"
            target_attrs["identity_hints"] = merged_hints
            target_attrs["merged_from"] = merged_from
            target_attrs["merged_entity_ids"] = [int(item) for item in merged_entity_ids if str(item).isdigit()]

            source_attrs["identity_status"] = "merged"
            source_attrs["effective_identity_status"] = "merged"
            source_attrs["merged_into_id"] = int(target_entity_id)
            source_attrs["merged_into_name"] = target["canonical_name"]

            conn.execute(
                """
                UPDATE entities
                SET aliases_json = ?,
                    attributes_json = ?,
                    first_seen_year = ?,
                    last_seen_year = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    dump_aliases(tuple(merged_aliases)),
                    dump_attributes(target_attrs),
                    _merge_year_text(target.get("first_seen_year"), source.get("first_seen_year"), prefer="min"),
                    _merge_year_text(target.get("last_seen_year"), source.get("last_seen_year"), prefer="max"),
                    target_entity_id,
                ),
            )
            conn.execute(
                """
                UPDATE entities
                SET attributes_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (dump_attributes(source_attrs), source_entity_id),
            )

            conn.execute(
                "UPDATE entity_mentions SET entity_id = ? WHERE entity_id = ?",
                (target_entity_id, source_entity_id),
            )
            self._dedupe_mentions_for_entity(conn, target_entity_id)

            relation_rows = rows_to_dicts(
                conn.execute(
                    """
                    SELECT id, subject_entity_id, predicate, object_entity_id, object_literal
                    FROM relations
                    WHERE subject_entity_id = ? OR object_entity_id = ?
                    ORDER BY id ASC
                    """,
                    (source_entity_id, source_entity_id),
                ).fetchall()
            )

            merged_relations = 0
            dropped_self_relations = 0

            for relation in relation_rows:
                relation_id = int(relation["id"])
                new_subject = target_entity_id if relation["subject_entity_id"] == source_entity_id else relation["subject_entity_id"]
                new_object = target_entity_id if relation["object_entity_id"] == source_entity_id else relation["object_entity_id"]

                if new_object is not None and new_subject == new_object:
                    conn.execute("DELETE FROM relation_evidence WHERE relation_id = ?", (relation_id,))
                    conn.execute("DELETE FROM relation_reviews WHERE relation_id = ?", (relation_id,))
                    conn.execute("DELETE FROM relations WHERE id = ?", (relation_id,))
                    dropped_self_relations += 1
                    continue

                existing = conn.execute(
                    SELECT_RELATION_ID_SQL,
                    (new_subject, relation["predicate"], new_object, relation["object_literal"]),
                ).fetchone()

                if existing and int(existing["id"]) != relation_id:
                    target_relation_id = int(existing["id"])
                    conn.execute(
                        "UPDATE relation_evidence SET relation_id = ? WHERE relation_id = ?",
                        (target_relation_id, relation_id),
                    )
                    conn.execute(
                        "UPDATE relation_reviews SET relation_id = ? WHERE relation_id = ?",
                        (target_relation_id, relation_id),
                    )
                    self._dedupe_relation_evidence(conn, target_relation_id)
                    conn.execute("DELETE FROM relations WHERE id = ?", (relation_id,))
                    merged_relations += 1
                    continue

                conn.execute(
                    """
                    UPDATE relations
                    SET subject_entity_id = ?,
                        object_entity_id = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (new_subject, new_object, relation_id),
                )

            merge_note = note.strip() or f"mesclada manualmente em {target['canonical_name']} (#{target_entity_id})"
            conn.execute(
                INSERT_ENTITY_IDENTITY_REVIEW_SQL,
                (source_entity_id, "merged", reviewer, merge_note),
            )
            target_note = f"absorveu {source['canonical_name']} (#{source_entity_id})"
            if note.strip():
                target_note = f"{target_note}; {note.strip()}"
            conn.execute(
                INSERT_ENTITY_IDENTITY_REVIEW_SQL,
                (target_entity_id, "resolved", reviewer, target_note),
            )
            conn.execute(
                INSERT_ENTITY_MERGE_REVIEW_SQL,
                (source_entity_id, target_entity_id, "approved", reviewer, merge_note),
            )

        return {
            "source_id": int(source_entity_id),
            "target_id": int(target_entity_id),
            "source_name": source["canonical_name"],
            "target_name": target["canonical_name"],
            "moved_mentions": moved_mentions,
            "merged_relations": merged_relations,
            "dropped_self_relations": dropped_self_relations,
        }

    def get_review_queue(self, limit: int = 12) -> dict:
        with self.connect() as conn:
            identity_rows = conn.execute(
                """
                SELECT e.id, e.canonical_name, e.type, e.aliases_json, e.attributes_json,
                       e.base_normalized_name,
                       COUNT(DISTINCT m.id) AS mentions,
                       MAX(p.ano) AS last_year
                FROM entities e
                LEFT JOIN entity_mentions m ON m.entity_id = e.id
                LEFT JOIN pages p ON p.id = m.page_id
                WHERE (
                    e.attributes_json LIKE '%"identity_status": "ambiguous"%'
                    OR e.attributes_json LIKE '%"identity_status": "contextual"%'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM entity_identity_reviews er WHERE er.entity_id = e.id
                )
                GROUP BY e.id
                ORDER BY mentions DESC, e.canonical_name ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            relation_rows = conn.execute(
                """
                SELECT r.id, r.predicate, r.status, r.confidence,
                       subject.canonical_name AS subject_name,
                       COALESCE(object_entity.canonical_name, r.object_literal) AS object_name,
                       MAX(p.ano) AS last_year
                FROM relations r
                JOIN entities subject ON subject.id = r.subject_entity_id
                LEFT JOIN entities object_entity ON object_entity.id = r.object_entity_id
                LEFT JOIN relation_evidence re ON re.relation_id = r.id
                LEFT JOIN pages p ON p.id = re.page_id
                WHERE r.status IN ('hypothesis', 'probable')
                  AND NOT EXISTS (
                      SELECT 1 FROM relation_reviews rr WHERE rr.relation_id = r.id
                  )
                GROUP BY r.id
                ORDER BY r.confidence DESC, r.id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            suspect_rows = conn.execute(
                f"""
                SELECT
                    e.id,
                    e.canonical_name,
                    e.type,
                    e.base_normalized_name,
                    e.aliases_json,
                    e.attributes_json,
                    {_entity_effective_status_sql('e')} AS identity_review_status,
                    COUNT(DISTINCT m.id) AS mentions,
                    MAX(p.ano) AS last_year
                FROM entities e
                LEFT JOIN entity_mentions m ON m.entity_id = e.id
                LEFT JOIN pages p ON p.id = m.page_id
                WHERE e.type = 'person'
                  AND {_active_entity_condition_sql('e')}
                GROUP BY e.id
                ORDER BY mentions DESC, e.canonical_name ASC
                LIMIT ?
                """,
                (limit * 8,),
            ).fetchall()
            identities = []
            for item in rows_to_dicts(identity_rows):
                attrs = _loads_json(item.get("attributes_json"), {})
                item["identity_status"] = attrs.get("identity_status", "ambiguous")
                item["noise_assessment"] = assess_entity_noise(
                    entity_type=item.get("type", ""),
                    canonical_name=item.get("canonical_name", ""),
                    attributes=attrs,
                )
                if not item["noise_assessment"]["is_probable_noise"]:
                    identities.append(item)

            suspects = []
            for item in rows_to_dicts(suspect_rows):
                attrs = _loads_json(item.get("attributes_json"), {})
                item["identity_status"] = _entity_effective_status_from_row(item)
                item["noise_assessment"] = assess_entity_noise(
                    entity_type=item.get("type", ""),
                    canonical_name=item.get("canonical_name", ""),
                    attributes=attrs,
                )
                if item["noise_assessment"]["is_probable_noise"]:
                    suspects.append(item)

            suspects.sort(
                key=lambda item: (
                    item["noise_assessment"]["score"],
                    int(item.get("mentions") or 0),
                    item.get("canonical_name", ""),
                ),
                reverse=True,
            )
            suspects = suspects[:limit]

            relations = rows_to_dicts(relation_rows)
            merge_sources = []
            seen_ids = set()
            for item in [*identities, *suspects]:
                entity_id = int(item["id"])
                if entity_id in seen_ids:
                    continue
                seen_ids.add(entity_id)
                merge_sources.append(item)
            merges = self._build_merge_review_queue(conn, source_rows=merge_sources, limit=limit)
            merge_candidates_by_entity: dict[int, list[dict]] = {}
            for merge in merges:
                source_id = int(merge["source_id"])
                target_id = int(merge["target_id"])
                shared_payload = {
                    "type": "person",
                    "reasons": merge.get("reasons", []),
                    "score": merge.get("score", 0),
                }
                merge_candidates_by_entity.setdefault(source_id, []).append(
                    {
                        **shared_payload,
                        "id": target_id,
                        "canonical_name": merge["target_name"],
                        "mentions": int(merge["target_mentions"]),
                        "last_year": merge.get("target_last_year"),
                    }
                )
                merge_candidates_by_entity.setdefault(target_id, []).append(
                    {
                        **shared_payload,
                        "id": source_id,
                        "canonical_name": merge["source_name"],
                        "mentions": int(merge["source_mentions"]),
                        "last_year": merge.get("source_last_year"),
                    }
                )
            for item in identities:
                item["merge_candidates"] = merge_candidates_by_entity.get(int(item["id"]), [])[:2]
            for item in suspects:
                item["merge_candidates"] = merge_candidates_by_entity.get(int(item["id"]), [])[:2]
            identities = _collapse_review_entities(identities, limit=limit)
            suspects = _collapse_review_entities(suspects, limit=limit)
            return {"identities": identities, "relations": relations, "suspects": suspects, "merges": merges}
