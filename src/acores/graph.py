"""Construção do grafo genealógico a partir dos registros paroquiais + cruzamento com PE."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from src.config import DATA_DIR, STRUCTURED_DB

PARISH_DB = DATA_DIR / "acores" / "parish_records.db"


def _norm(name: str) -> str:
    import unicodedata, re
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().strip()
    text = re.sub(r"\bd[' ]\b", "de ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_parish_graph(db_path: Path = PARISH_DB) -> dict:
    """Constrói o grafo genealógico completo dos registros paroquiais."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    records = [dict(r) for r in conn.execute("SELECT * FROM parish_records").fetchall()]
    conn.close()

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    _seen_edges: set[tuple] = set()

    def _add(name: str, role: str = "") -> str | None:
        n = _norm(name)
        if not n or n in ("?", "", "pai incerto", "pais incognitos", "incognito", "incognita"):
            return None
        if n not in nodes:
            nodes[n] = {
                "id": n,
                "label": name.strip(),
                "roles": set(),
                "records": [],
                "mentions": 0,
            }
        nodes[n]["roles"].add(role)
        nodes[n]["mentions"] += 1
        return n

    def _edge(src: str, tgt: str, predicate: str):
        key = (src, tgt, predicate)
        if key in _seen_edges or src == tgt:
            return
        _seen_edges.add(key)
        edges.append({"source": src, "target": tgt, "predicate": predicate})

    for r in records:
        child = _add(r["person_name"], "batizado")
        father = _add(r["father_name"], "pai")
        mother = _add(r["mother_name"], "mãe")

        if child:
            nodes[child]["records"].append({
                "id": r["id"],
                "date": r.get("event_date", ""),
                "type": r.get("record_type", ""),
                "parish": r.get("parish", ""),
            })

        if child and father:
            _edge(child, father, "child_of")
        if child and mother:
            _edge(child, mother, "child_of")
        if father and mother:
            _edge(father, mother, "spouse")

        godparents = json.loads(r.get("godparents_json", "[]") or "[]")
        for gp_name in godparents:
            gp = _add(gp_name, "padrinho")
            if child and gp:
                _edge(gp, child, "godparent_of")

        for field, role, parent in [
            ("paternal_grandfather", "avô", father),
            ("paternal_grandmother", "avó", father),
            ("maternal_grandfather", "avô", mother),
            ("maternal_grandmother", "avó", mother),
        ]:
            gp = _add(r.get(field, ""), role)
            if gp and parent:
                _edge(parent, gp, "child_of")

    # Converter sets para listas para JSON
    for n in nodes.values():
        n["roles"] = sorted(n["roles"])

    return {"nodes": list(nodes.values()), "edges": edges}


def get_family_subgraph(person_name: str, *, depth: int = 2, db_path: Path = PARISH_DB) -> dict:
    """Retorna subgrafo centrado em uma pessoa."""
    full = build_parish_graph(db_path)
    target = _norm(person_name)

    # BFS para expandir vizinhança
    visited: set[str] = set()
    queue = [(target, 0)]
    while queue:
        current, d = queue.pop(0)
        if current in visited or d > depth:
            continue
        visited.add(current)
        for edge in full["edges"]:
            other = None
            if edge["source"] == current:
                other = edge["target"]
            elif edge["target"] == current:
                other = edge["source"]
            if other and other not in visited:
                queue.append((other, d + 1))

    nodes = [n for n in full["nodes"] if n["id"] in visited]
    edges = [e for e in full["edges"] if e["source"] in visited and e["target"] in visited]

    # Marcar central
    for n in nodes:
        n["central"] = (n["id"] == target)

    return {"nodes": nodes, "edges": edges, "center": target}


def cross_reference_pe(db_path: Path = PARISH_DB, pe_db_path: Path = STRUCTURED_DB) -> list[dict]:
    """Cruza registros dos Açores com entidades de Pernambuco.

    Lógica:
    - Usa nomes completos dos PAIS (que têm sobrenome), não só do batizado
    - Verifica plausibilidade temporal (nascido nos Açores + 15-60 anos = mencionado em PE)
    - Exige pelo menos 2 tokens de nome em comum
    """
    import re

    parish_conn = sqlite3.connect(str(db_path))
    parish_conn.row_factory = sqlite3.Row
    pe_conn = sqlite3.connect(str(pe_db_path))
    pe_conn.row_factory = sqlite3.Row

    parish_records = [dict(r) for r in parish_conn.execute("SELECT * FROM parish_records").fetchall()]

    # Extrair nomes completos (>=2 tokens) com ano estimado
    acores_people: dict[str, list[dict]] = defaultdict(list)
    _too_common = {"jesus", "sousa", "silva", "costa", "maria", "santos", "jose", "joze",
                   "manoel", "antonio", "francisco", "joao", "anna", "joaquim", "pedro"}

    for r in parish_records:
        # Extrair ano do evento
        year = None
        date = r.get("event_date", "") or ""
        yr_match = re.search(r"(\d{4})", date)
        if yr_match:
            year = int(yr_match.group(1))

        for field in ("father_name", "mother_name", "person_name"):
            name = (r.get(field) or "").strip()
            if not name or len(name) < 5:
                continue
            parts = name.split()
            if len(parts) < 2:
                continue
            # Todos os tokens normalizados
            norm_parts = [_norm(p) for p in parts if len(p) >= 3]
            # Pular se todos os tokens são muito comuns
            substantive = [p for p in norm_parts if p not in _too_common and len(p) >= 4]
            if not substantive:
                continue

            key = _norm(name)
            if key not in acores_people:
                acores_people[key].append({
                    "name": name,
                    "field": field,
                    "record_id": r["id"],
                    "date": date,
                    "year": year,
                    "parish": r.get("parish", ""),
                    "substantive_tokens": substantive,
                })

    # Buscar matches em PE com nomes completos
    matches = []
    seen_pairs: set[tuple] = set()

    for acores_key, acores_records in acores_people.items():
        # Usar os tokens substantivos para busca
        tokens = acores_records[0]["substantive_tokens"]
        for token in tokens[:2]:  # buscar pelos 2 primeiros tokens significativos
            pe_rows = pe_conn.execute(
                """SELECT e.id, e.canonical_name, e.type,
                          COUNT(m.id) as mentions,
                          MIN(p.ano) as first_year_raw
                   FROM entities e
                   JOIN entity_mentions m ON m.entity_id = e.id
                   JOIN pages p ON p.id = m.page_id
                   WHERE e.type = 'person'
                     AND LOWER(e.canonical_name) LIKE ?
                     AND LENGTH(e.canonical_name) >= 8
                   GROUP BY e.id
                   HAVING mentions >= 2
                   ORDER BY mentions DESC
                   LIMIT 10""",
                (f"%{token}%",),
            ).fetchall()

            for pe in pe_rows:
                pair_key = (acores_key, int(pe["id"]))
                if pair_key in seen_pairs:
                    continue

                conf = _match_confidence_v2(acores_records, pe)
                if conf < 0.4:
                    continue

                seen_pairs.add(pair_key)
                matches.append({
                    "acores_name": acores_records[0]["name"],
                    "acores_field": acores_records[0]["field"],
                    "acores_date": acores_records[0]["date"],
                    "acores_year": acores_records[0]["year"],
                    "acores_parish": acores_records[0]["parish"],
                    "acores_record_id": acores_records[0]["record_id"],
                    "pe_entity": {
                        "id": int(pe["id"]),
                        "name": pe["canonical_name"],
                        "mentions": int(pe["mentions"]),
                    },
                    "confidence": conf,
                })

    parish_conn.close()
    pe_conn.close()

    matches.sort(key=lambda x: (-x["confidence"], -x["pe_entity"]["mentions"]))
    return matches


def _match_confidence_v2(acores_records: list, pe_entity) -> float:
    """Calcula confiança com verificação temporal e nome completo."""
    import re

    pe_name = _norm(pe_entity["canonical_name"])
    pe_parts = set(pe_name.split())
    best_score = 0.0

    # Extrair ano de PE
    pe_year = None
    yr_raw = pe_entity["first_year_raw"] or ""
    yr_match = re.search(r"(\d{4})", str(yr_raw))
    if yr_match:
        pe_year = int(yr_match.group(1))

    for ap in acores_records:
        score = 0.0
        acores_name = _norm(ap["name"])
        acores_parts = set(acores_name.split())

        # Tokens em comum (excluindo preposições)
        preps = {"de", "da", "do", "das", "dos", "e"}
        common = (acores_parts & pe_parts) - preps
        acores_substantive = acores_parts - preps
        pe_substantive = pe_parts - preps

        if not common:
            continue

        # Proporção de tokens em comum
        overlap_ratio = len(common) / max(len(acores_substantive), len(pe_substantive), 1)

        if acores_name == pe_name:
            score = 0.95
        elif overlap_ratio >= 0.8:
            score = 0.85
        elif overlap_ratio >= 0.5:
            score = 0.65
        elif len(common) >= 2:
            score = 0.50
        elif len(common) == 1 and len(list(common)[0]) >= 5:
            score = 0.35
        else:
            continue

        # Verificação temporal
        acores_year = ap.get("year")
        if acores_year and pe_year:
            gap = pe_year - acores_year
            if ap["field"] == "person_name":
                # Batizado nos Açores → mencionado em PE: gap deve ser 15-70 anos
                if 15 <= gap <= 70:
                    score += 0.1
                elif gap < 0 or gap > 80:
                    score *= 0.3  # penalizar fortemente
            elif ap["field"] in ("father_name", "mother_name"):
                # Pai/mãe nos Açores → mencionado em PE: gap pode ser 0-80 anos
                if 0 <= gap <= 80:
                    score += 0.05
                elif gap < -20 or gap > 100:
                    score *= 0.3

        # Campo: pais têm sobrenome mais confiável que batizado
        if ap["field"] in ("father_name", "mother_name") and len(common) >= 2:
            score += 0.05

        best_score = max(best_score, score)

    return min(round(best_score, 2), 0.99)


def build_family_trees(db_path: Path = PARISH_DB, min_descendants: int = 3) -> list[dict]:
    """Constrói árvores genealógicas automáticas a partir dos registros paroquiais."""
    import re as _re

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    records = [dict(r) for r in conn.execute("SELECT * FROM parish_records").fetchall()]
    conn.close()

    def _extract_year(date_str):
        m = _re.search(r"(\d{4})", str(date_str or ""))
        return int(m.group(1)) if m else None

    def _pid(name, year=None):
        n = _norm(name)
        if not n or n in ("?", "pai incerto", "pais incognitos", "incognito", "incognita", "nao mencionado"):
            return None
        decade = f"_{(year // 10) * 10}" if year else ""
        return f"{n}{decade}"

    people: dict[str, dict] = {}
    edges: list[tuple] = []

    for r in records:
        year = _extract_year(r.get("event_date"))
        parent_year = (year - 25) if year else None
        gp_year = (year - 50) if year else None

        child_id = _pid(r["person_name"], year)
        father_id = _pid(r["father_name"], parent_year)
        mother_id = _pid(r["mother_name"], parent_year)

        for pid, name, yr in [(child_id, r["person_name"], year),
                               (father_id, r["father_name"], parent_year),
                               (mother_id, r["mother_name"], parent_year)]:
            if pid and pid not in people:
                people[pid] = {"id": pid, "name": (name or "").strip(), "year": yr, "children": set(), "parents": set()}

        if child_id and father_id and child_id != father_id:
            people.setdefault(father_id, {"id": father_id, "name": "", "year": parent_year, "children": set(), "parents": set()})
            people[father_id]["children"].add(child_id)
            people[child_id]["parents"].add(father_id)
        if child_id and mother_id and child_id != mother_id:
            people.setdefault(mother_id, {"id": mother_id, "name": "", "year": parent_year, "children": set(), "parents": set()})
            people[mother_id]["children"].add(child_id)
            people[child_id]["parents"].add(mother_id)
        if father_id and mother_id and father_id != mother_id:
            edges.append((father_id, mother_id, "spouse"))

        for gf_field, parent_id in [("paternal_grandfather", father_id), ("maternal_grandfather", mother_id)]:
            gf_name = r.get(gf_field, "")
            gf_id = _pid(gf_name, gp_year)
            if gf_id and parent_id and gf_id != parent_id:
                if gf_id not in people:
                    people[gf_id] = {"id": gf_id, "name": (gf_name or "").strip(), "year": gp_year, "children": set(), "parents": set()}
                people[gf_id]["children"].add(parent_id)
                people[parent_id]["parents"].add(gf_id)

    # Encontrar raízes (patriarcas sem pais)
    roots = [p for p in people.values() if not p["parents"] and p["children"] and p["name"]]

    def _count_descendants(pid, visited=None):
        if visited is None:
            visited = set()
        if pid in visited:
            return 0
        visited.add(pid)
        p = people.get(pid)
        if not p:
            return 0
        return 1 + sum(_count_descendants(c, visited) for c in p["children"])

    def _build_tree_node(pid, visited=None, max_depth=5):
        if visited is None:
            visited = set()
        if pid in visited or max_depth <= 0:
            return None
        visited.add(pid)
        p = people.get(pid)
        if not p:
            return None
        children = []
        for c in sorted(p["children"]):
            child_node = _build_tree_node(c, visited, max_depth - 1)
            if child_node:
                children.append(child_node)
        return {
            "id": pid,
            "name": p["name"],
            "year": p.get("year"),
            "children": children,
        }

    trees = []
    for root in roots:
        desc = _count_descendants(root["id"])
        if desc >= min_descendants:
            tree = _build_tree_node(root["id"])
            if tree:
                tree["descendants"] = desc
                trees.append(tree)

    trees.sort(key=lambda t: -t["descendants"])
    return trees


def get_parish_stats(db_path: Path = PARISH_DB) -> dict:
    """Estatísticas do acervo paroquial para o frontend."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM parish_records").fetchone()[0]
    by_type = {r["record_type"]: r["c"] for r in conn.execute(
        "SELECT record_type, COUNT(*) as c FROM parish_records GROUP BY record_type"
    ).fetchall()}

    # Período
    dates = conn.execute(
        "SELECT MIN(event_date) as earliest, MAX(event_date) as latest FROM parish_records WHERE event_date != ''"
    ).fetchone()

    # Famílias
    families = conn.execute(
        """SELECT father_name, mother_name, COUNT(*) as children
           FROM parish_records
           WHERE father_name != '' AND mother_name != ''
             AND father_name NOT LIKE '%incognit%'
           GROUP BY father_name, mother_name
           HAVING children >= 2
           ORDER BY children DESC LIMIT 10"""
    ).fetchall()

    # Padrinhos mais ativos
    # (precisa parse do JSON)

    conn.close()

    graph = build_parish_graph(db_path)

    return {
        "total_records": total,
        "by_type": by_type,
        "earliest_date": dates["earliest"] if dates else "",
        "latest_date": dates["latest"] if dates else "",
        "unique_people": len(graph["nodes"]),
        "total_relations": len(graph["edges"]),
        "top_families": [
            {
                "father": f["father_name"],
                "mother": f["mother_name"],
                "children": f["children"],
            }
            for f in families
        ],
    }
