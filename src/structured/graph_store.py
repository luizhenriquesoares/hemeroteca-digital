"""Exportação da camada estruturada SQLite para um grafo navegável.

O objetivo aqui não é só desenhar conexões entre pessoas. O grafo precisa
manter proveniência documental, então inclui nós editoriais reais:

- entidades: person, institution, place
- publication
- issue
- page
- role/literal

Além das relações extraídas, ele também inclui arestas documentais como
`mentioned_in`, para que a UI consiga levar o usuário até a página do jornal.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from urllib.parse import quote

try:
    import networkx as nx
except ModuleNotFoundError:  # pragma: no cover - fallback for lean local envs
    class _MiniDiGraph:
        def __init__(self):
            self._nodes: dict[str, dict] = {}
            self._edges: list[tuple[str, str, dict]] = []

        def add_node(self, node_id: str, **attrs):
            self._nodes[node_id] = {**self._nodes.get(node_id, {}), **attrs}

        def has_node(self, node_id: str) -> bool:
            return node_id in self._nodes

        def add_edge(self, source: str, target: str, **attrs):
            self._edges = [edge for edge in self._edges if not (edge[0] == source and edge[1] == target and edge[2].get("predicate") == attrs.get("predicate"))]
            self._edges.append((source, target, attrs))

        def has_edge(self, source: str, target: str) -> bool:
            return any(edge[0] == source and edge[1] == target for edge in self._edges)

        def nodes(self, data: bool = False):
            return list(self._nodes.items()) if data else list(self._nodes.keys())

        def edges(self, data: bool = False):
            if data:
                return list(self._edges)
            return [(edge[0], edge[1]) for edge in self._edges]

        def number_of_nodes(self) -> int:
            return len(self._nodes)

        def number_of_edges(self) -> int:
            return len(self._edges)

    class _MiniNX:
        DiGraph = _MiniDiGraph

        @staticmethod
        def write_graphml(graph, output_path: str):
            payload = {
                "nodes": [{"id": node_id, **attrs} for node_id, attrs in graph.nodes(data=True)],
                "edges": [
                    {"source": source, "target": target, **attrs}
                    for source, target, attrs in graph.edges(data=True)
                ],
            }
            Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    nx = _MiniNX()

from src.config import DATA_DIR

logger = logging.getLogger(__name__)

GRAPH_DIR = DATA_DIR / "graph"
STRUCTURED_DB = DATA_DIR / "structured" / "hemeroteca.db"

GRAPH_PREDICATES = {
    "spouse_of",
    "child_of",
    "parent_of",
    "holds_role",
    "member_of",
    "resident_of",
}
WEAK_PREDICATES = {"mentioned_with"}


def _page_node_id(page_id: int) -> str:
    return f"page_{page_id}"


def _publication_node_id(bib: str) -> str:
    return f"publication_{bib}"


def _issue_node_id(bib: str, edicao: str) -> str:
    return f"issue_{bib}_{edicao or '?'}"


def _entity_node_id(entity_id: int) -> str:
    return f"entity_{entity_id}"


def _literal_node_id(predicate: str, literal: str) -> str:
    slug = (literal or "").strip()[:60]
    return f"literal_{predicate}_{slug}"


def _page_view_url(bib: str, pagina: str, highlight: str = "") -> str:
    url = f"/page/{bib}/{pagina}"
    if highlight:
        url += f"?q={quote(highlight)}"
    return url


def _load_entities(conn: sqlite3.Connection, min_mentions: int) -> tuple[list[sqlite3.Row], set[int]]:
    rows = conn.execute(
        """
        SELECT e.id, e.type, e.canonical_name, e.normalized_name,
               e.aliases_json, e.attributes_json,
               e.first_seen_year, e.last_seen_year,
               (
                   SELECT er.review_status
                   FROM entity_identity_reviews er
                   WHERE er.entity_id = e.id
                   ORDER BY er.id DESC
                   LIMIT 1
               ) AS identity_review_status,
               COUNT(DISTINCT m.id) AS mention_count
        FROM entities e
        LEFT JOIN entity_mentions m ON m.entity_id = e.id
        GROUP BY e.id
        HAVING mention_count >= ?
        ORDER BY mention_count DESC
        """,
        (min_mentions,),
    ).fetchall()
    return rows, {int(row["id"]) for row in rows}


def _load_relations(conn: sqlite3.Connection, predicates: set[str], min_confidence: float) -> list[sqlite3.Row]:
    placeholders = ",".join(["?"] * len(predicates))
    return conn.execute(
        f"""
        SELECT r.id, r.subject_entity_id, r.predicate,
               r.object_entity_id, r.object_literal,
               r.confidence, r.status, r.extraction_method,
               (
                   SELECT rr.review_status
                   FROM relation_reviews rr
                   WHERE rr.relation_id = r.id
                   ORDER BY rr.id DESC
                   LIMIT 1
               ) AS review_status
        FROM relations r
        WHERE r.predicate IN ({placeholders})
          AND r.confidence >= ?
        """,
        (*predicates, min_confidence),
    ).fetchall()


def _load_relation_evidences(conn: sqlite3.Connection, relation_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT re.id, re.quote, re.confidence, re.chunk_id,
               p.id AS page_id, p.bib, p.pagina, p.jornal, p.ano, p.edicao, p.image_path
        FROM relation_evidence re
        JOIN pages p ON p.id = re.page_id
        WHERE re.relation_id = ?
        ORDER BY re.confidence DESC, re.id ASC
        LIMIT 5
        """,
        (relation_id,),
    ).fetchall()


def _ensure_publication_and_issue_nodes(
    graph: nx.DiGraph,
    *,
    bib: str,
    jornal: str,
    edicao: str,
) -> tuple[str, str | None]:
    publication_id = _publication_node_id(bib)
    if not graph.has_node(publication_id):
        graph.add_node(
            publication_id,
            node_type="publication",
            label=jornal or bib,
            bib=bib,
        )

    issue_id = None
    if edicao and edicao != "?":
        issue_id = _issue_node_id(bib, edicao)
        if not graph.has_node(issue_id):
            graph.add_node(
                issue_id,
                node_type="issue",
                label=f"Edição {edicao}",
                bib=bib,
                issue=edicao,
            )
        if not graph.has_edge(issue_id, publication_id):
            graph.add_edge(
                issue_id,
                publication_id,
                predicate="published_in",
                confidence=1.0,
                status="confirmed",
            )

    return publication_id, issue_id


def _ensure_page_node(graph: nx.DiGraph, page_row: sqlite3.Row) -> tuple[str, str, str | None]:
    publication_id, issue_id = _ensure_publication_and_issue_nodes(
        graph,
        bib=page_row["bib"],
        jornal=page_row["jornal"],
        edicao=page_row["edicao"],
    )
    page_id = _page_node_id(int(page_row["page_id"]))
    if not graph.has_node(page_id):
        graph.add_node(
            page_id,
            node_type="page",
            label=f"{page_row['bib']} / {page_row['pagina']}",
            bib=page_row["bib"],
            pagina=str(page_row["pagina"]),
            jornal=page_row["jornal"],
            ano=page_row["ano"],
            edicao=page_row["edicao"],
            image_path=page_row["image_path"] or "",
            page_view_url=_page_view_url(page_row["bib"], str(page_row["pagina"])),
        )

    parent_id = issue_id or publication_id
    if not graph.has_edge(page_id, parent_id):
        graph.add_edge(
            page_id,
            parent_id,
            predicate="published_in" if issue_id else "belongs_to_publication",
            confidence=1.0,
            status="confirmed",
        )
    return page_id, publication_id, issue_id


def _add_relation_edge(
    graph: nx.DiGraph,
    *,
    source: str,
    target: str,
    relation: sqlite3.Row,
    evidences: list[sqlite3.Row],
) -> None:
    quotes = [ev["quote"][:200] for ev in evidences]
    source_pages = [f"{ev['bib']}:{ev['pagina']}" for ev in evidences]
    evidence_ids = [int(ev["id"]) for ev in evidences]
    page_ids = [int(ev["page_id"]) for ev in evidences]
    chunk_ids = [str(ev["chunk_id"]) for ev in evidences]
    primary_quote = quotes[0] if quotes else ""
    primary_page = source_pages[0] if source_pages else ""
    primary_view_url = (
        _page_view_url(evidences[0]["bib"], str(evidences[0]["pagina"]), primary_quote)
        if evidences
        else ""
    )
    graph.add_edge(
        source,
        target,
        predicate=relation["predicate"],
        confidence=relation["confidence"],
        status=relation["review_status"] or relation["status"],
        extraction_status=relation["status"],
        extraction_method=relation["extraction_method"],
        relation_id=int(relation["id"]),
        quotes=json.dumps(quotes, ensure_ascii=False),
        source_pages=json.dumps(source_pages, ensure_ascii=False),
        evidence_ids=json.dumps(evidence_ids),
        source_page_ids=json.dumps(page_ids),
        chunk_ids=json.dumps(chunk_ids, ensure_ascii=False),
        quote=primary_quote,
        source_page=primary_page,
        page_view_url=primary_view_url,
    )


def build_graph(
    db_path: Path = STRUCTURED_DB,
    include_mentioned_with: bool = False,
    min_confidence: float = 0.3,
    min_mentions: int = 1,
) -> nx.DiGraph:
    """Constrói um grafo dirigido com proveniência documental."""
    graph = nx.DiGraph()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    predicates = set(GRAPH_PREDICATES)
    if include_mentioned_with:
        predicates |= WEAK_PREDICATES

    logger.info("Carregando entidades para o grafo...")
    entities, entity_ids = _load_entities(conn, min_mentions)
    for entity in entities:
        graph.add_node(
            _entity_node_id(int(entity["id"])),
            node_type=entity["type"],
            label=entity["canonical_name"],
            normalized_name=entity["normalized_name"],
            aliases=entity["aliases_json"],
            attributes=entity["attributes_json"],
            identity_status=entity["identity_review_status"] or "",
            first_year=entity["first_seen_year"] or "",
            last_year=entity["last_seen_year"] or "",
            mentions=int(entity["mention_count"]),
        )

    logger.info("Carregando menções para nós documentais...")
    mentions = conn.execute(
        """
        SELECT m.id, m.entity_id, m.surface_form, m.snippet, m.confidence, m.chunk_id,
               p.id AS page_id, p.bib, p.pagina, p.jornal, p.ano, p.edicao, p.image_path
        FROM entity_mentions m
        JOIN pages p ON p.id = m.page_id
        WHERE m.entity_id IN (
            SELECT id FROM entities
        )
        ORDER BY m.confidence DESC, m.id ASC
        """
    ).fetchall()
    for mention in mentions:
        entity_id = int(mention["entity_id"])
        if entity_id not in entity_ids:
            continue
        page_id, _, _ = _ensure_page_node(graph, mention)
        graph.add_edge(
            _entity_node_id(entity_id),
            page_id,
            predicate="mentioned_in",
            confidence=float(mention["confidence"]),
            status="confirmed",
            mention_id=int(mention["id"]),
            quote=mention["snippet"][:200],
            source_page=f"{mention['bib']}:{mention['pagina']}",
            source_page_id=int(mention["page_id"]),
            chunk_id=str(mention["chunk_id"]),
            page_view_url=_page_view_url(
                mention["bib"],
                str(mention["pagina"]),
                mention["surface_form"] or mention["snippet"],
            ),
        )

    logger.info("Carregando relações com evidência...")
    relations = _load_relations(conn, predicates, min_confidence)
    for relation in relations:
        subject_entity_id = int(relation["subject_entity_id"])
        if subject_entity_id not in entity_ids:
            continue
        source = _entity_node_id(subject_entity_id)

        object_entity_id = relation["object_entity_id"]
        if object_entity_id and int(object_entity_id) in entity_ids:
            target = _entity_node_id(int(object_entity_id))
        elif relation["object_literal"]:
            target = _literal_node_id(relation["predicate"], relation["object_literal"])
            if not graph.has_node(target):
                graph.add_node(
                    target,
                    node_type="role" if relation["predicate"] == "holds_role" else "literal",
                    label=relation["object_literal"],
                )
        else:
            continue

        evidences = _load_relation_evidences(conn, int(relation["id"]))
        for evidence in evidences:
            _ensure_page_node(graph, evidence)
        _add_relation_edge(graph, source=source, target=target, relation=relation, evidences=evidences)

    conn.close()
    logger.info(
        "Grafo construído: %s nós, %s arestas",
        graph.number_of_nodes(),
        graph.number_of_edges(),
    )
    return graph


def export_graphml(graph: nx.DiGraph, output: Path | None = None) -> Path:
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    output = output or GRAPH_DIR / "hemeroteca.graphml"
    nx.write_graphml(graph, str(output))
    logger.info("GraphML exportado: %s", output)
    return output


def export_json(graph: nx.DiGraph, output: Path | None = None) -> Path:
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    output = output or GRAPH_DIR / "hemeroteca.json"

    nodes = []
    for node_id, data in graph.nodes(data=True):
        nodes.append(
            {
                "id": node_id,
                "label": data.get("label", node_id),
                "type": data.get("node_type", "unknown"),
                "mentions": data.get("mentions", 0),
                "identity_status": data.get("identity_status", ""),
                "first_year": data.get("first_year", ""),
                "last_year": data.get("last_year", ""),
                "bib": data.get("bib", ""),
                "pagina": data.get("pagina", ""),
                "issue": data.get("issue", ""),
                "page_view_url": data.get("page_view_url", ""),
            }
        )

    edges = []
    for source, target, data in graph.edges(data=True):
        edges.append(
            {
                "source": source,
                "target": target,
                "predicate": data.get("predicate", ""),
                "confidence": data.get("confidence", 0),
                "status": data.get("status", ""),
                "quote": data.get("quote", ""),
                "quotes": json.loads(data.get("quotes", "[]")) if data.get("quotes") else [],
                "source_page": data.get("source_page", ""),
                "source_pages": json.loads(data.get("source_pages", "[]")) if data.get("source_pages") else [],
                "page_view_url": data.get("page_view_url", ""),
            }
        )

    payload = {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "node_types": {},
            "edge_types": {},
        },
    }
    for node in nodes:
        node_type = node["type"]
        payload["stats"]["node_types"][node_type] = payload["stats"]["node_types"].get(node_type, 0) + 1
    for edge in edges:
        predicate = edge["predicate"]
        payload["stats"]["edge_types"][predicate] = payload["stats"]["edge_types"].get(predicate, 0) + 1

    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("JSON do grafo exportado: %s", output)
    return output


def _add_subgraph_entity_node(conn: sqlite3.Connection, nodes: dict[str, dict], entity_id: int, *, central_id: int) -> None:
    entity = conn.execute(
        """
        SELECT e.id, e.type, e.canonical_name, e.normalized_name,
               (
                   SELECT er.review_status
                   FROM entity_identity_reviews er
                   WHERE er.entity_id = e.id
                   ORDER BY er.id DESC
                   LIMIT 1
               ) AS identity_review_status,
               COUNT(DISTINCT m.id) AS mentions
        FROM entities e
        LEFT JOIN entity_mentions m ON m.entity_id = e.id
        WHERE e.id = ?
        GROUP BY e.id
        """,
        (entity_id,),
    ).fetchone()
    if not entity:
        return
    node_id = _entity_node_id(entity_id)
    nodes[node_id] = {
        "id": node_id,
        "entity_id": entity_id,
        "label": entity["canonical_name"],
        "type": entity["type"],
        "mentions": int(entity["mentions"]),
        "identity_status": entity["identity_review_status"] or "",
        "central": entity_id == central_id,
    }


def _add_subgraph_page_context(nodes: dict[str, dict], edges: list[dict], evidence: sqlite3.Row) -> None:
    page_id = _page_node_id(int(evidence["page_id"]))
    if page_id not in nodes:
        nodes[page_id] = {
            "id": page_id,
            "label": f"{evidence['bib']} / {evidence['pagina']}",
            "type": "page",
            "mentions": 0,
            "central": False,
            "bib": evidence["bib"],
            "pagina": str(evidence["pagina"]),
            "ano": evidence["ano"],
            "edicao": evidence["edicao"],
            "page_view_url": _page_view_url(evidence["bib"], str(evidence["pagina"]), evidence["quote"]),
        }

    publication_id = _publication_node_id(evidence["bib"])
    if publication_id not in nodes:
        nodes[publication_id] = {
            "id": publication_id,
            "label": evidence["jornal"] or evidence["bib"],
            "type": "publication",
            "mentions": 0,
            "central": False,
            "bib": evidence["bib"],
        }

    if not any(edge["source"] == page_id and edge["target"] == publication_id for edge in edges):
        edges.append(
            {
                "source": page_id,
                "target": publication_id,
                "predicate": "published_in",
                "confidence": 1.0,
                "status": "confirmed",
                "quote": "",
                "source_page": f"{evidence['bib']}:{evidence['pagina']}",
                "page_view_url": _page_view_url(evidence["bib"], str(evidence["pagina"])),
            }
        )


def get_entity_subgraph(
    entity_id: int,
    db_path: Path = STRUCTURED_DB,
    depth: int = 2,
    min_confidence: float = 0.3,
) -> dict:
    """Retorna subgrafo centrado em uma entidade para a UI."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    visited: set[int] = set()
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    queue: list[tuple[int, int]] = [(entity_id, 0)]

    while queue:
        current_entity_id, current_depth = queue.pop(0)
        if current_entity_id in visited or current_depth > depth:
            continue
        visited.add(current_entity_id)
        _add_subgraph_entity_node(conn, nodes, current_entity_id, central_id=entity_id)

        relations = conn.execute(
            """
            SELECT r.id, r.subject_entity_id, r.predicate, r.object_entity_id,
                   r.object_literal, r.confidence, r.status,
                   (
                       SELECT rr.review_status
                       FROM relation_reviews rr
                       WHERE rr.relation_id = r.id
                       ORDER BY rr.id DESC
                       LIMIT 1
                   ) AS review_status
            FROM relations r
            WHERE (r.subject_entity_id = ? OR r.object_entity_id = ?)
              AND r.confidence >= ?
            ORDER BY r.confidence DESC, r.id ASC
            """,
            (current_entity_id, current_entity_id, min_confidence),
        ).fetchall()

        for relation in relations:
            subject_id = int(relation["subject_entity_id"])
            object_id = relation["object_entity_id"]
            other_id = int(object_id) if object_id and int(object_id) != current_entity_id else subject_id
            target = ""
            if object_id:
                _add_subgraph_entity_node(conn, nodes, int(object_id), central_id=entity_id)
                target = _entity_node_id(int(object_id))
            elif relation["object_literal"]:
                target = _literal_node_id(relation["predicate"], relation["object_literal"])
                nodes.setdefault(
                    target,
                    {
                        "id": target,
                        "label": relation["object_literal"],
                        "type": "role" if relation["predicate"] == "holds_role" else "literal",
                        "mentions": 0,
                        "central": False,
                    },
                )
            else:
                continue

            evidences = _load_relation_evidences(conn, int(relation["id"]))
            primary = evidences[0] if evidences else None
            if primary:
                _add_subgraph_page_context(nodes, edges, primary)

            edges.append(
                {
                    "source": _entity_node_id(subject_id),
                    "target": target,
                    "predicate": relation["predicate"],
                    "confidence": float(relation["confidence"]),
                    "status": relation["status"],
                    "effective_status": relation["review_status"] or relation["status"],
                    "quote": primary["quote"][:200] if primary else "",
                    "quotes": [ev["quote"][:200] for ev in evidences],
                    "source_page": f"{primary['bib']}:{primary['pagina']}" if primary else "",
                    "source_pages": [f"{ev['bib']}:{ev['pagina']}" for ev in evidences],
                    "page_view_url": (
                        _page_view_url(primary["bib"], str(primary["pagina"]), primary["quote"])
                        if primary
                        else ""
                    ),
                    "source_page_id": int(primary["page_id"]) if primary else None,
                    "evidences": [
                        {
                            "quote": ev["quote"][:200],
                            "source_page": f"{ev['bib']}:{ev['pagina']}",
                            "page_view_url": _page_view_url(ev["bib"], str(ev["pagina"]), ev["quote"]),
                        }
                        for ev in evidences
                    ],
                }
            )

            if primary:
                page_id = _page_node_id(int(primary["page_id"]))
                if not any(
                    edge["source"] == _entity_node_id(subject_id)
                    and edge["target"] == page_id
                    and edge["predicate"] == "mentioned_in"
                    for edge in edges
                ):
                    edges.append(
                        {
                            "source": _entity_node_id(subject_id),
                            "target": page_id,
                            "predicate": "mentioned_in",
                            "confidence": float(primary["confidence"]),
                            "status": "confirmed",
                            "effective_status": "confirmed",
                            "quote": primary["quote"][:200],
                            "quotes": [primary["quote"][:200]],
                            "source_page": f"{primary['bib']}:{primary['pagina']}",
                            "source_pages": [f"{primary['bib']}:{primary['pagina']}"],
                            "page_view_url": _page_view_url(primary["bib"], str(primary["pagina"]), primary["quote"]),
                            "source_page_id": int(primary["page_id"]),
                            "evidences": [
                                {
                                    "quote": primary["quote"][:200],
                                    "source_page": f"{primary['bib']}:{primary['pagina']}",
                                    "page_view_url": _page_view_url(primary["bib"], str(primary["pagina"]), primary["quote"]),
                                }
                            ],
                        }
                    )

            if object_id and other_id not in visited and current_depth < depth:
                queue.append((other_id, current_depth + 1))

    conn.close()

    node_list = list(nodes.values())
    if len(node_list) > 30:
        central = [node for node in node_list if node.get("central")]
        others = sorted(
            [node for node in node_list if not node.get("central")],
            key=lambda item: (item.get("type") == "page", item.get("mentions", 0)),
            reverse=True,
        )
        node_list = central + others[:29]
        valid_ids = {node["id"] for node in node_list}
        edges = [edge for edge in edges if edge["source"] in valid_ids and edge["target"] in valid_ids]

    return {"nodes": node_list, "edges": edges, "center_entity_id": entity_id}


def _is_graph_legible(name: str) -> bool:
    """Filtro rigoroso de legibilidade para nomes no grafo da homepage."""
    import re as _re
    import unicodedata as _ud
    if not name or len(name) < 5:
        return False
    tokens = name.split()
    if len(tokens) < 2:
        return False
    # Hifens soltos (OCR cortado: "Ra- tisquien")
    if "- " in name or " -" in name:
        return False
    # Letras repetidas após strip de acentos
    stripped = "".join(c for c in _ud.normalize("NFKD", name) if not _ud.combining(c))
    if _re.search(r"(.)\1{2,}", stripped):
        return False
    # Nomes comuns de cargos/funções, não pessoas
    lower = name.lower()
    if any(kw in lower for kw in ["juiz", "chefe", "mestre", "coronel ", "tenente ", "capitão "]):
        # Se o nome COMEÇA com cargo sem nome pessoal, é suspeito
        first = tokens[0].lower()
        if first in {"juiz", "chefe", "mestre", "mr", "dr"}:
            return False
    preps = {"de", "da", "do", "das", "dos", "e", "d'", "d'"}
    substantive = [t for t in tokens if t.lower() not in preps]
    if not substantive:
        return False
    # Verificar cada token substantivo
    _common_portuguese_names = {
        "antonio", "joze", "jose", "joaquim", "francisco", "manoel", "manuel",
        "joao", "luiz", "pedro", "maria", "anna", "rosa", "thomaz", "carlos",
        "silva", "ferreira", "pereira", "souza", "costa", "mello", "santos",
        "oliveira", "lima", "rodrigues", "pinto", "barros", "ramos", "gomes",
        "lopes", "alves", "moreira", "rocha", "bastos", "cavalcante", "monteiro",
    }
    has_recognizable = False
    for token in substantive:
        if len(token) <= 1:
            return False
        if len(token) >= 3 and not _re.search(r"[aeiouáàâãéêíóôõú]", token, _re.IGNORECASE):
            return False
        # Caracteres suspeitos de OCR (ê seguido de consoante incomum, etc.)
        if _re.search(r"[ôõêã][^aeiouáàâãéêíóôõúsrnlm\s]", token, _re.IGNORECASE):
            return False
        if token.lower().rstrip("s") in _common_portuguese_names:
            has_recognizable = True
    # Último token curto = truncado
    if len(substantive[-1]) <= 4:
        return False
    # Pelo menos um token deve parecer nome português reconhecível
    # OU o nome todo ter >= 3 tokens (nomes longos tendem a ser reais)
    if not has_recognizable and len(substantive) < 3:
        return False
    return True


def get_featured_graph(
    db_path: Path = STRUCTURED_DB,
    limit: int = 25,
    min_shared_pages: int = 3,
) -> dict:
    """Grafo de rede social: pessoas conectadas por co-presença em páginas, com cargos como atributos."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Pares de pessoas que compartilham muitas páginas
    pairs = conn.execute(
        """
        SELECT
            e1.id as id_a, e1.canonical_name as name_a,
            e2.id as id_b, e2.canonical_name as name_b,
            COUNT(DISTINCT m1.page_id) as shared_pages
        FROM entity_mentions m1
        JOIN entity_mentions m2 ON m2.page_id = m1.page_id AND m2.entity_id > m1.entity_id
        JOIN entities e1 ON e1.id = m1.entity_id
        JOIN entities e2 ON e2.id = m2.entity_id
        WHERE e1.type = 'person' AND e2.type = 'person'
          AND LENGTH(e1.canonical_name) >= 8
          AND LENGTH(e2.canonical_name) >= 8
        GROUP BY m1.entity_id, m2.entity_id
        HAVING shared_pages >= ?
        ORDER BY shared_pages DESC
        LIMIT ?
        """,
        (min_shared_pages, limit * 5),
    ).fetchall()

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    entity_ids: set[int] = set()

    for pair in pairs:
        if len(nodes) >= limit * 2:
            break
        for side, eid, name in [("a", pair["id_a"], pair["name_a"]), ("b", pair["id_b"], pair["name_b"])]:
            eid = int(eid)
            if not _is_graph_legible(name):
                continue
            nid = f"entity_{eid}"
            if nid not in nodes:
                nodes[nid] = {
                    "id": nid, "entity_id": eid, "label": name,
                    "type": "person", "mentions": 0, "role": "", "central": False,
                }
                entity_ids.add(eid)

        id_a, id_b = f"entity_{pair['id_a']}", f"entity_{pair['id_b']}"
        if id_a in nodes and id_b in nodes:
            edges.append({
                "source": id_a, "target": id_b,
                "predicate": "co_mention",
                "weight": int(pair["shared_pages"]),
                "label": f"{pair['shared_pages']} pag.",
            })

    # Cargos como atributo dos nós
    if entity_ids:
        id_list = ",".join(str(i) for i in entity_ids)
        for row in conn.execute(f"""
            SELECT r.subject_entity_id, COALESCE(o.canonical_name, r.object_literal) as role
            FROM relations r LEFT JOIN entities o ON o.id = r.object_entity_id
            WHERE r.predicate = 'holds_role' AND r.subject_entity_id IN ({id_list})
            ORDER BY r.confidence DESC
        """).fetchall():
            nid = f"entity_{row['subject_entity_id']}"
            if nid in nodes and not nodes[nid]["role"]:
                nodes[nid]["role"] = row["role"] or ""

        for row in conn.execute(f"""
            SELECT entity_id, COUNT(*) as c FROM entity_mentions
            WHERE entity_id IN ({id_list}) GROUP BY entity_id
        """).fetchall():
            nid = f"entity_{row['entity_id']}"
            if nid in nodes:
                nodes[nid]["mentions"] = int(row["c"])

        # Relações biográficas entre os nós
        for rel in conn.execute(f"""
            SELECT r.subject_entity_id, r.object_entity_id, r.predicate
            FROM relations r
            WHERE r.predicate IN ('child_of','spouse_of','widow_of','parent_of','appointed_to')
              AND r.subject_entity_id IN ({id_list}) AND r.object_entity_id IN ({id_list})
        """).fetchall():
            labels = {"child_of": "filho de", "spouse_of": "cônjuge", "widow_of": "viúva de",
                      "parent_of": "pai/mãe", "appointed_to": "nomeado"}
            edges.append({
                "source": f"entity_{rel['subject_entity_id']}",
                "target": f"entity_{rel['object_entity_id']}",
                "predicate": rel["predicate"],
                "weight": 3,
                "label": labels.get(rel["predicate"], rel["predicate"]),
            })

    conn.close()

    # Manter só nós conectados, limitar
    connected = set()
    for e in edges:
        connected.add(e["source"])
        connected.add(e["target"])
    nodes = {k: v for k, v in nodes.items() if k in connected}

    if len(nodes) > limit:
        ec: dict[str, int] = {}
        for e in edges:
            ec[e["source"]] = ec.get(e["source"], 0) + 1
            ec[e["target"]] = ec.get(e["target"], 0) + 1
        keep = set(sorted(ec, key=ec.get, reverse=True)[:limit])
        nodes = {k: v for k, v in nodes.items() if k in keep}
        edges = [e for e in edges if e["source"] in keep and e["target"] in keep]

    if nodes:
        ec = {}
        for e in edges:
            ec[e["source"]] = ec.get(e["source"], 0) + 1
            ec[e["target"]] = ec.get(e["target"], 0) + 1
        if ec:
            nodes[max(ec, key=ec.get)]["central"] = True

    return {"nodes": list(nodes.values()), "edges": edges}
