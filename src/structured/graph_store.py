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


# === Camadas semânticas para o grafo interativo ===

LAYER_PREDICATES = {
    "family": {"child_of", "parent_of", "widow_of", "spouse_of"},
    "roles": {"holds_role", "appointed_to", "captain_of", "indicated_for", "proposed", "admitted_to"},
    "justice": {"accused_of", "accused", "absolved", "victim_of", "witness_of", "defended", "opposed"},
    "slavery": {"slave_of", "fugitive", "owner_of", "freed_by"},
    "residence": {"resident_of", "location_of"},
    "events": {"deceased", "signed_by", "authored", "beneficiary_of", "observed"},
}

LAYER_LABELS = {
    "family": "Família",
    "roles": "Cargos",
    "justice": "Justiça",
    "slavery": "Escravidão",
    "residence": "Residência",
    "events": "Eventos",
    "co_mention": "Co-menção",
}

LAYER_COLORS = {
    "family": "#dc2626",
    "roles": "#2563eb",
    "justice": "#7c3aed",
    "slavery": "#b45309",
    "residence": "#0891b2",
    "events": "#059669",
    "co_mention": "#a8a29e",
}

PREDICATE_LABELS = {
    "child_of": "filho(a) de",
    "parent_of": "pai/mãe de",
    "widow_of": "viúva de",
    "spouse_of": "cônjuge de",
    "holds_role": "tem o cargo",
    "appointed_to": "nomeado para",
    "captain_of": "capitão de",
    "indicated_for": "indicado para",
    "proposed": "proposto",
    "admitted_to": "admitido em",
    "accused_of": "acusado de",
    "accused": "acusado",
    "absolved": "absolvido",
    "victim_of": "vítima de",
    "witness_of": "testemunha de",
    "defended": "defendeu",
    "opposed": "opôs-se a",
    "slave_of": "escravo(a) de",
    "fugitive": "fugitivo",
    "owner_of": "proprietário de",
    "freed_by": "libertado por",
    "resident_of": "morador em",
    "location_of": "localizado em",
    "deceased": "faleceu",
    "signed_by": "assinado por",
    "authored": "autor de",
    "beneficiary_of": "beneficiário de",
    "observed": "observado",
    "co_mention": "citados juntos",
}


def _predicate_to_layer(predicate: str) -> str | None:
    for layer, predicates in LAYER_PREDICATES.items():
        if predicate in predicates:
            return layer
    return None


def get_layered_graph(
    db_path: Path = STRUCTURED_DB,
    layers=None,
    limit: int = 30,
    min_shared_pages: int = 4,
    min_confidence: float = 0.4,
    focus_entity_id=None,
    focus_depth: int = 2,
) -> dict:
    """Grafo com filtro por camadas semânticas (família, cargos, justiça, etc).

    Cada aresta vem etiquetada com a camada e o predicado original. Co-menção
    só entra se a camada estiver explicitamente solicitada (é cara e ruidosa).

    Se ``focus_entity_id`` for fornecido, retorna um ego-network: a entidade
    central + vizinhança até ``focus_depth`` saltos, seguindo apenas as camadas
    selecionadas.
    """
    layers = layers or ["family", "roles", "co_mention"]
    layer_set = {l.strip() for l in layers if l.strip()}

    selected_predicates: set[str] = set()
    for layer in layer_set:
        selected_predicates |= LAYER_PREDICATES.get(layer, set())

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # === Modo ego-network (focus) ===
    if focus_entity_id:
        return _build_ego_network(
            conn,
            focus_entity_id=int(focus_entity_id),
            depth=max(1, min(int(focus_depth), 3)),
            layer_set=layer_set,
            selected_predicates=selected_predicates,
            min_shared_pages=min_shared_pages,
            min_confidence=min_confidence,
            limit=limit,
        )

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def _ensure_node(entity_id: int, name: str, etype: str = "person") -> str | None:
        if not _is_graph_legible(name) and etype == "person":
            return None
        nid = f"entity_{entity_id}"
        if nid not in nodes:
            nodes[nid] = {
                "id": nid,
                "entity_id": int(entity_id),
                "label": name,
                "type": etype,
                "mentions": 0,
                "role": "",
                "central": False,
                "layers": [],
            }
        return nid

    # 1. Carrega relações biográficas das camadas selecionadas
    if selected_predicates:
        placeholders = ",".join("?" * len(selected_predicates))
        rows = conn.execute(
            f"""
            SELECT r.id, r.subject_entity_id, r.object_entity_id, r.predicate,
                   r.object_literal, r.confidence,
                   se.canonical_name AS subject_name, se.type AS subject_type,
                   oe.canonical_name AS object_name, oe.type AS object_type
            FROM relations r
            JOIN entities se ON se.id = r.subject_entity_id
            LEFT JOIN entities oe ON oe.id = r.object_entity_id
            WHERE r.predicate IN ({placeholders})
              AND r.confidence >= ?
              AND LENGTH(se.canonical_name) >= 6
              AND r.object_entity_id IS NOT NULL
              AND LENGTH(oe.canonical_name) >= 6
            ORDER BY r.confidence DESC
            LIMIT ?
            """,
            (*selected_predicates, min_confidence, limit * 8),
        ).fetchall()

        for row in rows:
            layer = _predicate_to_layer(row["predicate"])
            if not layer:
                continue
            sid = _ensure_node(row["subject_entity_id"], row["subject_name"], row["subject_type"] or "person")
            oid = _ensure_node(row["object_entity_id"], row["object_name"], row["object_type"] or "person")
            if not sid or not oid:
                continue
            if layer not in nodes[sid]["layers"]:
                nodes[sid]["layers"].append(layer)
            if layer not in nodes[oid]["layers"]:
                nodes[oid]["layers"].append(layer)
            edges.append({
                "source": sid,
                "target": oid,
                "predicate": row["predicate"],
                "layer": layer,
                "label": PREDICATE_LABELS.get(row["predicate"], row["predicate"]),
                "weight": 3,
                "relation_id": int(row["id"]),
                "confidence": float(row["confidence"]),
            })

    # 2. Adiciona co-menção (se solicitado)
    if "co_mention" in layer_set:
        pairs = conn.execute(
            """
            SELECT
                e1.id AS id_a, e1.canonical_name AS name_a,
                e2.id AS id_b, e2.canonical_name AS name_b,
                COUNT(DISTINCT m1.page_id) AS shared_pages
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
            (min_shared_pages, limit * 4),
        ).fetchall()

        for pair in pairs:
            if len(nodes) >= limit * 2:
                break
            sid = _ensure_node(pair["id_a"], pair["name_a"])
            oid = _ensure_node(pair["id_b"], pair["name_b"])
            if not sid or not oid:
                continue
            if "co_mention" not in nodes[sid]["layers"]:
                nodes[sid]["layers"].append("co_mention")
            if "co_mention" not in nodes[oid]["layers"]:
                nodes[oid]["layers"].append("co_mention")
            edges.append({
                "source": sid,
                "target": oid,
                "predicate": "co_mention",
                "layer": "co_mention",
                "label": f"{pair['shared_pages']} págs.",
                "weight": int(pair["shared_pages"]),
            })

    # 3. Enriquece nós com cargo e contagem de menções
    if nodes:
        entity_ids = [n["entity_id"] for n in nodes.values()]
        id_list = ",".join(str(i) for i in entity_ids)
        for row in conn.execute(f"""
            SELECT entity_id, COUNT(*) AS c FROM entity_mentions
            WHERE entity_id IN ({id_list}) GROUP BY entity_id
        """).fetchall():
            nid = f"entity_{row['entity_id']}"
            if nid in nodes:
                nodes[nid]["mentions"] = int(row["c"])

        for row in conn.execute(f"""
            SELECT r.subject_entity_id, COALESCE(o.canonical_name, r.object_literal) AS role
            FROM relations r LEFT JOIN entities o ON o.id = r.object_entity_id
            WHERE r.predicate = 'holds_role' AND r.subject_entity_id IN ({id_list})
            ORDER BY r.confidence DESC
        """).fetchall():
            nid = f"entity_{row['subject_entity_id']}"
            if nid in nodes and not nodes[nid]["role"]:
                nodes[nid]["role"] = row["role"] or ""

    conn.close()

    # 4. Limita ao top N por grau (mais conexões)
    if len(nodes) > limit:
        degree: dict[str, int] = {}
        for e in edges:
            degree[e["source"]] = degree.get(e["source"], 0) + 1
            degree[e["target"]] = degree.get(e["target"], 0) + 1
        keep = set(sorted(degree, key=lambda nid: degree.get(nid, 0), reverse=True)[:limit])
        nodes = {k: v for k, v in nodes.items() if k in keep}
        edges = [e for e in edges if e["source"] in keep and e["target"] in keep]

    # 5. Marca o nó central (maior grau)
    if nodes:
        degree = {}
        for e in edges:
            degree[e["source"]] = degree.get(e["source"], 0) + 1
            degree[e["target"]] = degree.get(e["target"], 0) + 1
        if degree:
            nodes[max(degree, key=lambda nid: degree.get(nid, 0))]["central"] = True

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "layers_active": sorted(layer_set),
        "layer_meta": {
            layer: {
                "label": LAYER_LABELS.get(layer, layer),
                "color": LAYER_COLORS.get(layer, "#78716c"),
            }
            for layer in LAYER_LABELS
        },
    }


def get_edge_evidence(
    source_entity_id: int,
    target_entity_id: int,
    db_path: Path = STRUCTURED_DB,
) -> dict:
    """Encontra a página onde duas entidades aparecem juntas, com snippet, imagem
    e qualquer relação direta entre elas.

    Retorna:
        {
            "source": {id, name, type},
            "target": {id, name, type},
            "shared_pages": [
                {bib, pagina, jornal, ano, image_url, page_view_url, source_snippet, target_snippet}
            ],
            "direct_relations": [
                {predicate, label, confidence, evidence: {quote, page_view_url, ...}}
            ]
        }
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # 1. Metadados das entidades
    source = conn.execute(
        "SELECT id, canonical_name, type FROM entities WHERE id = ?", (source_entity_id,)
    ).fetchone()
    target = conn.execute(
        "SELECT id, canonical_name, type FROM entities WHERE id = ?", (target_entity_id,)
    ).fetchone()
    if not source or not target:
        conn.close()
        return {"error": "Entidade não encontrada"}

    # 2. Páginas compartilhadas (top 5 por densidade de menções)
    shared_pages_rows = conn.execute(
        """
        SELECT p.id AS page_id, p.bib, p.pagina, p.jornal, p.ano, p.edicao, p.image_path,
               m1.snippet AS source_snippet, m2.snippet AS target_snippet,
               COUNT(*) OVER (PARTITION BY p.id) AS row_count
        FROM entity_mentions m1
        JOIN entity_mentions m2 ON m2.page_id = m1.page_id
        JOIN pages p ON p.id = m1.page_id
        WHERE m1.entity_id = ? AND m2.entity_id = ?
        GROUP BY p.id
        ORDER BY p.id DESC
        LIMIT 5
        """,
        (source_entity_id, target_entity_id),
    ).fetchall()

    shared_pages = []
    for row in shared_pages_rows:
        shared_pages.append({
            "page_id": int(row["page_id"]),
            "bib": row["bib"],
            "pagina": str(row["pagina"]),
            "jornal": row["jornal"] or row["bib"],
            "ano": row["ano"] or "?",
            "edicao": row["edicao"] or "?",
            "page_view_url": _page_view_url(row["bib"], str(row["pagina"]), source["canonical_name"]),
            "image_api_url": f"/api/page/{row['bib']}/{row['pagina']}",
            "source_snippet": (row["source_snippet"] or "")[:300],
            "target_snippet": (row["target_snippet"] or "")[:300],
        })

    # 3. Relações diretas entre as duas entidades (qualquer direção)
    relation_rows = conn.execute(
        """
        SELECT r.id, r.predicate, r.subject_entity_id, r.object_entity_id, r.confidence
        FROM relations r
        WHERE (r.subject_entity_id = ? AND r.object_entity_id = ?)
           OR (r.subject_entity_id = ? AND r.object_entity_id = ?)
        ORDER BY r.confidence DESC
        LIMIT 6
        """,
        (source_entity_id, target_entity_id, target_entity_id, source_entity_id),
    ).fetchall()

    direct_relations = []
    for row in relation_rows:
        evidences = _load_relation_evidences(conn, int(row["id"]))
        primary = evidences[0] if evidences else None
        direct_relations.append({
            "relation_id": int(row["id"]),
            "predicate": row["predicate"],
            "label": PREDICATE_LABELS.get(row["predicate"], row["predicate"]),
            "direction": "source_to_target" if row["subject_entity_id"] == source_entity_id else "target_to_source",
            "confidence": float(row["confidence"]),
            "evidence": (
                {
                    "quote": (primary["quote"] or "")[:300],
                    "bib": primary["bib"],
                    "pagina": str(primary["pagina"]),
                    "jornal": primary["jornal"] or primary["bib"],
                    "ano": primary["ano"] or "?",
                    "page_view_url": _page_view_url(primary["bib"], str(primary["pagina"]), source["canonical_name"]),
                    "image_api_url": f"/api/page/{primary['bib']}/{primary['pagina']}",
                }
                if primary
                else None
            ),
        })

    conn.close()

    return {
        "source": {"id": int(source["id"]), "name": source["canonical_name"], "type": source["type"]},
        "target": {"id": int(target["id"]), "name": target["canonical_name"], "type": target["type"]},
        "shared_pages": shared_pages,
        "direct_relations": direct_relations,
    }


def _build_ego_network(
    conn: sqlite3.Connection,
    *,
    focus_entity_id: int,
    depth: int,
    layer_set: set[str],
    selected_predicates: set[str],
    min_shared_pages: int,
    min_confidence: float,
    limit: int,
) -> dict:
    """BFS a partir de uma entidade central, seguindo só predicados das camadas ativas.

    Retorna o mesmo formato de get_layered_graph (nodes/edges/layers_active),
    para que o renderer do front não precise mudar.
    """
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    seen_edges: set[tuple] = set()  # (source, target, predicate) para deduplicação

    # Carrega entidade central primeiro (para garantir que ela exista mesmo sem vizinhos)
    central = conn.execute(
        """
        SELECT e.id, e.canonical_name, e.type,
               COUNT(DISTINCT m.id) AS mentions
        FROM entities e
        LEFT JOIN entity_mentions m ON m.entity_id = e.id
        WHERE e.id = ?
        GROUP BY e.id
        """,
        (focus_entity_id,),
    ).fetchone()
    if not central:
        return {
            "nodes": [],
            "edges": [],
            "layers_active": sorted(layer_set),
            "focus": {"entity_id": focus_entity_id, "found": False},
        }

    central_nid = f"entity_{focus_entity_id}"
    nodes[central_nid] = {
        "id": central_nid,
        "entity_id": int(focus_entity_id),
        "label": central["canonical_name"],
        "type": central["type"] or "person",
        "mentions": int(central["mentions"] or 0),
        "role": "",
        "central": True,
        "layers": [],
    }

    visited: set[int] = {focus_entity_id}
    frontier: set[int] = {focus_entity_id}

    for hop in range(depth):
        if not frontier or len(nodes) >= limit:
            break
        next_frontier: set[int] = set()
        ids_csv = ",".join(str(i) for i in frontier)

        # 1. Relações biográficas das camadas selecionadas, que tocam a fronteira
        if selected_predicates:
            placeholders = ",".join("?" * len(selected_predicates))
            rows = conn.execute(
                f"""
                SELECT r.id, r.subject_entity_id, r.object_entity_id, r.predicate,
                       r.confidence,
                       se.canonical_name AS subject_name, se.type AS subject_type,
                       oe.canonical_name AS object_name, oe.type AS object_type
                FROM relations r
                JOIN entities se ON se.id = r.subject_entity_id
                LEFT JOIN entities oe ON oe.id = r.object_entity_id
                WHERE r.predicate IN ({placeholders})
                  AND r.confidence >= ?
                  AND r.object_entity_id IS NOT NULL
                  AND (r.subject_entity_id IN ({ids_csv}) OR r.object_entity_id IN ({ids_csv}))
                  AND LENGTH(se.canonical_name) >= 6
                  AND LENGTH(oe.canonical_name) >= 6
                ORDER BY r.confidence DESC
                LIMIT ?
                """,
                (*selected_predicates, min_confidence, limit * 4),
            ).fetchall()

            for row in rows:
                if len(nodes) >= limit:
                    break
                layer = _predicate_to_layer(row["predicate"])
                if not layer:
                    continue

                sid_int = int(row["subject_entity_id"])
                oid_int = int(row["object_entity_id"])

                # Adiciona nó se ainda não existe (e for legível)
                for eid, name, etype in [
                    (sid_int, row["subject_name"], row["subject_type"] or "person"),
                    (oid_int, row["object_name"], row["object_type"] or "person"),
                ]:
                    nid = f"entity_{eid}"
                    if nid not in nodes:
                        if etype == "person" and not _is_graph_legible(name):
                            continue
                        nodes[nid] = {
                            "id": nid,
                            "entity_id": eid,
                            "label": name,
                            "type": etype,
                            "mentions": 0,
                            "role": "",
                            "central": False,
                            "layers": [],
                        }
                        if eid not in visited:
                            next_frontier.add(eid)
                            visited.add(eid)

                sid_n = f"entity_{sid_int}"
                oid_n = f"entity_{oid_int}"
                if sid_n not in nodes or oid_n not in nodes:
                    continue
                edge_key = (sid_n, oid_n, row["predicate"])
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                if layer not in nodes[sid_n]["layers"]:
                    nodes[sid_n]["layers"].append(layer)
                if layer not in nodes[oid_n]["layers"]:
                    nodes[oid_n]["layers"].append(layer)
                edges.append({
                    "source": sid_n,
                    "target": oid_n,
                    "predicate": row["predicate"],
                    "layer": layer,
                    "label": PREDICATE_LABELS.get(row["predicate"], row["predicate"]),
                    "weight": 3,
                    "relation_id": int(row["id"]),
                    "confidence": float(row["confidence"]),
                })

        # 2. Co-menção (se solicitada) — só vizinhos diretos da fronteira atual
        if "co_mention" in layer_set and len(nodes) < limit:
            pairs = conn.execute(
                f"""
                SELECT
                    e1.id AS id_a, e1.canonical_name AS name_a,
                    e2.id AS id_b, e2.canonical_name AS name_b,
                    COUNT(DISTINCT m1.page_id) AS shared_pages
                FROM entity_mentions m1
                JOIN entity_mentions m2 ON m2.page_id = m1.page_id AND m2.entity_id != m1.entity_id
                JOIN entities e1 ON e1.id = m1.entity_id
                JOIN entities e2 ON e2.id = m2.entity_id
                WHERE (m1.entity_id IN ({ids_csv}) OR m2.entity_id IN ({ids_csv}))
                  AND e1.type = 'person' AND e2.type = 'person'
                  AND e1.id < e2.id
                  AND LENGTH(e1.canonical_name) >= 8
                  AND LENGTH(e2.canonical_name) >= 8
                GROUP BY m1.entity_id, m2.entity_id
                HAVING shared_pages >= ?
                ORDER BY shared_pages DESC
                LIMIT ?
                """,
                (min_shared_pages, limit * 2),
            ).fetchall()

            for pair in pairs:
                if len(nodes) >= limit:
                    break
                aid, bid = int(pair["id_a"]), int(pair["id_b"])

                for eid, name in [(aid, pair["name_a"]), (bid, pair["name_b"])]:
                    nid = f"entity_{eid}"
                    if nid not in nodes:
                        if not _is_graph_legible(name):
                            continue
                        nodes[nid] = {
                            "id": nid,
                            "entity_id": eid,
                            "label": name,
                            "type": "person",
                            "mentions": 0,
                            "role": "",
                            "central": False,
                            "layers": [],
                        }
                        if eid not in visited:
                            next_frontier.add(eid)
                            visited.add(eid)

                a_n, b_n = f"entity_{aid}", f"entity_{bid}"
                if a_n not in nodes or b_n not in nodes:
                    continue
                edge_key = (a_n, b_n, "co_mention")
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                if "co_mention" not in nodes[a_n]["layers"]:
                    nodes[a_n]["layers"].append("co_mention")
                if "co_mention" not in nodes[b_n]["layers"]:
                    nodes[b_n]["layers"].append("co_mention")
                edges.append({
                    "source": a_n,
                    "target": b_n,
                    "predicate": "co_mention",
                    "layer": "co_mention",
                    "label": f"{pair['shared_pages']} págs.",
                    "weight": int(pair["shared_pages"]),
                })

        frontier = next_frontier

    # Enriquece nós com cargo + contagem de menções
    if nodes:
        entity_ids = [n["entity_id"] for n in nodes.values()]
        id_list = ",".join(str(i) for i in entity_ids)
        for row in conn.execute(f"""
            SELECT entity_id, COUNT(*) AS c FROM entity_mentions
            WHERE entity_id IN ({id_list}) GROUP BY entity_id
        """).fetchall():
            nid = f"entity_{row['entity_id']}"
            if nid in nodes:
                nodes[nid]["mentions"] = int(row["c"])

        for row in conn.execute(f"""
            SELECT r.subject_entity_id, COALESCE(o.canonical_name, r.object_literal) AS role
            FROM relations r LEFT JOIN entities o ON o.id = r.object_entity_id
            WHERE r.predicate = 'holds_role' AND r.subject_entity_id IN ({id_list})
            ORDER BY r.confidence DESC
        """).fetchall():
            nid = f"entity_{row['subject_entity_id']}"
            if nid in nodes and not nodes[nid]["role"]:
                nodes[nid]["role"] = row["role"] or ""

    conn.close()

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "layers_active": sorted(layer_set),
        "layer_meta": {
            layer: {
                "label": LAYER_LABELS.get(layer, layer),
                "color": LAYER_COLORS.get(layer, "#78716c"),
            }
            for layer in LAYER_LABELS
        },
        "focus": {
            "entity_id": int(focus_entity_id),
            "name": central["canonical_name"],
            "depth": depth,
            "found": True,
        },
    }
