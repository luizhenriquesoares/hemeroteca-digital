"""Correção + extração unificada via GPT-4o para páginas com colunas misturadas."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import openai
from dotenv import load_dotenv

from src.config import TEXT_DIR, DATA_DIR

load_dotenv()
logger = logging.getLogger(__name__)

EXTRACTION_DIR = DATA_DIR / "extractions"

SYSTEM_PROMPT = """Você é historiador e paleógrafo especialista em jornais brasileiros do século XIX.
O texto tem MÚLTIPLAS COLUNAS misturadas pelo OCR. Separadores "|" indicam onde colunas se cruzaram.
Separe as colunas mentalmente e extraia entidades e relações corretamente."""

EXTRACTION_PROMPT = """Texto OCR de jornal brasileiro ({ano}), colunas misturadas. Separadores "|".

EXTRAIA em JSON válido todas as pessoas e relações:

```json
{{
  "corrected_text": "texto corrigido com colunas separadas (resumido, max 500 chars)",
  "entities": [
    {{"name": "nome completo", "type": "person|institution|place", "role": "cargo/título", "context": "réu|vítima|cônsul|morador|escravo|etc"}}
  ],
  "relations": [
    {{"subject": "nome", "predicate": "accused_of|victim_of|appointed_to|resident_of|spouse_of|child_of|widow_of|slave_of|absolved|signed_by|deceased", "object": "nome ou literal", "evidence": "trecho curto do texto"}}
  ]
}}
```

REGRAS:
- Capture TODAS as pessoas, mesmo sem cargo
- Separe sentenças judiciais (cada uma tem réu, vítima e resultado próprios)
- Identifique escravos e seus donos
- Capture relações familiares (viúva de, filho de)
- Capture moradores (morador em/no/na)
- Nomes COMPLETOS (não truncar)
- O JSON deve ser VÁLIDO

TEXTO:
{texto}"""

BIO_PATTERNS = re.compile(
    # Família
    r"filho|filha|viuva|viúva|casad[oa]|casamento|matrimonio|matrimônio|"
    r"noiva|noivo|desposou|batismo|baptismo|baptiz|nasceu|nascido|nascida|"
    # Morte/doença
    r"faleceu|falleceo|falecido|obito|óbito|enterr|sepulta|defunto|defunta|funeral|"
    r"doença|doente|molestia|moléstia|epidemia|febre|colera|cólera|lazareto|hospital|"
    # Judicial
    r"reo |réu |absolvid|condenad|sentença|tribunal|jury|promotor|crime|preso|prisão|"
    r"cadeia|pronunciad|denuncia|denúncia|querela|"
    # Cargos/nomeações
    r"nomead[oa]|eleit[oa]|presidente|governador|prefeito|inspector|commandante|"
    r"secretario|secretário|"
    # Residência/viagem
    r"morador|residente|domiciliad|embarcou|partiu|chegou|desembarcou|passageiro|"
    # Escravidão
    r"escravo|escrava|liberto|liberta|forro|alforria|fugido|fugida|"
    # Comércio/propriedade
    r"compra|venda|arremat|hipoteca|devedor|credor|fiador|"
    r"herança|herdeiro|inventario|inventário|testamento|"
    # Assinatura/documento
    r"assinado|assignado|requerimento|procuração|"
    # Militar
    r"desertor|desertou|recruta|destacamento|guarnição",
    re.IGNORECASE,
)


def find_dense_pages(bib: str | None = None, min_bio_hits: int = 5) -> list[dict]:
    """Encontra páginas densas (colunas + padrões biográficos)."""
    if bib:
        dirs = [TEXT_DIR / bib]
    else:
        dirs = [d for d in TEXT_DIR.iterdir() if d.is_dir()]

    pages = []
    for d in dirs:
        for f in sorted(d.glob("*.txt")):
            if f.name.endswith("_corrigido.txt"):
                continue
            content = f.read_text(encoding="utf-8", errors="ignore")
            words = content.split()
            if len(words) < 30:
                continue
            has_columns = content.count("|") > 5
            bio_hits = len(BIO_PATTERNS.findall(content))
            if has_columns and bio_hits >= min_bio_hits:
                # Ler metadados se existir
                meta_path = f.parent / f"{f.stem}.json"
                ano = "1837"
                if meta_path.exists():
                    try:
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                        ano = str(meta.get("ano", "1837")).replace("Ano ", "")
                    except Exception:
                        pass
                pages.append({
                    "path": f,
                    "bib": d.name,
                    "pagina": f.stem,
                    "bio_hits": bio_hits,
                    "chars": len(content),
                    "ano": ano,
                })

    pages.sort(key=lambda x: -x["bio_hits"])
    return pages


def extract_page(txt_path: Path, *, ano: str = "1837", model: str = "gpt-4o") -> dict:
    """Executa correção + extração unificada numa página."""
    content = txt_path.read_text(encoding="utf-8", errors="ignore")
    client = openai.OpenAI()

    all_entities = []
    all_relations = []
    corrected_parts = []

    # Dividir em partes se muito grande
    if len(content) > 6000:
        parts = [content[:6000], content[5500:]]
    else:
        parts = [content]

    for part in parts:
        prompt = EXTRACTION_PROMPT.format(ano=ano, texto=part)
        try:
            response = client.chat.completions.create(
                model=model,
                max_tokens=4000,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            )
            text = response.choices[0].message.content

            # Extrair JSON
            m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
            if m:
                data = json.loads(m.group(1))
                all_entities.extend(data.get("entities", []))
                all_relations.extend(data.get("relations", []))
                if data.get("corrected_text"):
                    corrected_parts.append(data["corrected_text"])
        except Exception as exc:
            logger.warning("Erro na extração de %s: %s", txt_path.name, exc)

    # Deduplicar entidades por nome normalizado
    seen_names = set()
    unique_entities = []
    for e in all_entities:
        key = e.get("name", "").strip().lower()
        if key and key not in seen_names:
            seen_names.add(key)
            unique_entities.append(e)

    return {
        "entities": unique_entities,
        "relations": all_relations,
        "corrected_text": "\n\n".join(corrected_parts),
    }


def run_batch_extraction(
    *,
    bib: str | None = None,
    min_bio_hits: int = 5,
    model: str = "gpt-4o",
    max_pages: int | None = None,
    force: bool = False,
) -> dict:
    """Roda extração unificada em batch nas páginas densas."""
    pages = find_dense_pages(bib=bib, min_bio_hits=min_bio_hits)
    if max_pages:
        pages = pages[:max_pages]

    out_dir = EXTRACTION_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    total_entities = 0
    total_relations = 0
    total_cost = 0.0
    processed = 0
    skipped = 0
    errors = 0

    logger.info("Extraindo %d páginas densas com %s...", len(pages), model)

    for idx, page in enumerate(pages, 1):
        out_file = out_dir / f"{page['bib']}_{page['pagina']}.json"

        if out_file.exists() and not force:
            skipped += 1
            continue

        try:
            t0 = time.time()
            result = extract_page(page["path"], ano=page["ano"], model=model)
            dt = time.time() - t0

            # Estimar custo
            chars = page["chars"]
            tokens = chars / 3.5
            cost = (tokens / 1e6 * 2.50) + (tokens / 1e6 * 10.00)
            total_cost += cost

            out_data = {
                "bib": page["bib"],
                "pagina": page["pagina"],
                "ano": page["ano"],
                "bio_hits": page["bio_hits"],
                "entities": result["entities"],
                "relations": result["relations"],
                "corrected_text": result["corrected_text"],
                "model": model,
                "cost_estimate": round(cost, 4),
            }
            out_file.write_text(
                json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            n_ent = len(result["entities"])
            n_rel = len(result["relations"])
            total_entities += n_ent
            total_relations += n_rel
            processed += 1

            if idx <= 5 or idx % 50 == 0:
                logger.info(
                    "[%d/%d] %s: %d ent, %d rel (%.1fs, ~$%.3f)",
                    idx, len(pages), page["pagina"], n_ent, n_rel, dt, cost,
                )
        except Exception as exc:
            errors += 1
            logger.error("[%d/%d] %s: ERRO %s", idx, len(pages), page["pagina"], exc)

    return {
        "pages_total": len(pages),
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "total_entities": total_entities,
        "total_relations": total_relations,
        "total_cost_estimate": round(total_cost, 2),
    }


def import_extractions_to_db() -> dict:
    """Importa resultados da extração unificada para o banco estruturado."""
    from src.structured.repository import StructuredRepository
    from src.structured.entities import normalize_name

    repo = StructuredRepository()
    ext_dir = EXTRACTION_DIR

    if not ext_dir.exists():
        return {"imported_entities": 0, "imported_relations": 0}

    imported_entities = 0
    imported_relations = 0

    for f in sorted(ext_dir.glob("*.json")):
        data = json.loads(f.read_text(encoding="utf-8"))
        bib = data.get("bib", "")
        pagina = data.get("pagina", "")

        with repo.connect() as conn:
            # Garantir que a página existe
            page = conn.execute(
                "SELECT id FROM pages WHERE bib = ? AND pagina = ?", (bib, pagina)
            ).fetchone()
            if not page:
                continue
            page_id = page["id"]

            for entity in data.get("entities", []):
                name = entity.get("name", "").strip()
                if not name or len(name) < 3:
                    continue
                etype = entity.get("type", "person")
                if etype not in ("person", "institution", "place"):
                    etype = "person"

                normalized = normalize_name(name)
                # Verificar se já existe
                existing = conn.execute(
                    "SELECT id FROM entities WHERE type = ? AND normalized_name = ?",
                    (etype, normalized),
                ).fetchone()

                if existing:
                    entity_id = existing["id"]
                else:
                    cursor = conn.execute(
                        """INSERT OR IGNORE INTO entities
                           (type, canonical_name, normalized_name, base_normalized_name, aliases_json, attributes_json)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (etype, name, normalized, normalized, json.dumps([name]), json.dumps({
                            "role": entity.get("role", ""),
                            "context": entity.get("context", ""),
                            "extraction_method": "gpt4o_unified",
                        })),
                    )
                    entity_id = cursor.lastrowid or existing["id"] if existing else cursor.lastrowid

                if entity_id:
                    conn.execute(
                        """INSERT OR IGNORE INTO entity_mentions
                           (entity_id, page_id, chunk_id, surface_form, snippet, confidence, source_text)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (entity_id, page_id, f"gpt4o_{pagina}", name,
                         (entity.get("context") or "")[:200], 0.9, ""),
                    )
                    imported_entities += 1

            for rel in data.get("relations", []):
                subject = rel.get("subject", "").strip()
                predicate = rel.get("predicate", "").strip()
                obj = rel.get("object", "").strip()
                evidence = rel.get("evidence", "").strip()

                if not subject or not predicate:
                    continue

                # Buscar entity IDs
                sub_norm = normalize_name(subject)
                sub_row = conn.execute(
                    "SELECT id FROM entities WHERE normalized_name = ? LIMIT 1", (sub_norm,)
                ).fetchone()
                if not sub_row:
                    continue

                obj_entity_id = None
                obj_literal = obj
                if predicate not in ("absolved", "deceased", "signed_by", "accused_of"):
                    obj_norm = normalize_name(obj)
                    obj_row = conn.execute(
                        "SELECT id FROM entities WHERE normalized_name = ? LIMIT 1", (obj_norm,)
                    ).fetchone()
                    if obj_row:
                        obj_entity_id = obj_row["id"]
                        obj_literal = ""

                conn.execute(
                    """INSERT OR IGNORE INTO relations
                       (subject_entity_id, predicate, object_entity_id, object_literal,
                        confidence, status, extraction_method)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (sub_row["id"], predicate, obj_entity_id, obj_literal,
                     0.85, "probable", "gpt4o_unified"),
                )

                if evidence:
                    rel_row = conn.execute(
                        """SELECT id FROM relations
                           WHERE subject_entity_id = ? AND predicate = ?
                             AND COALESCE(object_entity_id, 0) = COALESCE(?, 0)
                             AND object_literal = ?
                           LIMIT 1""",
                        (sub_row["id"], predicate, obj_entity_id, obj_literal),
                    ).fetchone()
                    if rel_row:
                        conn.execute(
                            """INSERT OR IGNORE INTO relation_evidence
                               (relation_id, page_id, chunk_id, quote, confidence)
                               VALUES (?, ?, ?, ?, ?)""",
                            (rel_row["id"], page_id, f"gpt4o_{pagina}", evidence[:200], 0.85),
                        )

                imported_relations += 1

    return {"imported_entities": imported_entities, "imported_relations": imported_relations}
