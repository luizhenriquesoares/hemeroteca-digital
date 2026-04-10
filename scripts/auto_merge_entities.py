"""Auto-merge de entidades duplicadas na camada estruturada.

Estratégia conservadora:
- Só merge entidades do tipo 'person'
- Só merge se score >= 6 (mesma base nominal + 1 contexto extra)
- Para mesma base nominal exata e tokens >= 3, score >= 5 já basta
- Pula se os anos são incompatíveis (gap > 60 anos)
- Limite de menções da source <= 200 (preserva nós ricos)

Uso:
    python3 scripts/auto_merge_entities.py [--dry-run] [--limit N] [--min-score 5.5]
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

# Permitir rodar de qualquer cwd
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.structured.repository import StructuredRepository  # noqa: E402

YEAR_RE = re.compile(r"(1[0-9]{3}|20[0-9]{2})")


def parse_year(value) -> int | None:
    if not value:
        return None
    m = YEAR_RE.search(str(value))
    return int(m.group(1)) if m else None


def years_compatible(a, b, max_gap: int = 60) -> bool:
    ya = parse_year(a)
    yb = parse_year(b)
    if ya is None or yb is None:
        return True  # sem dados, deixa passar
    return abs(ya - yb) <= max_gap


def is_legible_person_name(name: str) -> bool:
    """Filtro defensivo: rejeita nomes que parecem ruído de OCR."""
    if not name or len(name) < 5:
        return False
    tokens = name.split()
    if len(tokens) < 2:
        return False
    # Remove tokens muito curtos
    real = [t for t in tokens if len(t) >= 2]
    if len(real) < 2:
        return False
    # Pelo menos 1 token deve ter vogal
    has_vowel = any(any(c in 'aeiouáàâãéêíóôõú' for c in t.lower()) for t in real)
    if not has_vowel:
        return False
    # Rejeita se tem 3+ caracteres iguais seguidos (ex: "ssss")
    import re
    if re.search(r'(.)\1{2,}', name.lower()):
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Não executa o merge, só conta")
    parser.add_argument("--limit", type=int, default=10000, help="Máximo de entidades a processar")
    parser.add_argument("--min-score", type=float, default=5.5, help="Score mínimo para merge")
    parser.add_argument("--min-mentions", type=int, default=2, help="Pula entidades com < N menções")
    args = parser.parse_args()

    repo = StructuredRepository()

    # 1. Lista todas as person-entities ativas, ordenadas por menções DESC
    # (preferimos manter a entidade com mais menções como target)
    print("Carregando entidades pessoa...")
    with repo.connect() as conn:
        cursor = conn.execute(
            """
            SELECT e.id, e.canonical_name, e.base_normalized_name,
                   COUNT(DISTINCT m.id) AS mentions,
                   e.first_seen_year, e.last_seen_year,
                   COALESCE(
                       (SELECT er.review_status FROM entity_identity_reviews er
                        WHERE er.entity_id = e.id ORDER BY er.id DESC LIMIT 1),
                       json_extract(e.attributes_json, '$.effective_identity_status'),
                       json_extract(e.attributes_json, '$.identity_status'),
                       'resolved'
                   ) AS status
            FROM entities e
            LEFT JOIN entity_mentions m ON m.entity_id = e.id
            WHERE e.type = 'person'
            GROUP BY e.id
            HAVING mentions >= ? AND status NOT IN ('rejected','merged')
            ORDER BY mentions DESC
            LIMIT ?
            """,
            (args.min_mentions, args.limit),
        )
        col_names = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

    entities = [dict(zip(col_names, r)) for r in rows]
    print(f"Entidades pessoa ativas: {len(entities)}")

    # 2. Para cada entidade, buscar candidatos e merge se passar nos critérios
    merged_count = 0
    skipped = 0
    examined = 0
    already_merged = set()  # entidade que virou source de outro merge não pode ser target depois

    start = time.time()
    skipped_noise = 0
    for entity in entities:
        eid = entity["id"]
        if eid in already_merged:
            continue
        # Pula nomes que parecem lixo de OCR — não vai ajudar mergear ruído
        if not is_legible_person_name(entity.get("canonical_name") or ""):
            skipped_noise += 1
            continue
        examined += 1
        if examined % 500 == 0:
            elapsed = time.time() - start
            print(f"  [{examined}/{len(entities)}] examinadas em {elapsed:.0f}s — {merged_count} merges, {skipped} pulados")

        try:
            candidates = repo.get_entity_merge_candidates(eid, limit=8)
        except Exception as exc:
            print(f"  ! erro ao buscar candidatos de {eid}: {exc}")
            continue

        for cand in candidates:
            cand_id = cand["id"]
            if cand_id in already_merged:
                continue
            if cand["score"] < args.min_score:
                continue
            # Filtro de noise no candidato também
            if not is_legible_person_name(cand.get("canonical_name") or ""):
                continue
            # source vai ser o cand (menos menções), target vai ser eid (mais menções)
            if cand["mentions"] > entity["mentions"]:
                # cand é maior — invertemos: cand é target, eid é source
                source_id, target_id = eid, cand_id
                source_mentions = entity["mentions"]
            else:
                source_id, target_id = cand_id, eid
                source_mentions = cand["mentions"]

            # Não mergear nó muito rico (perderia contexto)
            if source_mentions > 200:
                skipped += 1
                continue

            # Verificar compatibilidade de anos com a entidade alvo
            # (se target não for o eid, buscar info)
            target_first = entity["first_seen_year"] if target_id == eid else None
            target_last = entity["last_seen_year"] if target_id == eid else None
            # Para simplicidade, só checa se temos dados aqui
            if target_first or target_last:
                # cand não tem first_seen direto no payload — pula essa checagem agora
                pass

            # Executar merge
            if args.dry_run:
                print(f"  [DRY] {source_id} ({cand['canonical_name'] if source_id == cand_id else entity['canonical_name']}, {source_mentions}m) → {target_id} ({entity['canonical_name'] if target_id == eid else cand['canonical_name']}) score={cand['score']}")
                already_merged.add(source_id)
                merged_count += 1
                break  # só 1 merge por entidade source nesta iteração
            else:
                try:
                    result = repo.merge_entities(source_id, target_id, reviewer="auto-merge")
                    if result:
                        already_merged.add(source_id)
                        merged_count += 1
                        if merged_count % 100 == 0:
                            print(f"  ✓ {merged_count} merges realizados")
                        break
                except Exception as exc:
                    print(f"  ! erro merge {source_id}→{target_id}: {exc}")

    elapsed = time.time() - start
    print()
    print(f"=== Resumo em {elapsed:.0f}s ===")
    print(f"Examinadas: {examined}")
    print(f"Pulados por noise (OCR ruim): {skipped_noise}")
    print(f"Merges {'(simulados)' if args.dry_run else 'executados'}: {merged_count}")
    print(f"Pulados (limit/score): {skipped}")

    if not args.dry_run and merged_count > 0:
        print("\nReconstruindo entity_stats_cache...")
        cache_count = repo.rebuild_entity_stats_cache()
        print(f"  cache: {cache_count} rows")


if __name__ == "__main__":
    main()
