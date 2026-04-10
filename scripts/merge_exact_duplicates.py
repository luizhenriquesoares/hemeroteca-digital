"""Merge rápido de duplicatas EXATAS de canonical_name (sem heurística).

Para cada grupo de entidades pessoa com o mesmo canonical_name, escolhe a com
mais menções como TARGET e mergeia as demais nela. Não usa Python score — é só
um SELECT/JOIN bruto, então é rápido (~30s pra 30k grupos).

Uso:
    python3 scripts/merge_exact_duplicates.py [--dry-run] [--type person]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.structured.repository import StructuredRepository  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--type", default="person", help="Tipo (person, place, institution)")
    parser.add_argument("--min-mentions-target", type=int, default=1,
                        help="Mínimo de menções pro target ser elegível")
    args = parser.parse_args()

    repo = StructuredRepository()

    print(f"Buscando duplicatas exatas (type={args.type})...")
    with repo.connect() as conn:
        cursor = conn.execute(
            """
            SELECT canonical_name,
                   GROUP_CONCAT(id) AS ids,
                   COUNT(*) AS dup_count
            FROM entities
            WHERE type = ?
              AND COALESCE(
                    json_extract(attributes_json, '$.identity_status'),
                    'resolved'
              ) NOT IN ('rejected','merged')
            GROUP BY canonical_name
            HAVING dup_count >= 2
            ORDER BY dup_count DESC
            """,
            (args.type,),
        )
        groups = cursor.fetchall()

    print(f"Grupos com duplicata: {len(groups)}")
    total_dups = sum(g[2] for g in groups)
    print(f"Entidades duplicadas: {total_dups}")
    print(f"Merges potenciais: {total_dups - len(groups)}")

    if args.dry_run:
        print("\nTop 20 maiores grupos:")
        for name, ids, dup in groups[:20]:
            print(f"  {dup}x {name}")
        return

    # Para cada grupo: identificar target (mais menções) e mergear o resto
    print("\nExecutando merges...")
    start = time.time()
    merged = 0
    failed = 0

    for i, (name, ids_csv, dup_count) in enumerate(groups, start=1):
        ids = [int(x) for x in ids_csv.split(",")]

        # Buscar contagem de menções de cada um pra escolher o target
        with repo.connect() as conn:
            id_list = ",".join(str(x) for x in ids)
            mention_rows = conn.execute(
                f"""
                SELECT entity_id, COUNT(*) AS c
                FROM entity_mentions WHERE entity_id IN ({id_list})
                GROUP BY entity_id
                """
            ).fetchall()
        mentions_map = {int(r[0]): int(r[1]) for r in mention_rows}
        # Garantir que todos os ids estão no map
        for x in ids:
            mentions_map.setdefault(x, 0)

        # Target = maior número de menções (desempate por id menor)
        target_id = max(ids, key=lambda x: (mentions_map[x], -x))
        if mentions_map[target_id] < args.min_mentions_target:
            # Sem menções nem no target — pula esse grupo
            continue

        # Mergear todos os outros nele
        for source_id in ids:
            if source_id == target_id:
                continue
            try:
                result = repo.merge_entities(source_id, target_id, reviewer="auto-exact")
                if result:
                    merged += 1
                else:
                    failed += 1
            except Exception as exc:
                failed += 1
                if failed < 10:
                    print(f"  ! erro {source_id}→{target_id}: {exc}")

        if i % 500 == 0:
            elapsed = time.time() - start
            rate = i / elapsed if elapsed else 0
            eta = (len(groups) - i) / rate if rate else 0
            print(f"  [{i}/{len(groups)}] grupos · {merged} merges · {elapsed:.0f}s · ETA {eta:.0f}s")

    elapsed = time.time() - start
    print(f"\n=== Concluído em {elapsed:.0f}s ===")
    print(f"Grupos processados: {len(groups)}")
    print(f"Merges realizados: {merged}")
    print(f"Falhas: {failed}")

    if merged > 0:
        print("\nReconstruindo entity_stats_cache...")
        cache = repo.rebuild_entity_stats_cache()
        print(f"  cache: {cache} rows")


if __name__ == "__main__":
    main()
