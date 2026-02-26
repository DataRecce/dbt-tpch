"""Compare row counts between base and current schemas.

Simulates Recce's row_count_diff check to demonstrate the difference between
shared base (same data) and isolated base (same sample window) scenarios.

Usage:
    uv run python scripts/compare_environments.py
    uv run python scripts/compare_environments.py --base-schema base --current-schema current
"""

import argparse

import psycopg2


def get_table_row_counts(conn, schema: str) -> dict[str, int]:
    """Get row counts for all tables and views in a schema."""
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW') "
        "ORDER BY table_name",
        (schema,),
    )
    relations = [(row[0], row[1]) for row in cur.fetchall()]

    counts = {}
    for name, rel_type in relations:
        cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')
        tag = "(view)" if rel_type == "VIEW" else ""
        counts[f"{name} {tag}".strip()] = cur.fetchone()[0]
    cur.close()
    return counts


def compare(base_counts: dict, current_counts: dict) -> list[dict]:
    """Compare row counts between base and current."""
    all_tables = sorted(set(base_counts.keys()) | set(current_counts.keys()))
    results = []

    for table in all_tables:
        base_n = base_counts.get(table, 0)
        current_n = current_counts.get(table, 0)
        diff = current_n - base_n
        pct = (diff / base_n * 100) if base_n > 0 else (100.0 if current_n > 0 else 0.0)

        results.append({
            "table": table,
            "base": base_n,
            "current": current_n,
            "diff": diff,
            "pct": pct,
            "status": "match" if diff == 0 else "MISMATCH",
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="Compare row counts between environments")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="dbt")
    parser.add_argument("--password", default="dbt")
    parser.add_argument("--dbname", default="tpch")
    parser.add_argument("--base-schema", default="base")
    parser.add_argument("--current-schema", default="current")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host, port=args.port, user=args.user, password=args.password, dbname=args.dbname
    )

    print(f"Comparing: {args.base_schema} vs {args.current_schema}")
    print(f"Database: {args.host}:{args.port}/{args.dbname}")
    print()

    base_counts = get_table_row_counts(conn, args.base_schema)
    current_counts = get_table_row_counts(conn, args.current_schema)
    results = compare(base_counts, current_counts)

    # Print results
    matches = sum(1 for r in results if r["status"] == "match")
    mismatches = sum(1 for r in results if r["status"] == "MISMATCH")

    print(f"{'Table':<40} {'Base':>10} {'Current':>10} {'Diff':>10} {'Result'}")
    print("-" * 90)

    for r in results:
        status_marker = "  " if r["status"] == "match" else "!!"
        label = f"{r['pct']:+.1f}% [{r['status']}]"
        print(
            f"{status_marker}{r['table']:<38} {r['base']:>10,} {r['current']:>10,} "
            f"{r['diff']:>+10,} {label}"
        )

    print("-" * 90)
    print(f"Total tables: {len(results)}  |  Matches: {matches}  |  Mismatches: {mismatches}")

    if mismatches == 0:
        print("\nResult: ZERO false alarms — all row counts match between environments.")
    else:
        print(f"\nResult: {mismatches} table(s) have row count differences.")
        print("These would appear as potential false alarms in Recce agent summaries.")

    conn.close()


if __name__ == "__main__":
    main()
