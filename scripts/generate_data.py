"""Generate TPC-H data using DuckDB's built-in extension.

Usage:
    uv run python scripts/generate_data.py          # SF1 (default, ~1GB)
    uv run python scripts/generate_data.py --sf 10   # SF10 (~10GB)
    uv run python scripts/generate_data.py --sf 0.1  # SF0.1 (~100MB, quick test)
"""

import argparse
import time

import duckdb


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H data in DuckDB")
    parser.add_argument(
        "--sf",
        type=float,
        default=1,
        help="Scale factor (default: 1 = ~1GB, 6M lineitem rows)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="tpch.duckdb",
        help="DuckDB database file path (default: tpch.duckdb)",
    )
    args = parser.parse_args()

    print(f"Generating TPC-H data with scale factor {args.sf} into {args.db}...")
    start = time.time()

    con = duckdb.connect(args.db)
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={args.sf});")

    # Print table row counts
    tables = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
    print(f"\nGeneration completed in {time.time() - start:.1f}s\n")
    print(f"{'Table':<12} {'Rows':>12}")
    print("-" * 26)
    for table in tables:
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table:<12} {count:>12,}")

    con.close()


if __name__ == "__main__":
    main()
