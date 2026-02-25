"""Generate TPC-H data and load into PostgreSQL.

Uses DuckDB's built-in TPC-H extension to generate data, then loads into Postgres.

Usage:
    uv run python scripts/generate_data_postgres.py              # SF1 (default)
    uv run python scripts/generate_data_postgres.py --sf 0.1     # SF0.1 (quick test)
    uv run python scripts/generate_data_postgres.py --sf 10      # SF10
"""

import argparse
import csv
import io
import time

import duckdb
import psycopg2


TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

# Postgres DDL for TPC-H tables (in raw schema)
TABLE_DDL = {
    "region": """
        CREATE TABLE raw.region (
            r_regionkey  INTEGER PRIMARY KEY,
            r_name       CHAR(25) NOT NULL,
            r_comment    VARCHAR(152)
        )
    """,
    "nation": """
        CREATE TABLE raw.nation (
            n_nationkey  INTEGER PRIMARY KEY,
            n_name       CHAR(25) NOT NULL,
            n_regionkey  INTEGER NOT NULL,
            n_comment    VARCHAR(152)
        )
    """,
    "supplier": """
        CREATE TABLE raw.supplier (
            s_suppkey    INTEGER PRIMARY KEY,
            s_name       CHAR(25) NOT NULL,
            s_address    VARCHAR(40) NOT NULL,
            s_nationkey  INTEGER NOT NULL,
            s_phone      CHAR(15) NOT NULL,
            s_acctbal    DECIMAL(15,2) NOT NULL,
            s_comment    VARCHAR(101)
        )
    """,
    "customer": """
        CREATE TABLE raw.customer (
            c_custkey    INTEGER PRIMARY KEY,
            c_name       VARCHAR(25) NOT NULL,
            c_address    VARCHAR(40) NOT NULL,
            c_nationkey  INTEGER NOT NULL,
            c_phone      CHAR(15) NOT NULL,
            c_acctbal    DECIMAL(15,2) NOT NULL,
            c_mktsegment CHAR(10) NOT NULL,
            c_comment    VARCHAR(117)
        )
    """,
    "part": """
        CREATE TABLE raw.part (
            p_partkey    INTEGER PRIMARY KEY,
            p_name       VARCHAR(55) NOT NULL,
            p_mfgr       CHAR(25) NOT NULL,
            p_brand      CHAR(10) NOT NULL,
            p_type       VARCHAR(25) NOT NULL,
            p_size       INTEGER NOT NULL,
            p_container  CHAR(10) NOT NULL,
            p_retailprice DECIMAL(15,2) NOT NULL,
            p_comment    VARCHAR(23)
        )
    """,
    "partsupp": """
        CREATE TABLE raw.partsupp (
            ps_partkey   INTEGER NOT NULL,
            ps_suppkey   INTEGER NOT NULL,
            ps_availqty  INTEGER NOT NULL,
            ps_supplycost DECIMAL(15,2) NOT NULL,
            ps_comment   VARCHAR(199),
            PRIMARY KEY (ps_partkey, ps_suppkey)
        )
    """,
    "orders": """
        CREATE TABLE raw.orders (
            o_orderkey      INTEGER PRIMARY KEY,
            o_custkey       INTEGER NOT NULL,
            o_orderstatus   CHAR(1) NOT NULL,
            o_totalprice    DECIMAL(15,2) NOT NULL,
            o_orderdate     DATE NOT NULL,
            o_orderpriority CHAR(15) NOT NULL,
            o_clerk         CHAR(15) NOT NULL,
            o_shippriority  INTEGER NOT NULL,
            o_comment       VARCHAR(79)
        )
    """,
    "lineitem": """
        CREATE TABLE raw.lineitem (
            l_orderkey      INTEGER NOT NULL,
            l_partkey       INTEGER NOT NULL,
            l_suppkey       INTEGER NOT NULL,
            l_linenumber    INTEGER NOT NULL,
            l_quantity      DECIMAL(15,2) NOT NULL,
            l_extendedprice DECIMAL(15,2) NOT NULL,
            l_discount      DECIMAL(15,2) NOT NULL,
            l_tax           DECIMAL(15,2) NOT NULL,
            l_returnflag    CHAR(1) NOT NULL,
            l_linestatus    CHAR(1) NOT NULL,
            l_shipdate      DATE NOT NULL,
            l_commitdate    DATE NOT NULL,
            l_receiptdate   DATE NOT NULL,
            l_shipinstruct  CHAR(25) NOT NULL,
            l_shipmode      CHAR(10) NOT NULL,
            l_comment       VARCHAR(44),
            PRIMARY KEY (l_orderkey, l_linenumber)
        )
    """,
}


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H data and load into PostgreSQL")
    parser.add_argument("--sf", type=float, default=1, help="Scale factor (default: 1)")
    parser.add_argument("--host", default="localhost", help="Postgres host (default: localhost)")
    parser.add_argument("--port", type=int, default=5432, help="Postgres port (default: 5432)")
    parser.add_argument("--user", default="dbt", help="Postgres user (default: dbt)")
    parser.add_argument("--password", default="dbt", help="Postgres password (default: dbt)")
    parser.add_argument("--dbname", default="tpch", help="Postgres database (default: tpch)")
    args = parser.parse_args()

    print(f"Generating TPC-H SF{args.sf} data via DuckDB...")
    start = time.time()

    # Generate data in DuckDB (in-memory)
    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL tpch; LOAD tpch;")
    duck.execute(f"CALL dbgen(sf={args.sf});")
    gen_time = time.time() - start
    print(f"  DuckDB generation: {gen_time:.1f}s")

    # Connect to Postgres
    conn = psycopg2.connect(
        host=args.host, port=args.port, user=args.user, password=args.password, dbname=args.dbname
    )
    cur = conn.cursor()

    # Create raw schema and tables
    cur.execute("DROP SCHEMA IF EXISTS raw CASCADE;")
    cur.execute("CREATE SCHEMA raw;")

    for table in TABLES:
        cur.execute(TABLE_DDL[table])
    conn.commit()

    # Load each table via COPY from CSV
    print(f"\nLoading into PostgreSQL ({args.host}:{args.port}/{args.dbname})...")
    for table in TABLES:
        t0 = time.time()
        rows = duck.execute(f"SELECT * FROM {table}").fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerows(rows)
        buf.seek(0)
        cur.copy_expert(f"COPY raw.{table} FROM STDIN WITH (FORMAT csv)", buf)
        conn.commit()

        print(f"  {table:<12} {len(rows):>12,} rows  ({time.time() - t0:.1f}s)")

    cur.close()
    conn.close()
    duck.close()

    print(f"\nTotal time: {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
