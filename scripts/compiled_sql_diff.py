"""Detect non-deterministic models by diffing compiled SQL across targets.

Approach 1 from DRC-2863: Compile dbt under two targets, normalize schema
names out of the SQL, then diff. Any model with remaining differences has
non-deterministic SQL that varies by build context.

Usage:
    # First compile under both targets:
    #   dbt compile --target pg-base   && cp -r target/compiled target/compiled_pg_base
    #   dbt compile --target pg-current && cp -r target/compiled target/compiled_pg_current
    #
    # Then run:
    uv run python scripts/compiled_sql_diff.py
    uv run python scripts/compiled_sql_diff.py --base-dir target/compiled_pg_base --current-dir target/compiled_pg_current
    uv run python scripts/compiled_sql_diff.py --json

Alternatively, use manifest.json compiled_code (requires dbt compile, not just dbt parse):
    uv run python scripts/compiled_sql_diff.py --use-manifest --base-manifest target_base/manifest.json --current-manifest target_current/manifest.json
"""

import argparse
import difflib
import json
import re
import sys
from pathlib import Path


def strip_sql_comments(sql: str) -> str:
    """Remove SQL comments to avoid false positives from comment text."""
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def normalize_sql(
    sql: str,
    db_name: str = "",
    schema_name: str = "",
    strip_comments: bool = True,
    strip_batch_metadata: bool = True,
) -> str:
    """Normalize compiled SQL to remove expected target-specific differences.

    Performs precise schema replacement: only in qualified ref positions like
    `db.schema.table` or `"schema"."table"`, NOT in SQL keywords like CURRENT ROW.

    Also strips dbt batch metadata (dbt_batch_id, dbt_batch_ts) since these
    are compile-time artifacts that always differ between runs.
    """
    normalized = sql

    if strip_comments:
        normalized = strip_sql_comments(normalized)

    if db_name and schema_name:
        # Replace db.schema. prefix (most common pattern in compiled SQL)
        normalized = re.sub(
            rf"\b{re.escape(db_name)}\.{re.escape(schema_name)}\.",
            f"{db_name}.__SCHEMA__.",
            normalized,
        )
        # Replace "schema"."table" pattern (quoted identifiers)
        normalized = re.sub(
            rf'"{re.escape(schema_name)}"',
            '"__SCHEMA__"',
            normalized,
        )

    if strip_batch_metadata:
        # dbt_batch_id and dbt_batch_ts are compile-time UUIDs/timestamps
        normalized = re.sub(
            r"cast\('[0-9a-f-]+' as varchar\) as dbt_batch_id",
            "cast('__BATCH_ID__' as varchar) as dbt_batch_id",
            normalized,
        )
        normalized = re.sub(
            r"cast\('[^']+' as timestamp\) as dbt_batch_ts",
            "cast('__BATCH_TS__' as timestamp) as dbt_batch_ts",
            normalized,
        )

    # Normalize whitespace for cleaner diffs
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    return normalized


def diff_compiled_files(
    base_dir: Path,
    current_dir: Path,
    db_name: str,
    base_schema: str,
    current_schema: str,
) -> list[dict]:
    """Diff compiled SQL files between two target directories."""

    findings = []
    base_files = sorted(base_dir.rglob("*.sql"))

    for base_file in base_files:
        rel_path = base_file.relative_to(base_dir)
        current_file = current_dir / rel_path

        if not current_file.exists():
            findings.append({
                "model": rel_path.stem,
                "path": str(rel_path),
                "status": "missing_in_current",
                "diff_lines": [],
            })
            continue

        base_sql = normalize_sql(base_file.read_text(), db_name=db_name, schema_name=base_schema)
        current_sql = normalize_sql(current_file.read_text(), db_name=db_name, schema_name=current_schema)

        if base_sql == current_sql:
            continue

        # Generate unified diff for the non-schema differences
        diff = list(difflib.unified_diff(
            base_sql.splitlines(keepends=True),
            current_sql.splitlines(keepends=True),
            fromfile=f"base/{rel_path}",
            tofile=f"current/{rel_path}",
            lineterm="",
        ))

        # Extract only the changed lines (+ and - prefixed)
        changed_lines = [
            line for line in diff
            if line.startswith("+") or line.startswith("-")
            if not line.startswith("+++") and not line.startswith("---")
        ]

        findings.append({
            "model": rel_path.stem,
            "path": str(rel_path),
            "status": "non_deterministic",
            "diff_lines": changed_lines,
            "diff_full": diff,
        })

    # Check for files only in current
    current_files = sorted(current_dir.rglob("*.sql"))
    current_rels = {f.relative_to(current_dir) for f in current_files}
    base_rels = {f.relative_to(base_dir) for f in base_files}

    for rel_path in sorted(current_rels - base_rels):
        findings.append({
            "model": rel_path.stem,
            "path": str(rel_path),
            "status": "missing_in_base",
            "diff_lines": [],
        })

    return findings


def diff_manifests(
    base_manifest_path: str,
    current_manifest_path: str,
    db_name: str,
    base_schema: str,
    current_schema: str,
) -> list[dict]:
    """Diff compiled_code from two manifest.json files."""

    with open(base_manifest_path) as f:
        base_manifest = json.load(f)
    with open(current_manifest_path) as f:
        current_manifest = json.load(f)

    project_name = base_manifest.get("metadata", {}).get("project_name")

    findings = []

    # Build lookup for current manifest
    current_nodes = {}
    for uid, node in current_manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model" and node.get("package_name") == project_name:
            current_nodes[uid] = node

    for uid, node in base_manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model" or node.get("package_name") != project_name:
            continue

        name = node.get("name", uid)
        base_compiled = node.get("compiled_code", "")

        current_node = current_nodes.get(uid)
        if not current_node:
            findings.append({
                "model": name,
                "path": node.get("path", ""),
                "status": "missing_in_current",
                "diff_lines": [],
            })
            continue

        current_compiled = current_node.get("compiled_code", "")

        if not base_compiled or not current_compiled:
            continue

        base_norm = normalize_sql(base_compiled, db_name=db_name, schema_name=base_schema)
        current_norm = normalize_sql(current_compiled, db_name=db_name, schema_name=current_schema)

        if base_norm == current_norm:
            continue

        diff = list(difflib.unified_diff(
            base_norm.splitlines(keepends=True),
            current_norm.splitlines(keepends=True),
            fromfile=f"base/{name}",
            tofile=f"current/{name}",
            lineterm="",
        ))

        changed_lines = [
            line for line in diff
            if line.startswith("+") or line.startswith("-")
            if not line.startswith("+++") and not line.startswith("---")
        ]

        findings.append({
            "model": name,
            "path": node.get("path", ""),
            "materialized": node.get("config", {}).get("materialized", "unknown"),
            "status": "non_deterministic",
            "diff_lines": changed_lines,
            "diff_full": diff,
        })

    return findings


def format_report(findings: list[dict], approach: str) -> str:
    """Format human-readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("  Compiled SQL Diff — Non-Deterministic Model Detection")
    lines.append(f"  Approach: {approach}")
    lines.append("=" * 70)
    lines.append("")

    non_det = [f for f in findings if f["status"] == "non_deterministic"]
    identical = len(findings) - len(non_det) - len([f for f in findings if f["status"].startswith("missing")])
    missing = [f for f in findings if f["status"].startswith("missing")]

    lines.append(f"  Non-deterministic models: {len(non_det)}")
    lines.append(f"  Identical models (after schema normalization): NOT directly counted — {len(findings)} total checked")
    if missing:
        lines.append(f"  Missing in one target: {len(missing)}")
    lines.append("")

    if non_det:
        lines.append("-" * 70)
        lines.append("  Models with non-deterministic SQL:")
        lines.append("")
        for f in non_det:
            mat = f.get("materialized", "")
            mat_str = f" ({mat})" if mat else ""
            lines.append(f"  !! {f['model']}{mat_str}  [{f['path']}]")
            for dl in f["diff_lines"][:10]:
                lines.append(f"       {dl}")
            if len(f["diff_lines"]) > 10:
                lines.append(f"       ... ({len(f['diff_lines'])} changed lines total)")
            lines.append("")

    if not non_det:
        lines.append("  All models produce identical SQL across targets (after schema normalization).")
        lines.append("  Shared base is safe.")

    lines.append("=" * 70)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Detect non-deterministic models via compiled SQL diff")
    parser.add_argument("--base-dir", default="target/compiled_pg_base",
                        help="Directory with compiled SQL from base target")
    parser.add_argument("--current-dir", default="target/compiled_pg_current",
                        help="Directory with compiled SQL from current target")
    parser.add_argument("--db-name", default="tpch",
                        help="Database name used in qualified refs (e.g., tpch)")
    parser.add_argument("--base-schema", default="base",
                        help="Schema name used by base target")
    parser.add_argument("--current-schema", default="current",
                        help="Schema name used by current target")
    parser.add_argument("--use-manifest", action="store_true",
                        help="Use manifest.json compiled_code instead of file-based diff")
    parser.add_argument("--base-manifest", default="target_base/manifest.json",
                        help="Path to base manifest.json (with --use-manifest)")
    parser.add_argument("--current-manifest", default="target_current/manifest.json",
                        help="Path to current manifest.json (with --use-manifest)")
    parser.add_argument("--json", action="store_true", dest="json_output",
                        help="Output machine-readable JSON")
    args = parser.parse_args()

    if args.use_manifest:
        findings = diff_manifests(
            args.base_manifest,
            args.current_manifest,
            db_name=args.db_name,
            base_schema=args.base_schema,
            current_schema=args.current_schema,
        )
        approach = "manifest compiled_code diff"
    else:
        base_dir = Path(args.base_dir)
        current_dir = Path(args.current_dir)

        if not base_dir.exists():
            print(f"Error: base directory not found: {base_dir}", file=sys.stderr)
            print("Run: dbt compile --target pg-base && cp -r target/compiled target/compiled_pg_base", file=sys.stderr)
            sys.exit(1)
        if not current_dir.exists():
            print(f"Error: current directory not found: {current_dir}", file=sys.stderr)
            print("Run: dbt compile --target pg-current && cp -r target/compiled target/compiled_pg_current", file=sys.stderr)
            sys.exit(1)

        findings = diff_compiled_files(
            base_dir, current_dir,
            db_name=args.db_name,
            base_schema=args.base_schema,
            current_schema=args.current_schema,
        )
        approach = "compiled file diff"

    if args.json_output:
        output = {
            "approach": approach,
            "total_findings": len(findings),
            "non_deterministic": [
                {
                    "model": f["model"],
                    "path": f["path"],
                    "materialized": f.get("materialized", ""),
                    "diff_lines": f["diff_lines"],
                }
                for f in findings
                if f["status"] == "non_deterministic"
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_report(findings, approach))


if __name__ == "__main__":
    main()
