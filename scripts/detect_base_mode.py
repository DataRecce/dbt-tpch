"""Detect whether a dbt project needs shared base or isolated base.

Analyzes model SQL to classify the project and recommend the appropriate
base environment mode for Recce.

Usage:
    uv run python scripts/detect_base_mode.py                          # default: target/manifest.json
    uv run python scripts/detect_base_mode.py --manifest path/to/manifest.json
    uv run python scripts/detect_base_mode.py --json                   # machine-readable output

Root cause of false alarms:
    Conditional or non-deterministic logic in models produces DIFFERENT SQL
    depending on build context — target name, build time, existing table state.
    Two environments built under different conditions run different queries
    against the same source data, producing different results.

    This is NOT limited to incremental models. Any model (table, view, ephemeral)
    with conditional Jinja or non-deterministic functions can cause false alarms.
    Conversely, an incremental model with a deterministic else branch is SAFE
    in CI (both envs get fresh builds → same SQL).

Detection approach — SQL pattern scanning:
    Scans raw_code from manifest for patterns that make SQL non-deterministic:
        - target.name/schema  → different SQL per target environment
        - current_date()/now()→ different results per build time
    Note: is_incremental() and {{ this }} are NOT flagged by themselves —
    they only matter if combined with non-deterministic patterns above.
"""

import argparse
import json
import re
import sys
from pathlib import Path


# Jinja/SQL patterns that make SQL non-deterministic across environments.
#
# Key insight: is_incremental() and {{ this }} are NOT flagged here.
# In CI, both envs get fresh builds → is_incremental() returns false →
# {{ this }} is never reached. An incremental model with a deterministic
# else branch produces identical SQL in both environments.
#
# What actually causes false alarms is non-deterministic content INSIDE
# the branches: target.name, current_date(), etc.
CONDITIONAL_PATTERNS = [
    {
        "name": "target.name",
        "regex": re.compile(r"\btarget\s*\.\s*name\b"),
        "weight": "strong",
        "reason": "SQL varies by target environment — different targets produce different queries",
    },
    {
        "name": "target.schema",
        "regex": re.compile(r"\btarget\s*\.\s*schema\b"),
        "weight": "strong",
        "reason": "SQL varies by target schema — different targets produce different queries",
    },
    {
        "name": "current_date/now",
        "regex": re.compile(r"\b(current_date|current_timestamp|now\s*\(\s*\)|getdate\s*\(\s*\))\b", re.IGNORECASE),
        "weight": "moderate",
        "reason": "Result depends on build time — builds at different times produce different data",
    },
]


def load_manifest(path: str) -> dict:
    manifest_path = Path(path)
    if not manifest_path.exists():
        print(f"Error: manifest not found at {manifest_path}", file=sys.stderr)
        print("Run 'dbt parse' or 'dbt build' first to generate the manifest.", file=sys.stderr)
        sys.exit(1)
    with open(manifest_path) as f:
        return json.load(f)


def analyze_models(manifest: dict) -> dict:
    """Extract model metadata from manifest."""
    models = {}
    materialization_counts = {"table": 0, "view": 0, "ephemeral": 0, "incremental": 0, "other": 0}
    incremental_models = []
    snapshot_models = []

    project_name = manifest.get("metadata", {}).get("project_name")

    for unique_id, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type")
        if resource_type not in ("model", "snapshot"):
            continue
        if node.get("package_name") != project_name:
            continue

        name = node.get("name", unique_id)
        schema_path = node.get("path", "")

        if resource_type == "snapshot":
            snapshot_models.append({
                "name": name,
                "path": schema_path,
                "strategy": node.get("config", {}).get("strategy"),
            })
            continue

        mat = node.get("config", {}).get("materialized", "unknown")

        if mat in materialization_counts:
            materialization_counts[mat] += 1
        else:
            materialization_counts["other"] += 1

        models[name] = {
            "materialized": mat,
            "path": schema_path,
            "depends_on": node.get("depends_on", {}).get("nodes", []),
            "raw_code": node.get("raw_code", ""),
        }

        if mat == "incremental":
            incremental_models.append({
                "name": name,
                "path": schema_path,
                "strategy": node.get("config", {}).get("incremental_strategy"),
                "unique_key": node.get("config", {}).get("unique_key"),
            })

    return {
        "total_models": len(models),
        "materialization_counts": materialization_counts,
        "incremental_models": incremental_models,
        "snapshot_models": snapshot_models,
        "models": models,
    }


def strip_sql_comments(code: str) -> str:
    """Remove SQL line comments (--) and block comments (/* */) from code.

    Preserves Jinja comments ({# #}) since they're already stripped by dbt.
    This prevents false positives from keywords mentioned in comments like:
        -- no target.name or current_date() dependency
    """
    # Remove block comments
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.DOTALL)
    # Remove line comments (but not inside strings — good enough for PoC)
    code = re.sub(r"--[^\n]*", "", code)
    return code


def scan_sql_patterns(model_analysis: dict) -> list[dict]:
    """Scan ALL model SQL for non-deterministic patterns.

    Scans every model (including incremental) for patterns that make SQL
    vary by build context. An incremental model with a deterministic else
    branch will NOT be flagged — only those with target.name, current_date(),
    etc. in their code.

    SQL comments are stripped before scanning to avoid false positives.
    """
    findings = []

    for name, model in model_analysis["models"].items():
        raw_code = model.get("raw_code", "")
        if not raw_code:
            continue

        code_only = strip_sql_comments(raw_code)

        model_patterns = []
        for pattern in CONDITIONAL_PATTERNS:
            if pattern["regex"].search(code_only):
                model_patterns.append({
                    "pattern": pattern["name"],
                    "weight": pattern["weight"],
                    "reason": pattern["reason"],
                })

        if model_patterns:
            findings.append({
                "name": name,
                "materialized": model["materialized"],
                "path": model["path"],
                "patterns": model_patterns,
            })

    return findings


def analyze_sources(manifest: dict) -> dict:
    """Extract source metadata, focusing on event_time config."""
    sources = []
    sources_with_event_time = []
    sources_without_event_time = []

    for unique_id, source in manifest.get("sources", {}).items():
        name = source.get("name", unique_id)
        source_name = source.get("source_name", "unknown")
        event_time = source.get("config", {}).get("event_time")

        entry = {
            "name": name,
            "source": source_name,
            "event_time": event_time,
        }
        sources.append(entry)

        if event_time:
            sources_with_event_time.append(entry)
        else:
            sources_without_event_time.append(entry)

    return {
        "total_sources": len(sources),
        "with_event_time": len(sources_with_event_time),
        "without_event_time": len(sources_without_event_time),
        "sources_with_event_time": sources_with_event_time,
        "sources_without_event_time": sources_without_event_time,
    }


def classify(model_analysis: dict, source_analysis: dict, sql_findings: list[dict]) -> dict:
    """Classify the project and recommend base mode.

    Detection is based on SQL pattern scanning of ALL models (including
    incremental). An incremental model with a deterministic else branch
    is NOT flagged — only models with target.name, current_date(), etc.
    """
    signals = []
    recommendation = "shared_base"
    confidence = "high"

    conditional_count = len(sql_findings)
    total = model_analysis["total_models"]
    table_count = model_analysis["materialization_counts"]["table"]
    view_count = model_analysis["materialization_counts"]["view"]
    inc_count = model_analysis["materialization_counts"]["incremental"]
    snap_count = len(model_analysis["snapshot_models"])
    event_time_pct = (
        source_analysis["with_event_time"] / source_analysis["total_sources"] * 100
        if source_analysis["total_sources"] > 0
        else 0
    )

    # Signal 1: Models with non-deterministic SQL patterns
    if conditional_count > 0:
        model_details = []
        for f in sql_findings:
            patterns = ", ".join(p["pattern"] for p in f["patterns"])
            model_details.append(f"{f['name']} ({f['materialized']}: {patterns})")

        signals.append({
            "signal": "non_deterministic_sql",
            "value": conditional_count,
            "detail": f"{conditional_count} model(s) with non-deterministic SQL",
            "weight": "strong",
            "direction": "isolated_base",
            "reason": "These models contain target.name, current_date(), or other patterns "
                       "that produce different SQL per build context. "
                       "Models: " + "; ".join(model_details),
        })
        recommendation = "isolated_base"
    else:
        signals.append({
            "signal": "deterministic_sql",
            "value": 0,
            "detail": "All models produce deterministic SQL (no target.name, current_date, etc.)",
            "weight": "strong",
            "direction": "shared_base",
            "reason": "SQL scanning found no non-deterministic patterns. "
                       "Same source data → same result regardless of build context.",
        })

    # Signal 2: Incremental models with safe else branches (informational)
    safe_incremental = []
    for name, model in model_analysis["models"].items():
        if model["materialized"] == "incremental":
            is_flagged = any(f["name"] == name for f in sql_findings)
            if not is_flagged:
                safe_incremental.append(name)

    if safe_incremental:
        signals.append({
            "signal": "safe_incremental",
            "value": len(safe_incremental),
            "detail": f"{len(safe_incremental)} incremental model(s) with deterministic else branch — safe in CI",
            "weight": "moderate",
            "direction": "shared_base",
            "reason": f"These incremental models have no target.name or current_date() in "
                       f"their code. In CI (fresh builds), is_incremental() returns false → "
                       f"both envs run the same deterministic SQL. "
                       f"Models: {', '.join(safe_incremental)}",
        })

    # Signal 3: Materialization profile
    if total > 0 and view_count == total and conditional_count == 0:
        signals.append({
            "signal": "all_views",
            "value": True,
            "detail": "All models are views — recomputed on read, no stored state",
            "weight": "moderate",
            "direction": "shared_base",
            "reason": "Views with no non-deterministic logic generate deterministic SQL.",
        })
    elif table_count > 0 and conditional_count == 0:
        signals.append({
            "signal": "table_models",
            "value": table_count,
            "detail": f"{table_count} table model(s) — deterministic full refresh",
            "weight": "weak",
            "direction": "shared_base",
            "reason": "Table models with no non-deterministic logic generate the same SQL "
                       "every build.",
        })

    # Signal 4: event_time coverage (enables --sample feasibility)
    if source_analysis["total_sources"] > 0:
        if event_time_pct == 100:
            signals.append({
                "signal": "event_time_coverage",
                "value": f"{event_time_pct:.0f}%",
                "detail": f"All {source_analysis['total_sources']} sources have event_time configured",
                "weight": "moderate",
                "direction": "enables_isolation",
                "reason": "Full event_time coverage means --sample can filter all sources "
                           "to a consistent time window, making isolated base builds fast "
                           "and deterministic.",
            })
        elif event_time_pct > 0:
            signals.append({
                "signal": "event_time_coverage",
                "value": f"{event_time_pct:.0f}%",
                "detail": f"{source_analysis['with_event_time']} of {source_analysis['total_sources']} "
                           f"sources have event_time",
                "weight": "weak",
                "direction": "partial_isolation",
                "reason": "Partial event_time coverage means --sample will only filter some "
                           "sources. Sources without event_time will still get full data.",
            })
        else:
            signals.append({
                "signal": "event_time_coverage",
                "value": "0%",
                "detail": "No sources have event_time configured",
                "weight": "moderate",
                "direction": "blocks_sample",
                "reason": "Without event_time on sources, --sample cannot be used. "
                           "Isolated base would require full rebuilds.",
            })

    # Signal 5: Project scale
    if total > 50:
        signals.append({
            "signal": "project_scale",
            "value": total,
            "detail": f"{total} models — more surface area for false alarm noise",
            "weight": "weak",
            "direction": "isolated_base",
            "reason": "Larger projects amplify the impact: downstream models inherit "
                       "divergent data, spreading false alarm diffs across more tables.",
        })

    # Determine confidence
    if conditional_count > 0 and event_time_pct == 100:
        confidence = "high"
    elif conditional_count > 0:
        confidence = "medium"
    else:
        confidence = "high"

    # Populate by_materialization breakdown
    mat_breakdown = {}
    for f in sql_findings:
        mat = f["materialized"]
        mat_breakdown[mat] = mat_breakdown.get(mat, 0) + 1

    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": signals,
        "conditional_models": {
            "total": conditional_count,
            "by_materialization": mat_breakdown,
        },
    }


def format_report(
    model_analysis: dict, source_analysis: dict, sql_findings: list[dict],
    classification: dict,
) -> str:
    """Format a human-readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("  Recce Base Mode Detection Report (v2)")
    lines.append("=" * 70)
    lines.append("")

    # Recommendation
    rec = classification["recommendation"]
    conf = classification["confidence"]
    cond = classification["conditional_models"]
    if rec == "isolated_base":
        lines.append(f"  RECOMMENDATION: Isolated Base ({conf} confidence)")
        lines.append(f"  Found {cond['total']} model(s) with non-deterministic SQL")
        mat_parts = [f"{v} {k}" for k, v in cond["by_materialization"].items()]
        if mat_parts:
            lines.append(f"    breakdown: {', '.join(mat_parts)}")
        lines.append("")
        lines.append("  These models produce different SQL depending on build context.")
        lines.append("  Use isolated base so both environments build deterministically.")
    else:
        lines.append(f"  RECOMMENDATION: Shared Base ({conf} confidence)")
        lines.append("  No non-deterministic patterns found in any model.")
        lines.append("  All models produce deterministic SQL — shared base is fine.")

    lines.append("")
    lines.append("-" * 70)

    # Model summary
    mc = model_analysis["materialization_counts"]
    lines.append(f"  Models: {model_analysis['total_models']} total")
    lines.append(f"    table: {mc['table']}  view: {mc['view']}  "
                 f"ephemeral: {mc['ephemeral']}  incremental: {mc['incremental']}")
    if model_analysis["snapshot_models"]:
        lines.append(f"    snapshots: {len(model_analysis['snapshot_models'])}")
    lines.append("")

    # Source summary
    sa = source_analysis
    lines.append(f"  Sources: {sa['total_sources']} total")
    lines.append(f"    with event_time: {sa['with_event_time']}  "
                 f"without: {sa['without_event_time']}")
    if sa["without_event_time"] > 0:
        missing = [s["name"] for s in sa["sources_without_event_time"]]
        lines.append(f"    missing event_time: {', '.join(missing)}")
    lines.append("")

    # Models with non-deterministic SQL
    if sql_findings:
        lines.append("  Models with non-deterministic SQL:")
        for f in sql_findings:
            patterns = ", ".join(p["pattern"] for p in f["patterns"])
            lines.append(f"    - {f['name']} ({f['materialized']}) — {patterns}")
        lines.append("")

    # Safe incremental models (informational)
    safe_inc = [
        name for name, m in model_analysis["models"].items()
        if m["materialized"] == "incremental"
        and not any(f["name"] == name for f in sql_findings)
    ]
    if safe_inc:
        lines.append("  Safe incremental models (deterministic else branch):")
        for name in safe_inc:
            lines.append(f"    - {name} (incremental, no target.name/current_date)")
        lines.append("")

    # Signals
    lines.append("-" * 70)
    lines.append("  Detection signals:")
    lines.append("")
    for sig in classification["signals"]:
        icon = {"strong": "***", "moderate": "**", "weak": "*"}[sig["weight"]]
        direction = sig["direction"].replace("_", " ")
        lines.append(f"  {icon} [{direction}] {sig['detail']}")
        lines.append(f"      {sig['reason']}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Detect recommended Recce base mode")
    parser.add_argument(
        "--manifest",
        default="target/manifest.json",
        help="Path to dbt manifest.json (default: target/manifest.json)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output machine-readable JSON",
    )
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    model_analysis = analyze_models(manifest)
    source_analysis = analyze_sources(manifest)
    sql_findings = scan_sql_patterns(model_analysis)
    classification = classify(model_analysis, source_analysis, sql_findings)

    if args.json_output:
        output = {
            "models": {
                "total": model_analysis["total_models"],
                "materialization_counts": model_analysis["materialization_counts"],
                "incremental_models": model_analysis["incremental_models"],
                "snapshot_models": model_analysis["snapshot_models"],
            },
            "sources": {
                "total": source_analysis["total_sources"],
                "with_event_time": source_analysis["with_event_time"],
                "without_event_time": source_analysis["without_event_time"],
                "sources_without_event_time": [
                    s["name"] for s in source_analysis["sources_without_event_time"]
                ],
            },
            "sql_findings": sql_findings,
            "classification": classification,
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_report(model_analysis, source_analysis, sql_findings, classification))


if __name__ == "__main__":
    main()
