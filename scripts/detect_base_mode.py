"""Detect whether a dbt project needs shared base or isolated base.

Analyzes manifest.json to classify the project and recommend the appropriate
base environment mode for Recce.

Usage:
    uv run python scripts/detect_base_mode.py                          # default: target/manifest.json
    uv run python scripts/detect_base_mode.py --manifest path/to/manifest.json
    uv run python scripts/detect_base_mode.py --json                   # machine-readable output

Root cause of false alarms:
    Incremental models contain conditional logic (is_incremental()) that produces
    DIFFERENT SQL depending on build context — existing table state, build time,
    target name. Two environments built under different conditions run different
    queries against the same source data, producing different results.

    This is NOT about "data accumulation" or "data volume." It's about
    non-deterministic SQL generation from conditional Jinja logic.

Detection signals:
    1. Incremental/snapshot models  → contain is_incremental() conditional logic
    2. Sources with event_time      → enables --sample for deterministic windows
    3. Materialization mix          → all views/tables = deterministic output
    4. Model count / complexity     → larger projects amplify the false alarm noise
"""

import argparse
import json
import sys
from pathlib import Path


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
        # Only count models from the root project, not packages
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


def classify(model_analysis: dict, source_analysis: dict) -> dict:
    """Classify the project and recommend base mode.

    The core question: does the project contain models with conditional logic
    that makes SQL output dependent on build context (time, existing state,
    target)? If yes, two environments built under different conditions will
    produce different results — causing false alarm diffs in Recce.
    """
    signals = []
    recommendation = "shared_base"
    confidence = "high"

    inc_count = model_analysis["materialization_counts"]["incremental"]
    snap_count = len(model_analysis["snapshot_models"])
    conditional_count = inc_count + snap_count
    total = model_analysis["total_models"]
    table_count = model_analysis["materialization_counts"]["table"]
    view_count = model_analysis["materialization_counts"]["view"]
    event_time_pct = (
        source_analysis["with_event_time"] / source_analysis["total_sources"] * 100
        if source_analysis["total_sources"] > 0
        else 0
    )

    # Signal 1: Models with conditional logic (strongest signal)
    # Incremental models use is_incremental() which forks SQL based on:
    #   - Whether the target table already exists (state-dependent)
    #   - Often combined with current_date()/current_timestamp() (time-dependent)
    #   - Sometimes with target.name checks (target-dependent)
    # Snapshots use similar conditional logic for SCD history tracking.
    if conditional_count > 0:
        parts = []
        if inc_count > 0:
            parts.append(f"{inc_count} incremental")
        if snap_count > 0:
            parts.append(f"{snap_count} snapshot")
        detail = f"{' + '.join(parts)} model(s) with conditional logic"

        signals.append({
            "signal": "conditional_models",
            "value": conditional_count,
            "detail": detail,
            "weight": "strong",
            "direction": "isolated_base",
            "reason": "These models contain is_incremental() or snapshot logic that produces "
                       "different SQL depending on build context (existing table state, build "
                       "time, target name). Two environments built under different conditions "
                       "will run different queries → different results → false alarm diffs.",
        })
        recommendation = "isolated_base"
    else:
        signals.append({
            "signal": "conditional_models",
            "value": 0,
            "detail": "No incremental or snapshot models found",
            "weight": "strong",
            "direction": "shared_base",
            "reason": "Without conditional logic (is_incremental, snapshots), all models "
                       "produce deterministic SQL. Same source data → same result regardless "
                       "of when or where the build runs.",
        })

    # Signal 2: Materialization profile
    if total > 0 and view_count == total:
        signals.append({
            "signal": "all_views",
            "value": True,
            "detail": "All models are views — recomputed on read, no stored state",
            "weight": "moderate",
            "direction": "shared_base",
            "reason": "Views generate deterministic SQL with no conditional logic. "
                       "Output depends only on current source data, not build history.",
        })
    elif table_count > 0 and conditional_count == 0:
        signals.append({
            "signal": "table_models",
            "value": table_count,
            "detail": f"{table_count} table model(s) — deterministic full refresh",
            "weight": "weak",
            "direction": "shared_base",
            "reason": "Table models generate the same SQL every build (no conditional "
                       "logic). Same source data → identical results in any environment.",
        })

    # Signal 3: event_time coverage (enables --sample feasibility)
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

    # Signal 4: Project scale
    if total > 50:
        signals.append({
            "signal": "project_scale",
            "value": total,
            "detail": f"{total} models — more surface area for false alarm noise",
            "weight": "weak",
            "direction": "isolated_base",
            "reason": "Larger projects amplify the impact of conditional models: "
                       "downstream models inherit the divergent data, spreading "
                       "false alarm diffs across more tables.",
        })

    # Determine confidence
    if conditional_count > 0 and event_time_pct == 100:
        confidence = "high"
        recommendation = "isolated_base"
    elif conditional_count > 0 and event_time_pct < 100:
        confidence = "medium"
        recommendation = "isolated_base"
    elif conditional_count == 0:
        confidence = "high"
        recommendation = "shared_base"

    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": signals,
    }


def format_report(
    model_analysis: dict, source_analysis: dict, classification: dict
) -> str:
    """Format a human-readable report."""
    lines = []
    lines.append("=" * 60)
    lines.append("  Recce Base Mode Detection Report")
    lines.append("=" * 60)
    lines.append("")

    # Recommendation
    rec = classification["recommendation"]
    conf = classification["confidence"]
    if rec == "isolated_base":
        lines.append(f"  RECOMMENDATION: Isolated Base ({conf} confidence)")
        lines.append("  Your project has models with conditional logic")
        lines.append("  (is_incremental/snapshots) that produce different SQL")
        lines.append("  depending on build context. Use isolated base mode")
        lines.append("  so both environments run the same deterministic SQL.")
    else:
        lines.append(f"  RECOMMENDATION: Shared Base ({conf} confidence)")
        lines.append("  All models produce deterministic SQL — no conditional")
        lines.append("  logic that varies by build context. Shared base is fine.")

    lines.append("")
    lines.append("-" * 60)

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

    # Models with conditional logic
    if model_analysis["incremental_models"]:
        lines.append("  Models with conditional logic:")
        for m in model_analysis["incremental_models"]:
            strategy = m.get("strategy") or "default"
            lines.append(f"    - {m['name']} (incremental, strategy: {strategy})")
        for m in model_analysis["snapshot_models"]:
            strategy = m.get("strategy") or "default"
            lines.append(f"    - {m['name']} (snapshot, strategy: {strategy})")
        lines.append("")

    # Signals
    lines.append("-" * 60)
    lines.append("  Detection signals:")
    lines.append("")
    for sig in classification["signals"]:
        icon = {"strong": "***", "moderate": "**", "weak": "*"}[sig["weight"]]
        direction = sig["direction"].replace("_", " ")
        lines.append(f"  {icon} [{direction}] {sig['detail']}")
        lines.append(f"      {sig['reason']}")
        lines.append("")

    lines.append("=" * 60)
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
    classification = classify(model_analysis, source_analysis)

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
            "classification": classification,
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_report(model_analysis, source_analysis, classification))


if __name__ == "__main__":
    main()
