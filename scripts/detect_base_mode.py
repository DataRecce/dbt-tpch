"""Detect whether a dbt project needs shared base or isolated base.

Analyzes manifest.json to classify the project and recommend the appropriate
base environment mode for Recce.

Usage:
    uv run python scripts/detect_base_mode.py                          # default: target/manifest.json
    uv run python scripts/detect_base_mode.py --manifest path/to/manifest.json
    uv run python scripts/detect_base_mode.py --json                   # machine-readable output

Detection signals:
    1. Incremental models          → strong signal for isolated base
    2. Sources with event_time     → enables --sample, makes isolated base feasible
    3. Materialization mix         → all views = shared base fine
    4. Model count / complexity    → large projects benefit more from isolation
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

    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        # Only count models from the root project, not packages
        if node.get("package_name") != manifest.get("metadata", {}).get("project_name"):
            continue

        mat = node.get("config", {}).get("materialized", "unknown")
        name = node.get("name", unique_id)
        schema_path = node.get("path", "")

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
            inc_config = {
                "name": name,
                "path": schema_path,
                "strategy": node.get("config", {}).get("incremental_strategy"),
                "unique_key": node.get("config", {}).get("unique_key"),
            }
            incremental_models.append(inc_config)

    return {
        "total_models": len(models),
        "materialization_counts": materialization_counts,
        "incremental_models": incremental_models,
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
    """Classify the project and recommend base mode."""
    signals = []
    recommendation = "shared_base"
    confidence = "high"

    inc_count = model_analysis["materialization_counts"]["incremental"]
    total = model_analysis["total_models"]
    table_count = model_analysis["materialization_counts"]["table"]
    view_count = model_analysis["materialization_counts"]["view"]
    event_time_pct = (
        source_analysis["with_event_time"] / source_analysis["total_sources"] * 100
        if source_analysis["total_sources"] > 0
        else 0
    )

    # Signal 1: Incremental models (strongest signal)
    if inc_count > 0:
        inc_pct = inc_count / total * 100
        signals.append({
            "signal": "incremental_models",
            "value": inc_count,
            "detail": f"{inc_count} incremental model(s) ({inc_pct:.0f}% of project)",
            "weight": "strong",
            "direction": "isolated_base",
            "reason": "Incremental models accumulate data over time. Shared base (production) "
                       "has full history while PR current has only recent data, causing "
                       "false alarms in row count and value diffs.",
        })
        recommendation = "isolated_base"
    else:
        signals.append({
            "signal": "incremental_models",
            "value": 0,
            "detail": "No incremental models found",
            "weight": "strong",
            "direction": "shared_base",
            "reason": "Without incremental models, base and current environments "
                       "produce the same data when given the same input.",
        })

    # Signal 2: Materialization profile
    if total > 0 and view_count == total:
        signals.append({
            "signal": "all_views",
            "value": True,
            "detail": "All models are views — no materialized data to diverge",
            "weight": "moderate",
            "direction": "shared_base",
            "reason": "Views are recomputed on read; no stored state to diverge between environments.",
        })
    elif table_count > 0:
        signals.append({
            "signal": "table_models",
            "value": table_count,
            "detail": f"{table_count} table model(s) — full refresh on each build",
            "weight": "weak",
            "direction": "shared_base",
            "reason": "Table models do full refresh. With the same source data, "
                       "base and current produce identical results.",
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
                           "to a consistent time window, making isolated base builds fast and deterministic.",
            })
        elif event_time_pct > 0:
            signals.append({
                "signal": "event_time_coverage",
                "value": f"{event_time_pct:.0f}%",
                "detail": f"{source_analysis['with_event_time']} of {source_analysis['total_sources']} "
                           f"sources have event_time",
                "weight": "weak",
                "direction": "partial_isolation",
                "reason": "Partial event_time coverage means --sample will only filter some sources. "
                           "Tables without event_time will still get full data.",
            })
        else:
            signals.append({
                "signal": "event_time_coverage",
                "value": "0%",
                "detail": "No sources have event_time configured",
                "weight": "moderate",
                "direction": "blocks_sample",
                "reason": "Without event_time on sources, --sample cannot be used. "
                           "Isolated base would require full rebuilds or alternative filtering.",
            })

    # Signal 4: Project scale
    if total > 50:
        signals.append({
            "signal": "project_scale",
            "value": total,
            "detail": f"{total} models — large project benefits more from isolation",
            "weight": "weak",
            "direction": "isolated_base",
            "reason": "Larger projects have more surface area for false alarms. "
                       "Isolated base reduces noise across all comparisons.",
        })

    # Determine confidence
    if inc_count > 0 and event_time_pct == 100:
        confidence = "high"
        recommendation = "isolated_base"
    elif inc_count > 0 and event_time_pct < 100:
        confidence = "medium"
        recommendation = "isolated_base"
    elif inc_count == 0:
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
        lines.append("  Your project has characteristics that cause false alarms")
        lines.append("  with a shared production base. Use isolated base mode")
        lines.append("  with --sample for accurate PR comparisons.")
    else:
        lines.append(f"  RECOMMENDATION: Shared Base ({conf} confidence)")
        lines.append("  Your project works well with the default shared base.")
        lines.append("  No special CI configuration needed.")

    lines.append("")
    lines.append("-" * 60)

    # Model summary
    mc = model_analysis["materialization_counts"]
    lines.append(f"  Models: {model_analysis['total_models']} total")
    lines.append(f"    table: {mc['table']}  view: {mc['view']}  "
                 f"ephemeral: {mc['ephemeral']}  incremental: {mc['incremental']}")
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

    # Incremental models
    if model_analysis["incremental_models"]:
        lines.append("  Incremental models:")
        for m in model_analysis["incremental_models"]:
            strategy = m.get("strategy") or "default"
            lines.append(f"    - {m['name']} (strategy: {strategy})")
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
