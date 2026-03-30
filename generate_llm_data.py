import json
import re
import os
from pathlib import Path
from typing import List, Dict, Any

from config import parse_config
import utils

# Workload features and query plan feature loaders
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd

workload_features_dir = Path("workload_features")
query_plans_dir = Path("query_plans")
internal_metrics_dir = Path("internal_metrics")

_workload_cache = {}
_plans_cache = {}
_metrics_cache = {}

# Benchmark parsing patterns
bench_patterns = {
    "job": re.compile(r"job_(\d+)_hebo_output"),
    "tpch": re.compile(r"tpch_(\d+)_hebo_output"),
    "tpcds": re.compile(r"tpcds_(\d+)_hebo_output"),
    "ssb": re.compile(r"ssb_(\d+)_hebo_output"),
}


def parse_bench_and_idx(source_path: str):
    """Parse benchmark name and index from a source path."""
    p = Path(source_path)
    parts = [str(x) for x in p.parts]
    bench = None
    for b in ["job", "tpch", "tpcds", "ssb"]:
        if b in parts:
            bench = b
            break
    if bench is None:
        return None
    match = bench_patterns[bench].search(source_path)
    if not match:
        return None
    idx = int(match.group(1))
    return bench, idx


# Path helpers


def workload_features_path_for(bench: str, idx: int) -> Path:
    return workload_features_dir / bench / f"{bench}_{idx}_features.json"


def query_plans_path_for(bench: str, idx: int) -> Path:
    return query_plans_dir / bench / f"{bench}_{idx}_plans.json"


def internal_metrics_path_for(bench: str, idx: int) -> Path:
    return internal_metrics_dir / bench / f"{bench}_{idx}_internal_metrics.json"


# Loaders


def load_workload_features(bench: str, idx: int) -> dict:
    """Load simple workload-level features like size, ratios, avg lengths.
    Returns {} if missing or on error.
    """
    key = (bench, idx)
    if key in _workload_cache:
        return _workload_cache[key]
    p = workload_features_path_for(bench, idx)
    if not p.exists():
        _workload_cache[key] = {}
        return {}
    try:
        with open(p, "r") as f:
            data = json.load(f)
        # Ensure numeric values and safe defaults
        result = {}
        for k, v in data.items():
            try:
                result[k] = float(v)
            except Exception:
                # Keep non-numeric as-is if needed
                result[k] = v
    except Exception:
        result = {}
    _workload_cache[key] = result
    return result


# Plan vectorization
OP_RE = re.compile(r"([A-Za-z ]+)\(cost=([0-9.]+)\)")


def vectorize_plans(plan_strings: list[str]) -> dict:
    """Parse plan strings into engineered features.
    Produces operator counts/ratios, top-level cost stats, depth proxies, and binary flags.
    """
    op_counts: dict[str, int] = {}
    top_costs: list[float] = []
    depths: list[int] = []

    for s in plan_strings or []:
        ops = OP_RE.findall(s)
        if not ops:
            continue
        # Top-level operator's cost
        try:
            top_costs.append(float(ops[0][1]))
        except Exception:
            pass
        # Simple proxy for depth: parentheses count
        depths.append(s.count("("))
        for name, cost in ops:
            key = name.strip().lower().replace(" ", "_")
            op_counts[key] = op_counts.get(key, 0) + 1

    total_ops = sum(op_counts.values()) or 1
    ratios = {f"plan__ratio__{k}": (v / total_ops) for k, v in op_counts.items()}

    agg = {
        "plan__cost_mean": float(np.mean(top_costs)) if top_costs else 0.0,
        "plan__cost_std": float(np.std(top_costs)) if top_costs else 0.0,
        "plan__cost_min": float(np.min(top_costs)) if top_costs else 0.0,
        "plan__cost_max": float(np.max(top_costs)) if top_costs else 0.0,
        "plan__depth_mean": float(np.mean(depths)) if depths else 0.0,
        "plan__depth_max": float(np.max(depths)) if depths else 0.0,
        "plan__has_index_scan": int("index_scan" in op_counts),
        "plan__has_seq_scan": int("seq_scan" in op_counts),
        "plan__has_sort": int("sort" in op_counts),
        "plan__has_aggregate": int("aggregate" in op_counts),
        "plan__has_hash_join": int("hash_join" in op_counts),
        "plan__has_nested_loop": int("nested_loop" in op_counts),
        "plan__has_merge_join": int("merge_join" in op_counts),
        "plan__has_gather": int("gather" in op_counts),
        "plan__has_gather_merge": int("gather_merge" in op_counts),
        "plan__has_bitmap_scan": int(
            "bitmap_heap_scan" in op_counts or "bitmap_index_scan" in op_counts
        ),
    }
    counts = {f"plan__count__{k}": v for k, v in op_counts.items()}
    return {**counts, **ratios, **agg}


def load_query_plan_features(bench: str, idx: int) -> dict:
    """Load and parse query plan strings into engineered features.
    Returns {} if missing or on error.
    """
    key = (bench, idx)
    if key in _plans_cache:
        return _plans_cache[key]
    p = query_plans_path_for(bench, idx)
    if not p.exists():
        _plans_cache[key] = {}
        return {}
    try:
        with open(p, "r") as f:
            data = json.load(f)
        plans = data.get("query_plans", [])
        feats = vectorize_plans(plans)
    except Exception:
        feats = {}
    _plans_cache[key] = feats
    return feats


def load_internal_metrics(bench: str, idx: int) -> dict:
    """Load internal metrics for a workload.
    Returns {} if missing or on error.
    """
    key = (bench, idx)
    if key in _metrics_cache:
        return _metrics_cache[key]
    mp = internal_metrics_path_for(bench, idx)
    if not mp.exists():
        _metrics_cache[key] = {}
        return {}
    try:
        with open(mp, "r") as f:
            raw = json.load(f)
        # Flatten nested JSON structure
        flat = pd.json_normalize(raw, sep="__")
        metrics = flat.to_dict(orient="records")[0] if len(flat) else {}
    except Exception:
        metrics = {}
    _metrics_cache[key] = metrics
    return metrics


def find_best_config(bench: str, idx: int) -> dict:
    """Find the best configuration from HEBO output directory.
    Returns {} if not found or on error.
    """
    hebo_dir = Path(bench) / f"{bench}_{idx}_hebo_output"
    runhistory_path = hebo_dir / "runhistory.jsonl"

    if not runhistory_path.exists():
        return {}

    try:
        best_config = None
        best_cost = float("inf")

        with open(runhistory_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    cost = record.get("cost")
                    if cost is None:
                        continue

                    # Handle cost as list or scalar
                    if isinstance(cost, list) and len(cost) > 0:
                        cost = cost[0]
                    cost = abs(float(cost))  # Make positive

                    if cost < best_cost:
                        best_cost = cost
                        best_config = record.get("config", {})
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue

        return best_config if best_config is not None else {}
    except Exception:
        return {}


def main(benchmark_n: str = None, benchmark_t: str = None, database_name: str = None):
    """Main function to combine all workload data into JSON objects."""
    # Load configuration from config.ini
    config_path = "config/config.ini"
    print(f"Loading configuration from: {config_path}")
    config = parse_config.parse_args(config_path)

    # Get benchmark configuration
    benchmark_config = config.get("benchmark_config", {})
    benchmark_name = benchmark_n or benchmark_config.get("benchmark", "unknown")
    benchmark_type = benchmark_t or benchmark_config.get("type", "olap")

    print(f"Processing benchmark: {benchmark_name} ({benchmark_type})")

    # Find all workload indices for this benchmark
    bench_dir = Path(benchmark_name)
    if not bench_dir.exists():
        print(f"Benchmark directory not found: {bench_dir}")
        return

    # Find all HEBO output directories
    hebo_dirs = list(bench_dir.glob(f"{benchmark_name}_*_hebo_output"))
    if not hebo_dirs:
        print(f"No HEBO output directories found in {bench_dir}")
        return

    print(f"Found {len(hebo_dirs)} workload(s) to process")

    combined_data = []

    for hebo_dir in sorted(hebo_dirs):
        # Extract workload index
        match = bench_patterns[benchmark_name].search(str(hebo_dir))
        if not match:
            continue
        idx = int(match.group(1))

        print(f"\nProcessing {benchmark_name}_{idx}...")

        # Load all components
        best_config = find_best_config(benchmark_name, idx)
        internal_metrics = load_internal_metrics(benchmark_name, idx)
        workload_features = load_workload_features(benchmark_name, idx)
        query_plan_features = load_query_plan_features(benchmark_name, idx)

        # Check if we have data
        if not best_config:
            print(f"  Warning: No best config found for {benchmark_name}_{idx}")
        if not internal_metrics:
            print(f"  Warning: No internal metrics found for {benchmark_name}_{idx}")
        if not workload_features:
            print(f"  Warning: No workload features found for {benchmark_name}_{idx}")
        if not query_plan_features:
            print(f"  Warning: No query plan features found for {benchmark_name}_{idx}")

        # Combine into single object
        workload_data = {
            "benchmark": benchmark_name,
            "workload_idx": idx,
            "best_config": best_config,
            "internal_metrics": internal_metrics,
            "workload_features": workload_features,
            "query_plan_features": query_plan_features,
        }

        combined_data.append(workload_data)
        print(f"  Successfully combined data for {benchmark_name}_{idx}")

    # Save combined data to JSON file
    output_dir = Path("llm_data")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"{benchmark_name}_combined_data.json"

    with open(output_file, "w") as f:
        json.dump(combined_data, f, indent=2)

    print(f"\n{'='*80}")
    print(f"Successfully processed {len(combined_data)} workload(s)")
    print(f"Output saved to: {output_file}")
    print(f"{'='*80}")


if __name__ == "__main__":
    benchmark_dict = [
        {"benchmark": "job", "type": "olap", "database": "imdb"},
        {"benchmark": "ssb", "type": "olap", "database": "ssb"},
        {"benchmark": "tpcds", "type": "olap", "database": "tpcds"},
        {"benchmark": "tpch", "type": "olap", "database": "dss"},
    ]
    for bench in benchmark_dict:
        print("\n" + "#" * 100)
        print(
            f"Starting workload feature extraction for benchmark: {bench['benchmark']} ({bench['type']})"
        )
        print("#" * 100 + "\n")
        main(
            benchmark_n=bench["benchmark"],
            benchmark_t=bench["type"],
            database_name=bench["database"],
        )
