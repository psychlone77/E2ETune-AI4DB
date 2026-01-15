#!/usr/bin/env python3
"""
PostgreSQL Query Plan Extractor
Extracts query plans from workloads using EXPLAIN (FORMAT JSON) and generates compact summaries
"""

import json
import re
import os
import glob
from pathlib import Path
from typing import List, Dict, Any

from config import parse_config
from Database import Database
import utils


def clean_json_string(json_str):
    """
    Clean JSON string by removing '+' line continuation characters and normalizing whitespace.

    Args:
        json_str: Raw JSON string with potential '+' continuations

    Returns:
        Cleaned JSON string
    """
    # Remove '+' continuation characters followed by newline
    cleaned = re.sub(r"\+\s*\n", "", json_str)
    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def extract_node_summary(node, depth=0):
    """
    Recursively extract a compact summary of a query plan node.

    Args:
        node: Dictionary representing a plan node
        depth: Current recursion depth

    Returns:
        String representation of the node in format: NodeType(cost=X.X)(child1; child2; ...)
    """
    if not isinstance(node, dict):
        return ""

    node_type = node.get("Node Type", "Unknown")
    total_cost = node.get("Total Cost", 0)

    # Format: NodeType(cost=X.X)
    summary = f"{node_type}(cost={total_cost:.1f})"

    # Process child plans
    plans = node.get("Plans", [])
    if plans:
        child_summaries = []
        for child_plan in plans:
            child_summary = extract_node_summary(child_plan, depth + 1)
            if child_summary:
                child_summaries.append(child_summary)

        if child_summaries:
            # Join child summaries with semicolons
            summary += "(" + "; ".join(child_summaries) + ")"

    return summary


def parse_query_plan_json(plan_json: Dict[str, Any]) -> str:
    """
    Parse a single query plan JSON and extract compact summary.

    Args:
        plan_json: Dictionary containing the query plan

    Returns:
        Compact query plan summary string
    """
    if isinstance(plan_json, list) and len(plan_json) > 0:
        plan_data = plan_json[0]
    else:
        plan_data = plan_json

    if "Plan" in plan_data:
        return extract_node_summary(plan_data["Plan"])

    return ""


def read_queries_from_file(file_path: str) -> List[str]:
    """
    Read SQL queries from a file.
    Handles both regular SQL files and EXPLAIN-wrapped queries.

    Args:
        file_path: Path to SQL file

    Returns:
        List of SQL query strings
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Split by semicolons and filter empty queries
    queries = [q.strip() for q in content.split(";") if q.strip()]

    return queries


def wrap_with_explain(query: str) -> str:
    """
    Wrap a query with EXPLAIN (FORMAT JSON) if not already wrapped.

    Args:
        query: SQL query string

    Returns:
        Query wrapped with EXPLAIN
    """
    query_upper = query.upper().strip()

    # If already has EXPLAIN, return as is
    if query_upper.startswith("EXPLAIN"):
        return query

    # Wrap with EXPLAIN
    return f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) {query}"


def extract_plans_from_workload(
    db: Database, workload_file: str, benchmark_name: str
) -> List[str]:
    """
    Extract query plans from a workload file.

    Args:
        db: Database instance
        workload_file: Path to workload SQL file
        benchmark_name: Name of the benchmark

    Returns:
        List of compact query plan summaries
    """
    print(f"\nProcessing: {workload_file}")

    # Read queries from file
    queries = read_queries_from_file(workload_file)
    print(f"  Found {len(queries)} queries")

    # Wrap queries with EXPLAIN if needed
    explain_queries = [wrap_with_explain(q) for q in queries]
    # Execute queries and get plans
    conn = db.get_conn()
    cursor = conn.cursor()
    plan_summaries = []

    try:
        for i, query in enumerate(explain_queries, 1):
            try:
                print(f"  Executing query {i}/{len(explain_queries)}...")
                cursor.execute(query)
                result = cursor.fetchall()
                if result and len(result) > 0:
                    # PostgreSQL returns EXPLAIN JSON as a list of rows
                    # Each row contains a JSON string
                    plan_json = []
                    for row in result:
                        if row and len(row) > 0:
                            plan_json.append(row[0][0])
                    print(plan_json)
                    # If we have plan data, parse it
                    if plan_json:
                        # The result is typically a list with one element containing the plan
                        if isinstance(plan_json[0], dict):
                            print("Dict detected")
                            summary = parse_query_plan_json(plan_json)
                        elif isinstance(plan_json[0], str):
                            print("String detected")
                            # Sometimes it's returned as JSON string
                            parsed = json.loads(plan_json[0])
                            summary = parse_query_plan_json(parsed)
                        else:
                            print("Unknown format detected")
                            summary = parse_query_plan_json(plan_json)

                        if summary:
                            plan_summaries.append(summary)
                            print(
                                f"    ✓ Extracted plan (length: {len(summary)} chars)"
                            )
                        else:
                            print(f"    ⚠ Empty summary for query {i}")

            except Exception as e:
                print(f"    ✗ Error executing query {i}: {e}")
                continue

    finally:
        cursor.close()
        conn.close()

    print(f"  Extracted {len(plan_summaries)} query plans")
    return plan_summaries


def save_plans(query_plans: List[str], output_dir: Path, workload_name: str):
    """
    Save query plans to text and JSON files.

    Args:
        query_plans: List of query plan summaries
        output_dir: Output directory path
        workload_name: Name of the workload
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save as text file
    text_file = output_dir / f"{workload_name}_plans.txt"
    with open(text_file, "w", encoding="utf-8") as f:
        f.write(f"PostgreSQL Query Plan Summaries - {workload_name}\n")
        f.write("=" * 80 + "\n\n")

        for i, plan in enumerate(query_plans, 1):
            f.write(f"Query {i}:\n")
            f.write(f"{plan}\n")
            f.write("-" * 80 + "\n\n")

    print(f"  Saved text output to: {text_file}")

    # Save as JSON file
    json_file = output_dir / f"{workload_name}_plans.json"
    data = {
        "workload": workload_name,
        "query_plans": query_plans,
        "count": len(query_plans),
    }

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"  Saved JSON output to: {json_file}")


def main(benchmark_n: str = None, benchmark_t: str = None, database_name: str = None):
    """Main function to extract query plans from workloads."""
    # Load configuration from config.ini
    config_path = "config/config.ini"
    print(f"Loading configuration from: {config_path}")
    config = parse_config.parse_args(config_path)
    config["database_config"]["database"] = database_name or config[
        "database_config"
    ].get("database", "imdb")
    # Get benchmark configuration
    benchmark_config = config.get("benchmark_config", {})
    benchmark_name = benchmark_n or benchmark_config.get("benchmark", "unknown")
    benchmark_type = benchmark_t or benchmark_config.get("type", "olap")

    # Determine workload directory based on benchmark type
    if benchmark_type.lower() == "olap":
        workload_dir = "olap_workloads"
    else:
        workload_dir = benchmark_config.get("workload_path", "oltp_workloads")

    print(f"Benchmark: {benchmark_name}")
    print(f"Benchmark type: {benchmark_type}")
    print(f"Workload directory: {workload_dir}")

    # Initialize database
    print("Initializing database connection...")
    db = Database(
        config=config, knob_config_path=config["tuning_config"]["knob_config"]
    )

    all_files = os.listdir(workload_dir)

    if config["benchmark_config"].get("type", "olap") == "oltp":
        workloads = [
            f
            for f in all_files
            if f.__contains__(benchmark_name) and f.endswith(".xml")
        ]
    else:
        all_files = os.listdir(workload_dir)
        workloads = [
            f for f in all_files if f.startswith(benchmark_name) and f.endswith(".wg")
        ]

    # Sort workloads in natural (numeric-aware) order, e.g. job_2.wg before job_10.wg
    workloads = utils.natural_sort(workloads)

    total_workloads = len(workloads)
    print(f"Found {total_workloads} workloads matching prefix '{benchmark_name}'")

    # Create output directory
    output_dir = Path("query_plans") / benchmark_name
    print(f"Output directory: {output_dir}")
    workloads = utils.natural_sort(workloads)[:13]
    # Process each workload file
    for workload_file in workloads:
        workload_name = Path(workload_file).stem

        try:
            # Extract plans
            plans = extract_plans_from_workload(
                db, os.path.join(workload_dir, workload_file), benchmark_name
            )

            if plans:
                # Save plans
                save_plans(plans, output_dir, workload_name)
                print(f"✓ Successfully processed {workload_name}\n")
            else:
                print(f"⚠ No plans extracted from {workload_name}\n")

        except Exception as e:
            print(f"✗ Error processing {workload_file}: {e}\n")
            import traceback

            traceback.print_exc()

    print("\n" + "=" * 80)
    print("Query plan extraction complete!")
    print(f"Results saved to: {output_dir}")
    print("=" * 80)


if __name__ == "__main__":
    benchmark_dict = [
        # {"benchmark": "job", "type": "olap", "database": "imdb"},
        {"benchmark": "ssb", "type": "olap", "database": "ssb"},
        # {"benchmark": "tpcds", "type": "olap", "database": "tpcds"},
        {"benchmark": "tpch", "type": "oltp", "database": "dss"},
    ]
    for bench in benchmark_dict:
        print("\n" + "#" * 100)
        print(
            f"Starting query plan extraction for benchmark: {bench['benchmark']} ({bench['type']})"
        )
        print("#" * 100 + "\n")
        main(
            benchmark_n=bench["benchmark"],
            benchmark_t=bench["type"],
            database_name=bench["database"],
        )
