#!/usr/bin/env python3
"""
PostgreSQL Query Plan Extractor
Extracts query plans from workloads using EXPLAIN (FORMAT JSON) and generates compact summaries
"""

import json
import re
import os
import glob
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any, Set

from config import parse_config
from Database import Database
from benchbase_runner import BenchBaseRunner
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


def extract_plans_from_workload(db: Database, workload_file: str) -> List[str]:
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


def configure_postgres_logging(db: Database) -> bool:
    """
    Enable PostgreSQL query logging to capture all executed queries.

    Args:
        db: Database instance

    Returns:
        True if successful, False otherwise
    """
    conn = db.get_conn()
    old_autocommit = conn.autocommit
    conn.autocommit = True  # ALTER SYSTEM cannot run in a transaction block
    cursor = conn.cursor()

    try:
        print("Configuring PostgreSQL logging...")
        cursor.execute("ALTER SYSTEM SET log_statement = 'all'")
        cursor.execute("ALTER SYSTEM SET log_min_duration_statement = 0")
        cursor.execute("ALTER SYSTEM SET log_line_prefix = '%m [%p] '")
        cursor.execute("SELECT pg_reload_conf()")
        print("  ✓ PostgreSQL logging enabled")
        return True
    except Exception as e:
        print(f"  ✗ Failed to configure logging: {e}")
        return False
    finally:
        cursor.close()
        conn.autocommit = old_autocommit
        conn.close()


def disable_postgres_logging(db: Database) -> bool:
    """
    Disable PostgreSQL query logging to reduce log size.

    Args:
        db: Database instance

    Returns:
        True if successful, False otherwise
    """
    conn = db.get_conn()
    old_autocommit = conn.autocommit
    conn.autocommit = True  # ALTER SYSTEM cannot run in a transaction block
    cursor = conn.cursor()

    try:
        print("Disabling PostgreSQL logging...")
        cursor.execute("ALTER SYSTEM SET log_statement = 'none'")
        cursor.execute("SELECT pg_reload_conf()")
        print("  ✓ PostgreSQL logging disabled")
        return True
    except Exception as e:
        print(f"  ✗ Failed to disable logging: {e}")
        return False
    finally:
        cursor.close()
        conn.autocommit = old_autocommit
        conn.close()


def get_postgres_log_file(db: Database) -> str:
    return "/var/log/postgresql/postgresql-12-main.log"


def run_benchbase_workload(
    config_file: str, config_args: dict, benchmark_name: str = "tpcc"
) -> bool:
    """
    Run BenchBase workload to generate query logs using BenchBaseRunner.

    Args:
        config_file: Path to BenchBase XML config file
        config_args: Configuration dictionary with database and benchmark settings
        benchmark_name: Name of the benchmark

    Returns:
        True if successful, False otherwise
    """
    print(f"\nRunning BenchBase workload...")
    print(f"  Config: {config_file}")
    print(f"  Benchmark: {benchmark_name}")

    try:
        # Create BenchBaseRunner instance
        runner = BenchBaseRunner(config_args)

        # Create a dummy log file path (results are saved by BenchBaseRunner)
        log_file = f"logs/benchbase_{benchmark_name}_{int(time.time())}.log"

        # Run the benchmark using BenchBaseRunner
        throughput = runner.run_benchmark(config_file, log_file)

        if throughput > 0:
            print(
                f"  ✓ BenchBase execution completed successfully (throughput: {throughput})"
            )
            return True
        else:
            print("  ⚠ BenchBase execution completed but returned 0 throughput")
            return True  # Still return True as queries may have been logged

    except Exception as e:
        print(f"  ✗ Error running BenchBase: {e}")
        import traceback

        traceback.print_exc()
        return False


def extract_queries_from_logs(log_file: str, start_byte: int = 0) -> List[str]:
    """
    Extract unique SQL queries from PostgreSQL log file starting from a specific byte offset.

    Args:
        log_file: Path to PostgreSQL log file
        start_byte: Byte offset to start reading from (to skip old logs)

    Returns:
        List of unique SQL queries
    """
    print(f"\nExtracting queries from log file: {log_file}")
    print(f"  Starting from byte offset: {start_byte}")

    if not os.path.exists(log_file):
        print(f"  ✗ Log file not found: {log_file}")
        return []

    queries: Set[str] = set()

    try:
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            # Seek to the starting byte position
            if start_byte > 0:
                f.seek(start_byte)
                print(f"  Skipped first {start_byte} bytes")

            content = f.read()

        # Extract queries using regex to capture multi-line statements
        # Pattern matches "statement:" or "execute" followed by the SQL until the next log entry or EOF
        pattern = r"(?:statement:|execute\s+<unnamed>:)\s*(.*?)(?=\n\d{4}-\d{2}-\d{2}|\nLOG:|\nERROR:|\nWARNING:|\nDETAIL:|\Z)"
        matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            query = match.group(1).strip()

            if not query or len(query) < 5:
                continue

            # Clean up the query - handle escaped newlines and tabs
            query = query.replace("\\n", " ").replace("\\t", " ")
            query = re.sub(r"\s+", " ", query).strip()
            query_upper = query.upper()

            # Skip system queries
            if query_upper in ["BEGIN", "COMMIT", "ROLLBACK"]:
                continue

            # Skip EXPLAIN statements
            if query_upper.startswith("EXPLAIN"):
                continue

            # Skip ALTER SYSTEM statements
            if "ALTER SYSTEM" in query_upper or "pg_reload_conf" in query_upper:
                continue

            # Skip pg_catalog and information_schema metadata queries
            if "pg_catalog." in query.lower() or "information_schema." in query.lower():
                continue

            # Skip pg_stat_* and system statistics queries
            if re.search(r"pg_stat_|pg_statio_", query, re.IGNORECASE):
                continue

            # Skip system function queries
            if re.match(
                r"^SELECT\s+(version|current_database|current_schema)\s*\(", query_upper
            ):
                continue

            # Only keep queries that start with valid SQL keywords
            if not any(
                query_upper.startswith(kw)
                for kw in ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH"]
            ):
                continue

            # Validate query completeness
            if query_upper.startswith("INSERT"):
                # Must have VALUES or SELECT
                if not any(kw in query_upper for kw in ["VALUES", "SELECT"]):
                    continue

            elif query_upper.startswith("UPDATE"):
                # Must have SET
                if " SET " not in query_upper:
                    continue

            elif query_upper.startswith("DELETE"):
                # Must have FROM
                if " FROM " not in query_upper:
                    continue

            elif query_upper.startswith("SELECT"):
                # Must have FROM clause (unless it's a simple expression)
                if " FROM " not in query_upper:
                    # Allow simple expressions like "SELECT 1", "SELECT CURRENT_TIMESTAMP"
                    # But skip incomplete column lists
                    if "," in query or re.search(r"[A-Z_]+\.[A-Z_]+", query):
                        continue
                    # Skip if it's just column names without FROM
                    if re.match(r"^SELECT\s+[A-Z_,\s()]+;?\s*$", query_upper):
                        continue

            # Add valid query
            queries.add(query)

    except Exception as e:
        print(f"  ✗ Error reading log file: {e}")
        return []

    unique_queries = list(queries)
    print(f"  ✓ Extracted {len(unique_queries)} unique queries")

    return unique_queries


def save_queries_to_file(queries: List[str], output_file: str):
    """
    Save extracted queries to a SQL file.

    Args:
        queries: List of SQL queries
        output_file: Output file path
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        for i, query in enumerate(queries, 1):
            f.write(f"-- Query {i}\n")
            f.write(query)
            if not query.endswith(";"):
                f.write(";")
            f.write("\n\n")

    print(f"  ✓ Saved {len(queries)} queries to: {output_file}")


def process_oltp_workload(
    benchmark_name: str, config_file: str, database_name: str, duration: int = 60
):
    """
    Process OLTP workload by logging queries during execution, then extracting plans.

    Args:
        benchmark_name: Name of benchmark (e.g., 'tpcc')
        config_file: Path to BenchBase XML config file
        database_name: Database name to connect to
        duration: How long to run workload in seconds
    """
    # Extract workload name from config file
    workload_id = Path(config_file).stem  # e.g., "sample_tpcc_config0"

    print(f"\n{'='*80}")
    print(f"Processing OLTP workload: {workload_id}")
    print(f"  Config: {config_file}")
    print(f"{'='*80}\n")

    # Load config
    config_path = "config/config.ini"
    config = parse_config.parse_args(config_path)
    config["database_config"]["database"] = database_name
    config["benchmark_config"]["benchmark"] = benchmark_name

    # Initialize database
    db = Database(
        config=config, knob_config_path=config["tuning_config"]["knob_config"]
    )

    # Step 1: Enable PostgreSQL logging
    if not configure_postgres_logging(db):
        print("Failed to enable logging. Aborting.")
        return

    # Wait for config to take effect
    time.sleep(2)

    # Step 2: Get log file path
    log_file = get_postgres_log_file(db)
    print(f"Log file: {log_file}")

    # Mark current position in log file
    log_start_size = 0
    if os.path.exists(log_file):
        log_start_size = os.path.getsize(log_file)
        print(f"Current log size: {log_start_size} bytes")

    # Step 3: Run BenchBase workload using BenchBaseRunner
    if not run_benchbase_workload(config_file, config, benchmark_name):
        print("BenchBase execution failed. Continuing to extract any logged queries...")

    # Wait a moment for logs to flush
    time.sleep(1)

    # Step 4: Extract queries from logs (only the new portion)
    queries = extract_queries_from_logs(log_file, start_byte=log_start_size)

    # Step 5: Disable logging
    disable_postgres_logging(db)

    if not queries:
        print("\n✗ No queries extracted from logs")
        return

    # Step 6: Save queries to file (workload-specific)
    queries_dir = Path("captured_queries") / benchmark_name
    queries_file = queries_dir / f"{workload_id}_queries.sql"
    save_queries_to_file(queries, str(queries_file))

    # Step 7: Extract query plans
    print(f"\nExtracting query plans for {len(queries)} queries...")
    conn = db.get_conn()
    cursor = conn.cursor()
    plan_summaries = []

    try:
        for i, query in enumerate(queries, 1):
            try:
                # Rollback any previous failed transaction
                conn.rollback()

                print(f"  Processing query {i}/{len(queries)}...", end="")

                # Replace parameter placeholders with sample values for realistic plans
                # Using simple numeric values - PostgreSQL will infer types from context
                def replace_param(match):
                    param_num = int(match.group(1))
                    # Use sequential values to avoid NULL optimization issues
                    return str(param_num)

                query_for_explain = re.sub(r"\$(\d+)", replace_param, query)

                # Wrap with EXPLAIN (without ANALYZE to avoid execution)
                explain_query = f"EXPLAIN (FORMAT JSON) {query_for_explain}"

                cursor.execute(explain_query)
                result = cursor.fetchall()

                if result and len(result) > 0:
                    # Extract JSON plan from result
                    plan_json = result[0][0]
                    summary = parse_query_plan_json(plan_json)
                    if summary:
                        plan_summaries.append(summary)
                        print(" ✓")
                    else:
                        print(" ✗ (no plan)")
                else:
                    print(" ✗ (no result)")

            except Exception as e:
                error_msg = str(e).split("\n")[0][:60]  # First line, max 60 chars
                print(f" ✗ Error: {error_msg}")
                # Continue to next query after rolling back
                continue

    finally:
        cursor.close()
        conn.close()

    # Step 8: Save plans (workload-specific)
    output_dir = Path("query_plans") / benchmark_name
    save_plans(plan_summaries, output_dir, workload_id)

    print(f"\n{'='*80}")
    print(f"✓ Extracted {len(plan_summaries)} query plans from {len(queries)} queries")
    print(f"  Queries saved to: {queries_file}")
    print(f"  Plans saved to: {output_dir / (workload_id + '_plans.txt')}")
    print(f"{'='*80}")


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


def process_olap_workload(
    db: Database, workload_path: str, output_dir: Path, workload_name: str
):
    """
    Process OLAP workload by extracting query plans directly from workload files.
    """
    # Check if plans already exist
    text_file = output_dir / f"{workload_name}_plans.txt"
    json_file = output_dir / f"{workload_name}_plans.json"

    if text_file.exists() and json_file.exists():
        print(f"⏭ Plans already exist for {workload_name}, skipping...\n")
        return

    try:
        # Extract plans
        plans = extract_plans_from_workload(db, workload_file=workload_path)

        if plans:
            # Save plans
            save_plans(plans, output_dir, workload_name)
            print(f"✓ Successfully processed {workload_name}\n")
        else:
            print(f"⚠ No plans extracted from {workload_name}\n")

    except Exception as e:
        print(f"✗ Error processing {workload_path}: {e}\n")
        import traceback

        traceback.print_exc()


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
        process_olap_workload(
            db,
            workload_path=os.path.join(workload_dir, workload_file),
            output_dir=output_dir,
            workload_name=workload_name,
        )

    print("\n" + "=" * 80)
    print("Query plan extraction complete!")
    print(f"Results saved to: {output_dir}")
    print("=" * 80)


if __name__ == "__main__":
    benchmark_dict = [
        # {"benchmark": "job", "type": "olap", "database": "imdb"},
        # {"benchmark": "ssb", "type": "olap", "database": "ssb"},
        # {"benchmark": "tpcds", "type": "olap", "database": "tpcds"},
        # {"benchmark": "tpch", "type": "olap", "database": "dss"},
        # OLTP workloads
        {
            "benchmark": "tpcc",
            "type": "oltp",
            "database": "tpcc",
            "workload_path": "oltp_workloads/tpcc",
        },
    ]

    for bench in benchmark_dict:
        print("\n" + "#" * 100)
        print(
            f"Starting query plan extraction for benchmark: {bench['benchmark']} ({bench['type']})"
        )
        print("#" * 100 + "\n")

        if bench["type"] == "oltp":
            # Discover all config files for this OLTP benchmark
            workload_base_path = bench["workload_path"]

            if not os.path.isdir(workload_base_path):
                print(f"✗ Workload path does not exist: {workload_base_path}")
                continue

            all_files = os.listdir(workload_base_path)
            workloads = [
                f
                for f in all_files
                if f.__contains__(bench["benchmark"]) and f.endswith(".xml")
            ]

            # Sort workloads in natural order
            workloads = utils.natural_sort(workloads)

            total_workloads = len(workloads)
            print(
                f"Found {total_workloads} workloads matching '{bench['benchmark']}' in {workload_base_path}"
            )

            if total_workloads == 0:
                print(f"✗ No XML config files found in {workload_base_path}")
                continue
            # config = parse_config.parse_args("config/config.ini")
            # config["database_config"]["database"] = bench["database"]
            # config["benchmark_config"]["benchmark"] = bench["benchmark"]
            # bb = BenchBaseRunner(config)
            # bb.load_database(
            #     workload_path=os.path.join(workload_base_path, workloads[0])
            # )
            # Process each workload config
            successful = 0
            failed = 0
            for idx, workload in enumerate(workloads, 1):
                config_file = os.path.join(workload_base_path, workload)

                try:
                    print(
                        f"\n[Workload {idx}/{total_workloads}] Processing: {workload}"
                    )
                    print("-" * 80)

                    # Process OLTP workload with query logging
                    process_oltp_workload(
                        benchmark_name=bench["benchmark"],
                        config_file=config_file,
                        database_name=bench["database"],
                        duration=60,  # Run for 60 seconds to capture queries
                    )

                    successful += 1
                    print(
                        f"✓ [Workload {idx}/{total_workloads}] Successfully processed: {workload}"
                    )

                except Exception as e:
                    failed += 1
                    print(
                        f"✗ [Workload {idx}/{total_workloads}] Error processing {workload}: {e}"
                    )
                    import traceback

                    traceback.print_exc()
                    continue

            # Summary for this benchmark
            print("\n" + "=" * 80)
            print(f"OLTP Benchmark Summary: {bench['benchmark']}")
            print("=" * 80)
            print(f"Total workloads: {total_workloads}")
            print(f"Successfully processed: {successful}")
            print(f"Failed: {failed}")
            print("=" * 80)

        else:
            # Process OLAP workload normally
            main(
                benchmark_n=bench["benchmark"],
                benchmark_t=bench["type"],
                database_name=bench["database"],
            )

    print("\n" + "#" * 100)
    print("All query plan extractions complete!")
    print("#" * 100)
