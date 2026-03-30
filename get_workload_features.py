#!/usr/bin/env python3
"""
Workload Feature Extractor
Extracts workload-level features from SQL query files
"""

import json
import re
import os
from pathlib import Path
from typing import List, Dict, Any

from config import parse_config
import utils


class WorkloadFeatureGenerator:
    """
    Analyzes SQL workloads and generates feature summaries including:
    - Workload statistics (size, ratios, query characteristics)
    """

    def __init__(self):
        self.queries = []

    def parse_queries(self, query_list: List[str]) -> None:
        """Parse and store queries for analysis"""
        self.queries = [q.strip() for q in query_list if q.strip()]

    def load_queries_from_file(self, file_path: str) -> None:
        """Load queries from SQL file"""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Split by semicolon to get individual queries
        queries = [q.strip() for q in content.split(";") if q.strip()]
        self.parse_queries(queries)

    def analyze_workload_features(self) -> Dict[str, Any]:
        """Extract workload-level features from queries"""
        features = {
            "size": len(self.queries),
            "read_ratio": 0.0,
            "group_by_ratio": 0.0,
            "order_by_ratio": 0.0,
            "avg_query_length": 0.0,
            "avg_joins": 0.0,
            "filter_ratio": 0.0,
        }

        if not self.queries:
            return features

        read_count = 0
        group_by_count = 0
        order_by_count = 0
        total_length = 0
        total_joins = 0
        filter_count = 0

        for query in self.queries:
            query_upper = query.upper()

            # Count SELECT queries (reads)
            if "SELECT" in query_upper and not any(
                kw in query_upper for kw in ["INSERT", "UPDATE", "DELETE"]
            ):
                read_count += 1

            # Count GROUP BY
            if "GROUP BY" in query_upper:
                group_by_count += 1

            # Count ORDER BY
            if "ORDER BY" in query_upper:
                order_by_count += 1

            # Query length (character count)
            total_length += len(query)

            # Count JOINs
            join_count = len(re.findall(r"\bJOIN\b", query_upper))
            total_joins += join_count

            # Count WHERE clauses (filters)
            if "WHERE" in query_upper:
                filter_count += 1

        num_queries = len(self.queries)
        features["read_ratio"] = round(read_count / num_queries, 2)
        features["group_by_ratio"] = round(group_by_count / num_queries, 2)
        features["order_by_ratio"] = round(order_by_count / num_queries, 2)
        features["avg_query_length"] = round(total_length / num_queries, 1)
        features["avg_joins"] = round(total_joins / num_queries, 1)
        features["filter_ratio"] = round(filter_count / num_queries, 2)

        return features

    def generate_feature_string(self) -> str:
        """Generate the complete feature string in the required format"""
        workload_features = self.analyze_workload_features()

        # Format workload features
        workload_str = (
            f"workload features: size of workload: {workload_features['size']}; "
            f"read ratio: {workload_features['read_ratio']}; "
            f"group by ratio: {workload_features['group_by_ratio']}; "
            f"order by ratio: {workload_features['order_by_ratio']}; "
            f"avg query length: {workload_features['avg_query_length']}; "
            f"number of joins: {workload_features['avg_joins']}; "
            f"filter ratio: {workload_features['filter_ratio']};"
        )

        return workload_str

    def save_to_json(self, output_path: str) -> None:
        """Save workload features to JSON file"""
        workload_features = self.analyze_workload_features()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(workload_features, f, indent=2)

        print(f"  Saved features to: {output_path}")

    def save_to_text(self, output_path: str) -> None:
        """Save workload features as text string"""
        feature_string = self.generate_feature_string()

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(feature_string)

        print(f"  Saved feature string to: {output_path}")


def extract_features_from_workload(
    workload_file: str, benchmark_name: str
) -> Dict[str, Any]:
    """
    Extract workload features from a SQL file.

    Args:
        workload_file: Path to workload SQL file
        benchmark_name: Name of the benchmark

    Returns:
        Dictionary containing workload features
    """
    print(f"\nProcessing: {workload_file}")

    generator = WorkloadFeatureGenerator()
    generator.load_queries_from_file(workload_file)

    print(f"  Found {len(generator.queries)} queries")

    features = generator.analyze_workload_features()
    feature_string = generator.generate_feature_string()

    print(f"  Feature string: {feature_string[:100]}...")

    return {
        "features": features,
        "feature_string": feature_string,
        "generator": generator,
    }


def save_features(features_data: Dict[str, Any], output_dir: Path, workload_name: str):
    """
    Save workload features to JSON and text files.

    Args:
        features_data: Dictionary containing features and generator
        output_dir: Output directory path
        workload_name: Name of the workload
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    generator = features_data["generator"]

    # Save as JSON file
    json_file = output_dir / f"{workload_name}_features.json"
    generator.save_to_json(str(json_file))

    # Save as text file (feature string)
    text_file = output_dir / f"{workload_name}_features.txt"
    generator.save_to_text(str(text_file))


def process_oltp_workload_features(benchmark_name: str):
    """
    Process OLTP workload features from captured queries.

    Args:
        benchmark_name: Name of benchmark (e.g., 'tpcc')
    """
    print(f"\n{'='*80}")
    print(f"Extracting workload features for OLTP: {benchmark_name}")
    print(f"{'='*80}\n")

    # Path to captured queries directory
    queries_dir = f"captured_queries/{benchmark_name}"

    if not os.path.exists(queries_dir):
        print(f"✗ Captured queries directory not found: {queries_dir}")
        print("  Please run get_query_plans.py first to capture queries!")
        return

    # Find all captured query files
    all_files = os.listdir(queries_dir)
    query_files = [f for f in all_files if f.endswith("_queries.sql")]

    # Sort in natural order
    query_files = utils.natural_sort(query_files)

    total_files = len(query_files)
    print(f"Found {total_files} captured query files in {queries_dir}")

    if total_files == 0:
        print(f"✗ No query files found in {queries_dir}")
        return

    # Create output directory
    output_dir = Path("workload_features") / benchmark_name

    # Process each query file
    successful = 0
    failed = 0

    for idx, query_file in enumerate(query_files, 1):
        queries_file_path = os.path.join(queries_dir, query_file)
        workload_id = Path(query_file).stem.replace("_queries", "")

        try:
            print(f"\n[Workload {idx}/{total_files}] Processing: {query_file}")
            print("-" * 60)

            # Extract features
            features_data = extract_features_from_workload(
                workload_file=queries_file_path, benchmark_name=benchmark_name
            )

            # Save features
            save_features(features_data, output_dir, workload_id)

            successful += 1
            print(
                f"✓ [Workload {idx}/{total_files}] Successfully processed: {workload_id}"
            )

        except Exception as e:
            failed += 1
            print(
                f"✗ [Workload {idx}/{total_files}] Error processing {query_file}: {e}"
            )
            import traceback

            traceback.print_exc()
            continue

    print(f"\n{'='*80}")
    print(f"OLTP Feature Extraction Summary: {benchmark_name}")
    print(f"{'='*80}")
    print(f"Total query files: {total_files}")
    print(f"Successfully processed: {successful}")
    print(f"Failed: {failed}")
    print(f"Output directory: {output_dir}")
    print(f"{'='*80}")


def process_olap_workload_features(
    workload_file: str, benchmark_name: str, output_dir: Path, workload_name: str
):
    """
    Process OLAP workload to extract features.
    Args:
        workload_file: Path to workload file
        benchmark_name: Name of the benchmark
        output_dir: Output directory path
        workload_name: Name of the workload
    """
    # Check if features already exist
    text_file = output_dir / f"{workload_name}_features.txt"
    json_file = output_dir / f"{workload_name}_features.json"
    if text_file.exists() and json_file.exists():
        print(f"⏭ Features already exist for {workload_name}, skipping...\n")
        return

    try:
        # Extract features
        features_data = extract_features_from_workload(workload_file, benchmark_name)

        if features_data:
            # Save features
            save_features(features_data, output_dir, workload_name)
            print(f"✓ Successfully processed {workload_name}\n")
        else:
            print(f"⚠ No features extracted from {workload_name}\n")

    except Exception as e:
        print(f"✗ Error processing {workload_file}: {e}\n")
        import traceback

        traceback.print_exc()


def main(benchmark_n: str = None, benchmark_t: str = None, database_name: str = None):
    """Main function to extract workload features."""
    # Load configuration from config.ini
    config_path = "config/config.ini"
    print(f"Loading configuration from: {config_path}")
    config = parse_config.parse_args(config_path)

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

    all_files = os.listdir(workload_dir)

    if config["benchmark_config"].get("type", "olap") == "oltp":
        workloads = [
            f
            for f in all_files
            if f.__contains__(benchmark_name) and f.endswith(".xml")
        ]
    else:
        workloads = [
            f for f in all_files if f.startswith(benchmark_name) and f.endswith(".wg")
        ]

    # Sort workloads in natural (numeric-aware) order
    workloads = utils.natural_sort(workloads)

    total_workloads = len(workloads)
    print(f"Found {total_workloads} workloads matching prefix '{benchmark_name}'")

    # Create output directory
    output_dir = Path("workload_features") / benchmark_name
    print(f"Output directory: {output_dir}")

    # Process each workload file
    for workload_file_name in workloads:
        workload_file = os.path.join(workload_dir, workload_file_name)
        workload_name = Path(workload_file_name).stem

        process_olap_workload_features(
            workload_file, benchmark_name, output_dir, workload_name
        )

    print("\n" + "=" * 80)
    print("Workload feature extraction complete!")
    print(f"Results saved to: {output_dir}")
    print("=" * 80)


if __name__ == "__main__":
    benchmark_dict = [
        {"benchmark": "job", "type": "olap", "database": "imdb"},
        {"benchmark": "ssb", "type": "olap", "database": "ssb"},
        {"benchmark": "tpcds", "type": "olap", "database": "tpcds"},
        {"benchmark": "tpch", "type": "olap", "database": "dss"},
        # OLTP workloads - process from captured queries
        {"benchmark": "tpcc", "type": "oltp", "database": "tpcc"},
    ]
    for bench in benchmark_dict:
        print("\n" + "#" * 100)
        print(
            f"Starting workload feature extraction for benchmark: {bench['benchmark']} ({bench['type']})"
        )
        print("#" * 100 + "\n")

        if bench["type"] == "oltp":
            # Process OLTP workload from captured queries
            process_oltp_workload_features(benchmark_name=bench["benchmark"])
        else:
            # Process OLAP workload normally
            main(
                benchmark_n=bench["benchmark"],
                benchmark_t=bench["type"],
                database_name=bench["database"],
            )

    print("\n" + "#" * 100)
    print("All workload feature extractions complete!")
    print("#" * 100)
