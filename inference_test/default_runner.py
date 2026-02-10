#!/usr/bin/env python3
"""
Model Tester - Extracts query plans and workload features from SQL files
"""
import os
import sys
import csv
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.parse_config import parse_args
from Database import Database
from get_query_plans import (
    extract_plans_from_workload,
    read_queries_from_file,
    save_plans,
)
from get_workload_features import extract_features_from_workload, save_features
from stress_testing_tool import stress_testing_tool
from knob_config import parse_knob_config
import utils
import json


class WorkloadAnalyzer:
    """Analyzes SQL workloads by extracting query plans and features."""

    def __init__(self, config_path: str = "../config/config.ini"):
        """
        Initialize the analyzer with configuration.

        Args:
            config_path: Path to config.ini file (relative to this script)
        """
        # Resolve config path relative to this script
        script_dir = Path(__file__).parent
        self.config_path = (script_dir / config_path).resolve()

        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        print(f"Loading configuration from: {self.config_path}")
        self.config = parse_args(str(self.config_path))

        # Get knob config path
        knob_config_path = self.config["tuning_config"]["knob_config"]
        if not Path(knob_config_path).is_absolute():
            knob_config_path = str((script_dir.parent / knob_config_path).resolve())

        # Initialize database connection
        print("Initializing database connection...")
        self.db = Database(self.config, knob_config_path)
        # Reset DB knobs to default and restart
        print("Resetting database knobs to default...")
        self.db.reset_db_knobs()
        self.db.restart_db()
        print("Database initialized.\n")
        # Get benchmark info
        benchmark_config = self.config.get("benchmark_config", {})
        self.benchmark_name = benchmark_config.get("benchmark", "unknown")
        self.benchmark_type = benchmark_config.get("type", "olap")

        print(f"Benchmark: {self.benchmark_name} ({self.benchmark_type})")

        # Initialize stress testing tool
        self.logger = utils.get_logger(self.config["tuning_config"]["log_path"])
        training_log = self.config["tuning_config"].get(
            "training_records", "logs/offline_sample/training_records.log"
        )
        self.stress_tester = stress_testing_tool(
            self.config, self.db, self.logger, records_log=training_log
        )
        print("Stress testing tool initialized.")

        # Load knob configuration
        self.knobs_detail = parse_knob_config.get_knobs(knob_config_path)
        print(f"Loaded {len(self.knobs_detail)} knob definitions.\n")

    def analyze_workload(self, sql_file: str, output_dir: str = "analysis_output"):
        """
        Analyze a SQL workload file: extract query plans and features.

        Args:
            sql_file: Path to SQL file (can be relative to this script)
            output_dir: Output directory for results

        Returns:
            Dictionary containing analysis results
        """
        # Resolve SQL file path
        script_dir = Path(__file__).parent
        sql_path = Path(sql_file)

        if not sql_path.is_absolute():
            sql_path = (script_dir / sql_file).resolve()

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        print(f"\n{'='*80}")
        print(f"Analyzing workload: {sql_path.name}")
        print(f"{'='*80}\n")

        workload_name = sql_path.stem

        # Create output directory with workload-specific subfolder
        output_path = Path(output_dir) / workload_name
        output_path.mkdir(parents=True, exist_ok=True)

        results = {
            "workload_file": str(sql_path),
            "workload_name": workload_name,
            "benchmark": self.benchmark_name,
        }

        # Update config with workload path
        original_workload = self.config["benchmark_config"].get("workload_path")
        self.config["benchmark_config"]["workload_path"] = str(sql_path)

        # Step 1: Extract query plans
        print("Step 1: Extracting query plans...")
        try:
            query_plans = extract_plans_from_workload(self.db, str(sql_path))
            results["query_plans"] = query_plans
            results["num_plans"] = len(query_plans)

            # Save query plans
            save_plans(query_plans, output_path, workload_name)
            print(f"✓ Extracted {len(query_plans)} query plans")
            print(f"  Saved to: {output_path / f'{workload_name}_plans.txt'}\n")
        except Exception as e:
            print(f"✗ Error extracting query plans: {e}\n")
            results["query_plans"] = []
            results["num_plans"] = 0

        # Step 2: Extract workload features
        print("Step 2: Extracting workload features...")
        try:
            features_data = extract_features_from_workload(
                str(sql_path), self.benchmark_name
            )
            results["features"] = features_data["features"]
            results["feature_string"] = features_data["feature_string"]

            # Save workload features
            save_features(features_data, output_path, workload_name)
            print(f"✓ Extracted workload features")
            print(f"  Saved to: {output_path / f'{workload_name}_features.txt'}\n")
        except Exception as e:
            print(f"✗ Error extracting workload features: {e}\n")
            results["features"] = {}
            results["feature_string"] = ""

        # Step 3: Run default configuration 4 times and average
        print("Step 3: Running default configuration 4 times...")
        default_knobs = self._get_default_knobs()
        performances = []
        internal_metrics_list = []

        for run in range(4):
            print(f"  Run {run + 1}/4...", end=" ")
            try:
                # Reset metrics before testing
                self.db.reset_inner_metrics()

                # Run stress test with default configuration
                performance = self.stress_tester.test_config(
                    default_knobs, iteration=run
                )
                performances.append(performance)

                # Fetch internal metrics after workload execution
                internal_metrics = self.db.fetch_inner_metrics()
                internal_metrics_list.append(internal_metrics)

                print(f"Performance: {performance:.6f}")

            except Exception as e:
                print(f"Failed: {e}")

        # Calculate average performance
        if performances:
            avg_performance = sum(performances) / len(performances)
            results["default_performance"] = avg_performance
            results["all_performances"] = performances
            print(f"\nCompleted {len(performances)} successful runs")
            print(f"  Average performance: {avg_performance:.6f}")
            print(f"  Individual runs: {[f'{p:.6f}' for p in performances]}")
        else:
            avg_performance = None
            results["default_performance"] = None
            print(f"\nWarning: No successful performance measurements")

        # Use metrics from the last successful run
        if internal_metrics_list:
            internal_metrics = internal_metrics_list[-1]
            results["internal_metrics"] = internal_metrics
            results["default_knobs"] = default_knobs

            # Save internal metrics
            import json

            metrics_file = output_path / f"{workload_name}_internal_metrics.json"
            with open(metrics_file, "w") as f:
                json.dump(internal_metrics, f, indent=4)
            print(f"  Internal metrics saved to: {metrics_file}")

            # Save performance results
            perf_file = output_path / f"{workload_name}_default_performance.json"
            perf_result = {
                "average_performance": avg_performance,
                "all_performances": performances,
                "knobs": default_knobs,
                "workload": workload_name,
                "internal_metrics": internal_metrics,
            }
            with open(perf_file, "w") as f:
                json.dump(perf_result, f, indent=4)
            print(f"  Performance results saved to: {perf_file}")

        # Save results to CSV
        csv_file = output_path / f"{workload_name}_default_results.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Workload", "Run", "Performance", "Average Performance"])
            for i, perf in enumerate(performances):
                writer.writerow([workload_name, i + 1, f"{perf:.6f}", ""])
            if avg_performance is not None:
                writer.writerow(
                    [workload_name, "Average", "", f"{avg_performance:.6f}"]
                )
        print(f"  CSV results saved to: {csv_file}\n")

        # Restore original workload path
        if original_workload:
            self.config["benchmark_config"]["workload_path"] = original_workload

        # Summary
        print(f"{'='*80}")
        print("Analysis Summary")
        print(f"{'='*80}")
        print(f"Workload: {workload_name}")
        print(f"Default performance: {results.get('default_performance', 'N/A')}")
        print(f"Internal metrics collected: {len(results.get('internal_metrics', {}))}")
        print(f"Query plans extracted: {results['num_plans']}")
        print(f"Features extracted: {len(results.get('features', {}))}")
        print(f"Output directory: {output_path.resolve()}")
        print(f"{'='*80}\n")

        return results

    def _get_default_knobs(self) -> dict:
        """Get default knob values from knob configuration."""
        default_knobs = {}
        for name, detail in self.knobs_detail.items():
            if "default" in detail:
                default_knobs[name] = detail["default"]
            elif "min" in detail and "max" in detail:
                # If no default, use middle of range
                min_val = detail["min"]
                max_val = detail["max"]
                if detail.get("type") == "integer":
                    default_knobs[name] = int((min_val + max_val) / 2)
                else:
                    default_knobs[name] = (min_val + max_val) / 2
        return default_knobs

    def close(self):
        """Clean up resources."""
        if hasattr(self, "db"):
            # Close any open connections
            try:
                conn = self.db.get_conn()
                conn.close()
            except:
                pass


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze SQL workload files: extract query plans and features"
    )
    parser.add_argument(
        "sql_file", help="Path to SQL file (relative to this script or absolute)"
    )
    parser.add_argument(
        "--config",
        default="../config/config.ini",
        help="Path to config.ini file (default: ../config/config.ini)",
    )
    parser.add_argument(
        "--output",
        default="analysis_output",
        help="Output directory for results (default: analysis_output)",
    )

    args = parser.parse_args()

    try:
        # Initialize analyzer
        analyzer = WorkloadAnalyzer(config_path=args.config)

        # Analyze workload
        results = analyzer.analyze_workload(args.sql_file, args.output)

        # Display feature string preview
        if results.get("feature_string"):
            print("\nFeature String Preview:")
            print("-" * 80)
            feature_str = results["feature_string"]
            print(feature_str[:500] + ("..." if len(feature_str) > 500 else ""))
            print("-" * 80)

        # Display sample query plans
        if results.get("query_plans"):
            print("\nSample Query Plans (first 3):")
            print("-" * 80)
            for i, plan in enumerate(results["query_plans"][:3], 1):
                print(f"{i}. {plan}")
            if len(results["query_plans"]) > 3:
                print(f"... and {len(results['query_plans']) - 3} more")
            print("-" * 80)

        print("\n✓ Analysis complete!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    finally:
        if "analyzer" in locals():
            analyzer.close()


if __name__ == "__main__":
    main()
