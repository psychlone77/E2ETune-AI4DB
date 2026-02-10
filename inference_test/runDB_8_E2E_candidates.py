#!/usr/bin/env python3
"""
Run Database Tests on E2ETune Generated Configurations
Tests E2ETune model predictions (actual values) on real database
"""
import os
import sys
import json
import csv
from pathlib import Path
from typing import Dict, Any, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.parse_config import parse_args
from Database import Database
from stress_testing_tool import stress_testing_tool
from knob_config import parse_knob_config
import utils


class E2ETuneTester:
    """Tests E2ETune-generated configurations on database."""

    def __init__(
        self,
        config_path: str = "../config/config.ini",
    ):
        """
        Initialize the E2ETune tester.

        Args:
            config_path: Path to config.ini
        """
        print("Initializing E2ETune Tester...")

        # Resolve paths
        script_dir = Path(__file__).parent
        self.config_path = (script_dir / config_path).resolve()

        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        # Load config
        print(f"Loading configuration from: {self.config_path}")
        self.config = parse_args(str(self.config_path))

        # Get knob config path
        knob_config_path = self.config["tuning_config"]["knob_config"]
        if not Path(knob_config_path).is_absolute():
            knob_config_path = str((script_dir.parent / knob_config_path).resolve())

        # Initialize database
        print("Initializing database connection...")
        self.db = Database(self.config, knob_config_path)
        self.db.reset_db_knobs()
        self.db.restart_db()
        print("Database initialized.\n")

        # Get benchmark info
        benchmark_config = self.config.get("benchmark_config", {})
        self.benchmark_name = benchmark_config.get("benchmark", "unknown")

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
        print("Ready to test E2ETune configurations.\n")

    def test_candidate(
        self, candidate_idx: int, raw_config: Dict[str, Any], sql_file: str
    ) -> Dict[str, Any]:
        """
        Test a single candidate configuration.

        Args:
            candidate_idx: Index of the candidate (0-7)
            raw_config: Configuration with actual knob values
            sql_file: Path to SQL workload file

        Returns:
            Dictionary with test results
        """
        print(f"\n{'='*80}")
        print(f"Testing Candidate {candidate_idx + 1}")
        print(f"{'='*80}\n")

        print(f"Configuration (first 5 knobs):")
        for i, (k, v) in enumerate(list(raw_config.items())[:5]):
            print(f"  {k}: {v}")
        if len(raw_config) > 5:
            print(f"  ... and {len(raw_config) - 5} more knobs")

        result = {
            "candidate_idx": candidate_idx,
            "raw_config": raw_config,
        }

        # Test on real database
        print("\n[Real Database Test]")
        try:
            self.db.reset_inner_metrics()

            # Update config with workload path
            original_workload = self.config["benchmark_config"].get("workload_path")
            self.config["benchmark_config"]["workload_path"] = str(sql_file)

            # Run stress test
            real_performance = self.stress_tester.test_config(
                raw_config, iteration=candidate_idx + 1
            )

            # Restore original workload path
            if original_workload:
                self.config["benchmark_config"]["workload_path"] = original_workload

            # Fetch internal metrics
            internal_metrics = self.db.fetch_inner_metrics()

            result["real_performance"] = real_performance
            result["internal_metrics"] = internal_metrics

            print(f"✓ Real performance: {real_performance:.4f}")

        except Exception as e:
            print(f"✗ Error testing on real database: {e}")
            result["real_performance"] = None
            result["real_error"] = str(e)

        return result

    def load_candidates(self, candidates_file: str) -> List[Dict[str, Any]]:
        """
        Load candidate configurations from JSON file.

        Args:
            candidates_file: Path to JSON file with candidate configurations

        Returns:
            List of candidate configurations (actual values)
        """
        with open(candidates_file, "r") as f:
            candidates = json.load(f)

        print(f"Loaded {len(candidates)} E2ETune candidates from {candidates_file}")
        return candidates

    def test_all_candidates(
        self, sql_file: str, candidates_file: str, output_dir: str = "analysis_output"
    ) -> Dict[str, Any]:
        """
        Test all candidates and save results.

        Args:
            sql_file: Path to SQL workload file
            candidates_file: Path to JSON file with candidate configurations
            output_dir: Directory to save results

        Returns:
            Dictionary with summary results
        """
        print(f"\n{'='*80}")
        print("Starting E2ETune Candidate Testing")
        print(f"{'='*80}\n")

        # Load candidates
        candidates = self.load_candidates(candidates_file)

        if not candidates:
            raise ValueError("No candidates loaded")

        # Get workload name from SQL file
        workload_name = Path(sql_file).stem

        # Test each candidate
        results = []
        for idx, raw_config in enumerate(candidates):
            result = self.test_candidate(idx, raw_config, sql_file)
            results.append(result)

        # Find best candidate (filter out failed tests)
        valid_results = [r for r in results if r.get("real_performance") is not None]
        if not valid_results:
            raise ValueError("All candidate tests failed")

        best_result = min(valid_results, key=lambda x: x["real_performance"])
        print(f"\n{'='*80}")
        print("Best E2ETune Configuration:")
        print(f"  Candidate: {best_result['candidate_idx'] + 1}")
        print(f"  Real Performance: {best_result['real_performance']:.4f}")
        print(f"{'='*80}\n")

        # Save results
        output_path = Path(output_dir) / workload_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Save as CSV
        csv_file = output_path / f"{workload_name}_e2etune_results.csv"
        self._save_to_csv(results, csv_file)

        # Save as readable text
        txt_file = output_path / f"{workload_name}_e2etune_results.txt"
        self._save_to_text(results, txt_file, workload_name)

        return {
            "total_candidates": len(results),
            "best_candidate": best_result,
            "all_results": results,
        }

    def _save_to_csv(self, results: List[Dict[str, Any]], csv_file: Path):
        """Save results to CSV file."""
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Candidate", "Real Performance"])

            for result in results:
                perf = result.get("real_performance", "ERROR")
                if perf is not None:
                    writer.writerow([result["candidate_idx"] + 1, f"{perf:.4f}"])
                else:
                    writer.writerow([result["candidate_idx"] + 1, "ERROR"])

        print(f"Saved CSV results to: {csv_file}")

    def _save_to_text(
        self, results: List[Dict[str, Any]], txt_file: Path, workload_name: str
    ):
        """Save results to human-readable text file."""
        with open(txt_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write(f"E2ETune Configuration Test Results - {workload_name}\n")
            f.write("=" * 80 + "\n\n")

            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total candidates tested: {len(results)}\n")

            performances = [
                r["real_performance"]
                for r in results
                if r.get("real_performance") is not None
            ]
            if performances:
                f.write(f"Performance - Min: {min(performances):.4f}\n")
                f.write(f"Performance - Max: {max(performances):.4f}\n")
                f.write(
                    f"Performance - Avg: {sum(performances)/len(performances):.4f}\n\n"
                )
            else:
                f.write("No successful tests\n\n")

            # Best candidate
            if performances:
                best_idx = performances.index(min(performances))
                best_result = [
                    r for r in results if r.get("real_performance") is not None
                ][best_idx]
                f.write("BEST CONFIGURATION\n")
                f.write("-" * 80 + "\n")
                f.write(f"Candidate: {best_result['candidate_idx'] + 1}\n")
                f.write(f"Real Performance: {best_result['real_performance']:.4f}\n\n")
            f.write("Configuration:\n")
            for knob, value in sorted(best_result["raw_config"].items()):
                f.write(f"  {knob}: {value}\n")
            f.write("\n")

            # Detailed results for all candidates
            f.write("DETAILED RESULTS\n")
            f.write("=" * 80 + "\n\n")

            for result in results:
                f.write(f"Candidate {result['candidate_idx'] + 1}\n")
                f.write("-" * 80 + "\n")
                perf = result.get("real_performance")
                if perf is not None:
                    f.write(f"Real Performance: {perf:.4f}\n\n")
                else:
                    error = result.get("real_error", "Unknown error")
                    f.write(f"ERROR: {error}\n\n")

                f.write("Configuration:\n")
                for knob, value in sorted(result["raw_config"].items()):
                    f.write(f"  {knob}: {value}\n")
                f.write("\n" + "=" * 80 + "\n\n")

        print(f"Saved text results to: {txt_file}")

    def close(self):
        """Clean up resources."""
        print("\nCleaning up...")
        self.db.reset_db_knobs()
        self.db.restart_db()
        print("Cleanup complete.")


def main():
    """Main function for command-line usage."""
    print("=" * 80)
    print("Database Testing for E2ETune Configurations")
    print("=" * 80 + "\n")

    import argparse

    parser = argparse.ArgumentParser(
        description="Test E2ETune-generated configurations on real database"
    )
    parser.add_argument("sql_file", help="Path to SQL workload file")
    parser.add_argument(
        "--candidates",
        default="generated_configs.json",
        help="Path to candidates JSON file (default: generated_configs.json)",
    )
    parser.add_argument(
        "--config",
        default="../config/config.ini",
        help="Path to config.ini (default: ../config/config.ini)",
    )
    parser.add_argument(
        "--output",
        default="analysis_output",
        help="Output directory for results (default: analysis_output)",
    )

    args = parser.parse_args()

    # If candidates file not explicitly provided, derive from sql_file name
    if args.candidates == "generated_configs.json":  # default value
        workload_name = Path(args.sql_file).stem
        args.candidates = str(
            Path(args.output)
            / f"{workload_name}"
            / f"{workload_name}_8_E2E_candidates.json"
        )
        print(f"Using derived candidates file: {args.candidates}\n")

    try:
        # Initialize tester
        tester = E2ETuneTester(
            config_path=args.config,
        )

        # Test all candidates
        summary = tester.test_all_candidates(
            sql_file=args.sql_file,
            candidates_file=args.candidates,
            output_dir=args.output,
        )

        print("\n" + "=" * 80)
        print("Testing Complete!")
        print(f"Total candidates tested: {summary['total_candidates']}")
        print(f"Best performance: {summary['best_candidate']['real_performance']:.4f}")
        print("=" * 80)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        if "tester" in locals():
            tester.close()


if __name__ == "__main__":
    main()
