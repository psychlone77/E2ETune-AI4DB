#!/usr/bin/env python3
"""
Run Database Tests on 8 Candidates
Tests LLM-predicted configurations on real database only
For cost model predictions, use runCM_8_candidates.py
"""
import os
import sys
import json
import pickle
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

# For downloading from Hugging Face
from huggingface_hub import hf_hub_download


class CandidateTester:
    """Tests multiple candidate configurations on database and surrogate model."""

    def __init__(
        self,
        config_path: str = "../config/config.ini",
        bin_edges_path: str = None,
        hf_repo: str = "NisithDissanayake/genknob-tuner",
    ):
        """
        Initialize the candidate tester.

        Args:
            config_path: Path to config.ini
            bin_edges_path: Path to local bin_edges.pkl file (optional)
            hf_repo: Hugging Face repository to download bin_edges from
        """
        print("Initializing Candidate Tester...")

        # Resolve paths
        script_dir = Path(__file__).parent
        self.config_path = (script_dir / config_path).resolve()

        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        # Handle bin_edges: download from HF or use local file
        if bin_edges_path is None:
            print(f"Downloading bin_edges.pkl from Hugging Face repo: {hf_repo}...")
            try:
                self.bin_edges_path = hf_hub_download(
                    repo_id=hf_repo, filename="bin_edges.pkl", repo_type="model"
                )
                print(f"Downloaded bin_edges.pkl to: {self.bin_edges_path}")
            except Exception as e:
                raise RuntimeError(
                    f"Failed to download bin_edges.pkl from Hugging Face: {e}"
                )
        else:
            self.bin_edges_path = (script_dir / bin_edges_path).resolve()
            if not self.bin_edges_path.exists():
                raise FileNotFoundError(
                    f"Bin edges file not found: {self.bin_edges_path}"
                )

        # Load config
        print(f"Loading configuration from: {self.config_path}")
        self.config = parse_args(str(self.config_path))

        # Get knob config path
        knob_config_path = self.config["tuning_config"]["knob_config"]
        if not Path(knob_config_path).is_absolute():
            knob_config_path = str((script_dir.parent / knob_config_path).resolve())

        # Load bin edges
        print(f"Loading bin edges from: {self.bin_edges_path}")
        with open(self.bin_edges_path, "rb") as f:
            self.bin_edges = pickle.load(f)
        print(f"Loaded bin edges for {len(self.bin_edges)} knobs\n")

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
        print("Note: Surrogate model testing moved to runCM_8_candidates.py\n")

    def bins_to_raw_values(self, predicted_bins: Dict[str, int]) -> Dict[str, Any]:
        """
        Convert bin indices to raw knob values.

        Args:
            predicted_bins: Dictionary mapping knob names to bin indices

        Returns:
            Dictionary mapping knob names to raw values
        """
        raw_config = {}

        for knob, bin_idx in predicted_bins.items():
            if knob not in self.bin_edges:
                continue

            edges = self.bin_edges[knob]

            # Convert bin index to integer
            try:
                idx = int(bin_idx)
            except (ValueError, TypeError):
                try:
                    idx = int(float(bin_idx))
                except (ValueError, TypeError):
                    idx = 0

            # Clamp index to valid range
            idx = max(0, min(idx, len(edges) - 2))

            # Get midpoint of bin
            raw_value = (edges[idx] + edges[idx + 1]) / 2

            # Round based on knob type and convert to native Python types
            if knob in self.knobs_detail:
                knob_type = self.knobs_detail[knob].get("type", "float")
                if knob_type == "integer":
                    raw_value = int(round(float(raw_value)))
                else:
                    raw_value = float(raw_value)
            else:
                # Default to float, ensure native Python type
                raw_value = float(raw_value)

            raw_config[knob] = raw_value

        return raw_config

    def normalize_knobs(self, raw_config: Dict[str, Any]) -> Dict[str, float]:
        """
        Normalize knob values to [0, 1] range for surrogate model.

        Args:
            raw_config: Dictionary mapping knob names to raw values

        Returns:
            Dictionary mapping knob names to normalized values
        """
        normalized = {}
        for knob, value in raw_config.items():
            if knob in self.knobs_detail:
                detail = self.knobs_detail[knob]
                min_val = detail.get("min", 0)
                max_val = detail.get("max", 1)
                if max_val > min_val:
                    normalized[knob] = (float(value) - min_val) / (max_val - min_val)
                else:
                    normalized[knob] = 0.0
        return normalized

    def test_candidate(
        self, candidate_idx: int, binned_config: Dict[str, int], sql_file: str
    ) -> Dict[str, Any]:
        """
        Test a single candidate configuration.

        Args:
            candidate_idx: Index of the candidate (0-7)
            binned_config: Configuration as bin indices
            sql_file: Path to SQL workload file

        Returns:
            Dictionary with test results
        """
        print(f"\n{'='*80}")
        print(f"Testing Candidate {candidate_idx + 1}")
        print(f"{'='*80}\n")

        # Convert bins to raw values
        raw_config = self.bins_to_raw_values(binned_config)
        print(f"Converted {len(raw_config)} knobs to raw values")

        result = {
            "candidate_idx": candidate_idx,
            "binned_config": binned_config,
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

    def load_candidates(self, candidates_file: str) -> List[Dict[str, int]]:
        """
        Load candidate configurations from JSON file.

        Args:
            candidates_file: Path to candidates JSON file

        Returns:
            List of candidate configurations (bin indices)
        """
        candidates_path = Path(candidates_file)
        if not candidates_path.exists():
            raise FileNotFoundError(f"Candidates file not found: {candidates_file}")

        print(f"Loading candidates from: {candidates_path}")
        with open(candidates_path, "r") as f:
            candidates = json.load(f)

        print(f"Loaded {len(candidates)} candidates\n")
        return candidates

    def test_all_candidates(
        self, sql_file: str, candidates_file: str, output_dir: str = "analysis_output"
    ) -> Dict[str, Any]:
        """
        Test all candidate configurations.

        Args:
            sql_file: Path to SQL workload file
            candidates_file: Path to candidates JSON file
            output_dir: Output directory for results

        Returns:
            Dictionary with all results
        """
        # Resolve paths
        script_dir = Path(__file__).parent
        sql_path = Path(sql_file)

        if not sql_path.is_absolute():
            sql_path = (script_dir / sql_file).resolve()

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        workload_name = sql_path.stem

        print(f"\n{'='*80}")
        print(f"Testing Multiple Candidates for: {workload_name}")
        print(f"{'='*80}\n")

        # Load candidates
        candidates = self.load_candidates(candidates_file)

        # Test each candidate
        results = []
        for idx, candidate in enumerate(candidates):
            result = self.test_candidate(idx, candidate, sql_path)
            results.append(result)

        # Prepare output in workload-specific subfolder
        output_path = Path(output_dir) / workload_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Save results to CSV
        csv_file = output_path / f"{workload_name}_candidates_results.csv"
        self._save_to_csv(results, csv_file)

        # Save detailed analysis to text file
        txt_file = output_path / f"{workload_name}_candidates_analysis.txt"
        self._save_to_text(results, txt_file, workload_name)

        # Save full results to JSON
        json_file = output_path / f"{workload_name}_candidates_full_results.json"
        with open(json_file, "w") as f:
            json.dump(
                {
                    "workload_name": workload_name,
                    "benchmark": self.benchmark_name,
                    "results": results,
                },
                f,
                indent=4,
            )

        print(f"\n{'='*80}")
        print("Testing Complete")
        print(f"{'='*80}")
        print(f"Tested {len(results)} candidates")
        print(f"CSV results: {csv_file}")
        print(f"Text analysis: {txt_file}")
        print(f"Full JSON: {json_file}")
        print(f"{'='*80}\n")

        return {
            "workload_name": workload_name,
            "results": results,
            "csv_file": str(csv_file),
            "txt_file": str(txt_file),
            "json_file": str(json_file),
        }

    def _save_to_csv(self, results: List[Dict[str, Any]], csv_file: Path):
        """Save results to CSV file."""
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)

            # Header - Database results only
            writer.writerow(["Candidate", "Real Performance", "Error"])

            # Data rows
            for result in results:
                writer.writerow(
                    [
                        result["candidate_idx"] + 1,
                        (
                            f"{result.get('real_performance', 'N/A'):.4f}"
                            if result.get("real_performance")
                            else "ERROR"
                        ),
                        result.get("real_error", ""),
                    ]
                )

        print(f"✓ CSV saved to: {csv_file}")

    def _save_to_text(
        self, results: List[Dict[str, Any]], txt_file: Path, workload_name: str
    ):
        """Save detailed analysis to text file (database testing only)."""
        with open(txt_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write(f"Database Testing Results\n")
            f.write(f"Workload: {workload_name}\n")
            f.write(f"Benchmark: {self.benchmark_name}\n")
            f.write(f"Note: Run runCM_8_candidates.py for cost model predictions\n")
            f.write("=" * 80 + "\n\n")

            # Individual results
            for result in results:
                idx = result["candidate_idx"]
                f.write(f"Candidate {idx + 1}:\n")
                f.write("-" * 40 + "\n")

                real_perf = result.get("real_performance")
                if real_perf:
                    f.write(f"  Real Performance:      {real_perf:.4f}\n")
                else:
                    f.write(f"  Real Performance:      ERROR\n")
                    if "real_error" in result:
                        f.write(f"    Error: {result['real_error']}\n")

                f.write("\n")

            # Summary statistics
            f.write("=" * 80 + "\n")
            f.write("SUMMARY ANALYSIS\n")
            f.write("=" * 80 + "\n\n")

            # Real performance stats
            real_perfs = [
                r["real_performance"]
                for r in results
                if r.get("real_performance") is not None
            ]
            if real_perfs:
                best_idx = real_perfs.index(max(real_perfs))
                worst_idx = real_perfs.index(min(real_perfs))
                avg_perf = sum(real_perfs) / len(real_perfs)

                f.write("Real Database Performance:\n")
                f.write(
                    f"  Best:    Candidate {results[best_idx]['candidate_idx'] + 1} with {max(real_perfs):.4f}\n"
                )
                f.write(
                    f"  Worst:   Candidate {results[worst_idx]['candidate_idx'] + 1} with {min(real_perfs):.4f}\n"
                )
                f.write(f"  Average: {avg_perf:.4f}\n")
                f.write(
                    f"  Range:   {max(real_perfs) - min(real_perfs):.4f} ({(max(real_perfs) - min(real_perfs)) / avg_perf * 100:.2f}% of average)\n"
                )
                f.write("\n")

                # Add ranking section
                f.write("=" * 80 + "\n")
                f.write("CANDIDATE RANKING (Best to Worst)\n")
                f.write("=" * 80 + "\n\n")

                # Create list of (result, performance) and sort by performance descending
                ranked = [
                    (r, r.get("real_performance"))
                    for r in results
                    if r.get("real_performance") is not None
                ]
                ranked.sort(key=lambda x: x[1], reverse=True)

                for rank, (result, perf) in enumerate(ranked, 1):
                    f.write(
                        f"  {rank}. Candidate {result['candidate_idx'] + 1}: {perf:.4f}\n"
                    )

                f.write("\n")

            f.write("\n" + "=" * 80 + "\n")

        print(f"✓ Analysis saved to: {txt_file}")

    def close(self):
        """Clean up resources."""
        if hasattr(self, "db"):
            try:
                conn = self.db.get_conn()
                conn.close()
            except:
                pass


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test LLM-predicted configurations on real database"
    )
    parser.add_argument("sql_file", help="Path to SQL workload file")
    parser.add_argument(
        "--candidates",
        help="Path to candidates JSON file (default: analysis_output/{workload}_8_candidates.json)",
    )
    parser.add_argument(
        "--config",
        default="../config/config.ini",
        help="Path to config.ini (default: ../config/config.ini)",
    )
    parser.add_argument(
        "--bin-edges",
        default=None,
        help="Path to local bin_edges.pkl (optional, downloads from HuggingFace if not provided)",
    )
    parser.add_argument(
        "--hf-repo",
        default="NisithDissanayake/genknob-tuner",
        help="Hugging Face repo for bin_edges (default: NisithDissanayake/genknob-tuner)",
    )
    parser.add_argument(
        "--output",
        default="analysis_output",
        help="Output directory for results (default: analysis_output)",
    )

    args = parser.parse_args()

    # Determine candidates file path
    if not args.candidates:
        workload_name = Path(args.sql_file).stem
        args.candidates = (
            f"{args.output}/{workload_name}/{workload_name}_8_candidates.json"
        )

    try:
        # Initialize tester
        tester = CandidateTester(
            config_path=args.config,
            bin_edges_path=args.bin_edges,
            hf_repo=args.hf_repo,
        )

        # Test all candidates
        results = tester.test_all_candidates(
            args.sql_file, args.candidates, args.output
        )

        print("\n✓ All candidates tested successfully!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        if "tester" in locals():
            tester.close()


if __name__ == "__main__":
    main()
