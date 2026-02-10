#!/usr/bin/env python3
"""
Run Model Output - Test LLM-predicted configurations
Converts LLM bin predictions to raw values and tests performance
"""
import os
import sys
import json
import pickle
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.parse_config import parse_args
from Database import Database
from stress_testing_tool import stress_testing_tool
from knob_config import parse_knob_config
import utils

# For downloading from Hugging Face
from huggingface_hub import hf_hub_download


class ConfigurationTester:
    """Tests LLM-predicted configurations by converting bins to raw values."""

    def __init__(
        self,
        config_path: str = "../config/config.ini",
        bin_edges_path: str = None,
        hf_repo: str = "NisithDissanayake/genknob-tuner",
    ):
        """
        Initialize the configuration tester.

        Args:
            config_path: Path to config.ini
            bin_edges_path: Path to local bin_edges.pkl file (optional, will download from HF if not provided)
            hf_repo: Hugging Face repository to download bin_edges from
        """
        print("Initializing Configuration Tester...")

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
        print("Stress testing tool initialized.\n")

        # Load knob configuration
        self.knobs_detail = parse_knob_config.get_knobs(knob_config_path)

    def bins_to_raw_values(self, predicted_bins: Dict[str, int]) -> Dict[str, Any]:
        """
        Convert bin indices to raw knob values.

        Args:
            predicted_bins: Dictionary mapping knob names to bin indices

        Returns:
            Dictionary mapping knob names to raw values
        """
        print("Converting bin indices to raw values...")
        raw_config = {}

        for knob, bin_idx in predicted_bins.items():
            if knob not in self.bin_edges:
                print(f"  Warning: No bin edges found for knob '{knob}', skipping")
                continue

            edges = self.bin_edges[knob]

            # Convert bin index to integer
            try:
                idx = int(bin_idx)
            except (ValueError, TypeError):
                try:
                    idx = int(float(bin_idx))
                except (ValueError, TypeError):
                    print(
                        f"  Warning: Invalid bin index for '{knob}': {bin_idx}, using 0"
                    )
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
            print(f"  {knob}: bin {idx} -> {raw_value}")

        print(f"Converted {len(raw_config)} knobs to raw values\n")
        return raw_config

    def load_llm_prediction(self, prediction_file: str) -> Dict[str, Any]:
        """
        Load LLM prediction from JSON file.

        Args:
            prediction_file: Path to LLM prediction JSON

        Returns:
            Dictionary containing prediction data
        """
        pred_path = Path(prediction_file)
        if not pred_path.exists():
            raise FileNotFoundError(f"Prediction file not found: {prediction_file}")

        print(f"Loading LLM prediction from: {pred_path}")
        with open(pred_path, "r") as f:
            data = json.load(f)

        workload_name = data.get("workload_name", "unknown")
        predicted_bins = data.get("predicted_bins", {})

        print(f"Loaded prediction for workload: {workload_name}")
        print(f"Predicted bins for {len(predicted_bins)} knobs\n")

        return data

    def test_configuration(
        self, sql_file: str, prediction_file: str, output_dir: str = "analysis_output"
    ) -> Dict[str, Any]:
        """
        Test LLM-predicted configuration.

        Args:
            sql_file: Path to SQL workload file
            prediction_file: Path to LLM prediction JSON
            output_dir: Output directory for results

        Returns:
            Dictionary with test results
        """
        # Resolve SQL file path
        script_dir = Path(__file__).parent
        sql_path = Path(sql_file)

        if not sql_path.is_absolute():
            sql_path = (script_dir / sql_file).resolve()

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        workload_name = sql_path.stem

        print(f"\n{'='*80}")
        print(f"Testing LLM Configuration for: {workload_name}")
        print(f"{'='*80}\n")

        # Load LLM prediction
        prediction_data = self.load_llm_prediction(prediction_file)
        predicted_bins = prediction_data.get("predicted_bins", {})

        if not predicted_bins:
            raise ValueError("No predicted bins found in prediction file")

        # Convert bins to raw values
        raw_config = self.bins_to_raw_values(predicted_bins)

        # Test configuration with stress testing tool
        print("Testing configuration with stress testing tool...")

        try:
            # Reset metrics before testing
            self.db.reset_inner_metrics()

            # Update config with workload path
            original_workload = self.config["benchmark_config"].get("workload_path")
            self.config["benchmark_config"]["workload_path"] = str(sql_path)

            # Run stress test with LLM configuration
            performance = self.stress_tester.test_config(raw_config, iteration=1)

            # Restore original workload path
            if original_workload:
                self.config["benchmark_config"]["workload_path"] = original_workload

            # Fetch internal metrics after workload execution
            internal_metrics = self.db.fetch_inner_metrics()

            print(f"\n✓ Configuration tested")
            print(f"  Performance: {performance:.4f}\n")

        except Exception as e:
            print(f"\n✗ Error testing configuration: {e}\n")
            raise

        # Prepare results
        results = {
            "workload_name": workload_name,
            "workload_file": str(sql_path),
            "benchmark": self.benchmark_name,
            "predicted_bins": predicted_bins,
            "raw_config": raw_config,
            "llm_performance": performance,
            "llm_internal_metrics": internal_metrics,
        }

        # Load default performance for comparison
        output_path = Path(output_dir)
        default_perf_file = output_path / f"{workload_name}_default_performance.json"
        if default_perf_file.exists():
            with open(default_perf_file, "r") as f:
                default_data = json.load(f)
                results["default_performance"] = default_data.get("performance", None)
                results["default_config"] = default_data.get("knobs", {})

        # Calculate improvement
        if results.get("default_performance"):
            improvement = (
                (performance - results["default_performance"])
                / results["default_performance"]
                * 100
            )
            results["improvement_percent"] = improvement
            print(f"Performance Comparison:")
            print(f"  Default:  {results['default_performance']:.4f}")
            print(f"  LLM:      {performance:.4f}")
            print(f"  Improvement: {improvement:+.2f}%\n")

        # Save results
        output_file = output_path / f"{workload_name}_llm_test_results.json"
        output_path.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(results, f, indent=4)

        print(f"✓ Results saved to: {output_file}")

        # Summary
        print(f"\n{'='*80}")
        print("Test Summary")
        print(f"{'='*80}")
        print(f"Workload: {workload_name}")
        print(f"LLM Performance: {performance:.4f}")
        if results.get("default_performance"):
            print(f"Default Performance: {results['default_performance']:.4f}")
            print(f"Improvement: {results.get('improvement_percent', 0):+.2f}%")
        print(f"Configuration tested: {len(raw_config)} knobs")
        print(f"Output: {output_file}")
        print(f"{'='*80}\n")

        return results

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
        description="Test LLM-predicted database configurations"
    )
    parser.add_argument("sql_file", help="Path to SQL workload file")
    parser.add_argument(
        "--prediction",
        help="Path to LLM prediction JSON file (default: analysis_output/{workload}_llm_prediction.json)",
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

    # Determine prediction file path
    if not args.prediction:
        workload_name = Path(args.sql_file).stem
        args.prediction = f"{args.output}/{workload_name}_llm_prediction.json"

    try:
        # Initialize tester
        tester = ConfigurationTester(
            config_path=args.config, bin_edges_path=args.bin_edges, hf_repo=args.hf_repo
        )

        # Test configuration
        results = tester.test_configuration(args.sql_file, args.prediction, args.output)

        print("\n✓ Testing complete!")

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
