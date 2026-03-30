#!/usr/bin/env python3
"""
Run Cost Model on 8 Candidates
Logic: Loads raw configurations, internal metrics, and workload features.
       Constructs a feature vector matching 'predict_from_input.py' logic.
       Predicts performance using the surrogate model pickle.
"""
import os
import sys
import json
import joblib
import csv
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

# Define paths relative to this script
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
SURROGATE_DIR = PROJECT_ROOT / "surrogate"
MODEL_PATH = SURROGATE_DIR / "cost_model.pkl"
FEATURE_LIST_PATH = SURROGATE_DIR / "best_r2_features.txt"
KNOB_CONFIG_PATH = PROJECT_ROOT / "knob_config/knob_config.json"


class CostModelTester:
    def __init__(self):
        print("Initializing Cost Model Tester...")

        # 1. Load the Surrogate Model
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Model not found at: {MODEL_PATH}")
        try:
            self.model = joblib.load(MODEL_PATH)
            print(f"✅ Loaded model from: {MODEL_PATH}")
        except Exception as e:
            raise RuntimeError(f"Failed to load model: {e}")

        # 2. Load Feature Names (Critical for strict alignment)
        self.feature_names = []
        # Priority A: Check if model has feature names saved
        if hasattr(self.model, "feature_names_in_"):
            self.feature_names = list(self.model.feature_names_in_)
        # Priority B: Load from text file
        elif FEATURE_LIST_PATH.exists():
            with open(FEATURE_LIST_PATH, "r") as f:
                self.feature_names = [line.strip() for line in f if line.strip()]

        if not self.feature_names:
            print(
                "⚠️ WARNING: No feature list found. Columns will not be aligned/padded."
            )
        else:
            print(f"✅ Loaded {len(self.feature_names)} expected features.")

        # 3. Load Knob Config (for Normalization)
        if not KNOB_CONFIG_PATH.exists():
            print(f"⚠️ Knob config not found at {KNOB_CONFIG_PATH}")
            self.knobs_detail = {}
        else:
            with open(KNOB_CONFIG_PATH, "r") as f:
                data = json.load(f)
                # Handle potential nested structure
                self.knobs_detail = data.get("knobs", data)

    def normalize_knobs(self, raw_config: Dict[str, Any]) -> Dict[str, float]:
        """Normalize knobs to [0,1] based on config min/max."""
        normalized = {}
        for knob, value in raw_config.items():
            if knob in self.knobs_detail:
                detail = self.knobs_detail[knob]
                min_val = detail.get("min", 0)
                max_val = detail.get("max", 1)

                try:
                    val_float = float(value)
                    if max_val > min_val:
                        normalized[knob] = (val_float - min_val) / (max_val - min_val)
                    else:
                        normalized[knob] = 0.0
                except (ValueError, TypeError):
                    normalized[knob] = 0.0
        return normalized

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Force DataFrame to match the exact column order of training data.
        Fills missing columns with 0. Drops extra columns.
        """
        if not self.feature_names:
            return df

        # Create empty DF with correct columns
        aligned = pd.DataFrame(0.0, index=df.index, columns=self.feature_names)

        # Fill strictly where columns match
        common_cols = [c for c in df.columns if c in self.feature_names]
        aligned[common_cols] = df[common_cols]

        return aligned

    def predict(self, raw_config, internal_metrics, workload_features, plan_features):
        """
        Construct feature vector row (cfg__*, metrics__*, etc.) and predict.
        """
        try:
            # 1. Knob Features (cfg__)
            cfg_norm = self.normalize_knobs(raw_config)
            row = {f"cfg__{k}": v for k, v in cfg_norm.items()}

            # 2. Internal Metrics (metrics__) - Flatten nested dicts
            metrics_flat = pd.json_normalize(internal_metrics or {}, sep="__").to_dict(
                orient="records"
            )
            metrics = metrics_flat[0] if metrics_flat else {}
            row.update({f"metrics__{k}": v for k, v in metrics.items()})

            # 3. Workload Features (workload__)
            row.update(
                {f"workload__{k}": v for k, v in (workload_features or {}).items()}
            )

            # 4. Plan Features (plan__)
            # Usually plan features are already flat (e.g. plan__cost_mean)
            row.update(plan_features or {})

            # 5. Build DataFrame
            df = pd.DataFrame([row])

            # Clean numeric types
            df = df.apply(pd.to_numeric, errors="coerce").fillna(0.0)

            # 6. Align Columns
            X = self._align_columns(df)

            # 7. Predict
            pred = self.model.predict(X.values.astype(np.float64))
            return float(pred[0])

        except Exception as e:
            print(f"  ❌ Prediction failed: {e}")
            return None

    def load_json(self, path: Path):
        if not path.exists():
            print(f"Warning: File not found {path}")
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def run_batch(self, workload_name: str, analysis_dir: str):
        analysis_path = Path(analysis_dir).resolve()

        # Check for workload-specific subfolder
        workload_dir = analysis_path / workload_name
        if not workload_dir.exists():
            workload_dir = analysis_path  # Fallback to root directory

        # Input Files
        metrics_file = workload_dir / f"{workload_name}_internal_metrics.json"
        features_file = workload_dir / f"{workload_name}_features.json"
        plans_file = (
            workload_dir / f"{workload_name}_plan_features.json"
        )  # If using vectorized plan features
        candidates_file = workload_dir / f"{workload_name}_candidates_full_results.json"

        # Load Context
        print(f"\nLoading context for {workload_name}...")
        metrics = self.load_json(metrics_file)
        wl_features = self.load_json(features_file)

        # Note: If plan_features.json doesn't exist, we might be relying on raw plans.
        # However, predict_from_input logic usually requires PRE-VECTORIZED plan features.
        # If your vectorizer is separate, ensure this file exists.
        plan_features = self.load_json(plans_file)

        # Load Candidates
        if not candidates_file.exists():
            print(f"❌ Candidates file not found: {candidates_file}")
            return

        with open(candidates_file, "r") as f:
            data = json.load(f)
            # Handle list wrapper vs dict wrapper
            candidates = data.get("results", data) if isinstance(data, dict) else data

        results = []
        print(f"\n🚀 Predicting performance for {len(candidates)} candidates...")

        for i, item in enumerate(candidates):
            # Extract raw config (handle if it's nested or flat)
            raw_config = item.get("raw_config", item)
            idx = item.get("candidate_idx", i)

            score = self.predict(raw_config, metrics, wl_features, plan_features)

            if score is not None:
                print(f"  Candidate {idx+1}: {score:.4f}")
                results.append(
                    {
                        "candidate_idx": idx,
                        "surrogate_performance": score,
                        "raw_config": raw_config,
                    }
                )

        # Save Results
        output_csv = workload_dir / f"{workload_name}_cost_model_results.csv"
        self.save_csv(results, output_csv)
        print(f"\n✅ Results saved to: {output_csv}")

    def save_csv(self, results, path):
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Candidate", "Surrogate Performance"])
            for r in results:
                writer.writerow(
                    [r["candidate_idx"] + 1, f"{r['surrogate_performance']:.4f}"]
                )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("workload_name", help="Name of the workload (e.g., job_12)")
    parser.add_argument(
        "--analysis-dir",
        default="analysis_output",
        help="Path to analysis output directory",
    )
    args = parser.parse_args()

    try:
        tester = CostModelTester()
        tester.run_batch(args.workload_name, args.analysis_dir)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
