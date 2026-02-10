#!/usr/bin/env python3
"""
LLM Model Output Generator
Loads workload analysis results and generates LLM predictions for optimal knob configurations
"""
import os
import sys
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional
import numpy as np

import torch
from transformers import AutoTokenizer
from peft import AutoPeftModelForCausalLM
import gc

# Constants
MODEL_REPO = "NisithDissanayake/genknob-tuner"
BASE_MODEL = "Qwen/Qwen2.5-Coder-7B-Instruct"
N_BINS = 20

# Regex pattern for parsing query plans
OP_RE = re.compile(r"(\w+(?:\s+\w+)*)\(cost=([\d.]+)\)")


def vectorize_plans(plan_strings: list) -> dict:
    """Parse plan strings into engineered features.
    Produces operator counts/ratios, top-level cost stats, depth proxies, and binary flags.
    """
    op_counts: dict = {}
    top_costs: list = []
    depths: list = []

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


class LLMPredictor:
    """Generate knob configuration predictions using fine-tuned LLM."""

    def __init__(self, benchmark_name: str = "unknown", use_quantization: bool = False):
        """
        Initialize the LLM predictor.

        Args:
            benchmark_name: Name of the benchmark for context
            use_quantization: Whether to use 4-bit quantization (reduces memory)
        """
        print("Initializing LLM Predictor...")

        self.benchmark_name = benchmark_name

        # Clear cache
        torch.cuda.empty_cache() if torch.cuda.is_available() else None
        gc.collect()

        # Load tokenizer
        print(f"Loading tokenizer from {BASE_MODEL}...")
        self.tokenizer = AutoTokenizer.from_pretrained(
            BASE_MODEL, trust_remote_code=True
        )
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        # Load model
        print(f"Loading model from {MODEL_REPO}...")
        try:
            if use_quantization:
                from transformers import BitsAndBytesConfig

                bnb_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_compute_dtype=torch.float16,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                )
                self.model = AutoPeftModelForCausalLM.from_pretrained(
                    MODEL_REPO,
                    quantization_config=bnb_config,
                    device_map="auto",
                    trust_remote_code=True,
                )
                print("Model loaded with 4-bit quantization.")
            else:
                self.model = AutoPeftModelForCausalLM.from_pretrained(
                    MODEL_REPO,
                    device_map="cpu",
                    low_cpu_mem_usage=True,
                    dtype=torch.float16,
                    trust_remote_code=True,
                )
                self.model.to("cpu")
                print("Model loaded on CPU.")

            self.model.eval()
        except Exception as e:
            print(f"Error loading model: {e}")
            raise

    def load_workload_data(
        self, analysis_dir: str, workload_name: str
    ) -> Dict[str, Any]:
        # Look in workload-specific subfolder
        workload_dir = Path(analysis_dir) / workload_name
        if not workload_dir.exists():
            # Fallback to root analysis_dir for backward compatibility
            workload_dir = Path(analysis_dir)
        """
        Load workload analysis results (query plans, features, and internal metrics).

        Args:
            analysis_dir: Directory containing analysis outputs
            workload_name: Name of the workload (without extension)

        Returns:
            Dictionary with query plans, features, and internal metrics
        """
        analysis_path = Path(analysis_dir)

        # Load query plans
        plans_file = workload_dir / f"{workload_name}_plans.json"
        query_plans = []
        if plans_file.exists():
            with open(plans_file, "r") as f:
                query_plans = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(query_plans)} query plans from {plans_file}")
        else:
            print(f"Warning: Query plans file not found: {plans_file}")

        # Load workload features
        features_file = workload_dir / f"{workload_name}_features.json"
        workload_features = {}
        if features_file.exists():
            with open(features_file, "r") as f:
                workload_features = json.load(f)
            print(
                f"Loaded {len(workload_features)} workload features from {features_file}"
            )
        else:
            print(f"Warning: Features file not found: {features_file}")

        # Load internal metrics
        metrics_file = analysis_path / f"{workload_name}_internal_metrics.json"
        internal_metrics = {}
        if metrics_file.exists():
            with open(metrics_file, "r") as f:
                internal_metrics = json.load(f)
            print(
                f"Loaded {len(internal_metrics)} internal metrics from {metrics_file}"
            )
        else:
            print(f"Warning: Internal metrics file not found: {metrics_file}")

        # Vectorize query plans
        query_plan_features = vectorize_plans(query_plans)
        print(f"Vectorized query plans into {len(query_plan_features)} features")

        return {
            "query_plans": query_plans,
            "workload_features": workload_features,
            "query_plan_features": query_plan_features,
            "internal_metrics": internal_metrics,
        }

    def format_instruction(
        self, internal_metrics: Dict, workload_features: Dict, query_plan_features: Dict
    ) -> str:
        """
        Format the instruction prompt for the LLM.

        Args:
            internal_metrics: Database internal metrics
            workload_features: Workload-level features
            query_plan_features: Vectorized query plan features

        Returns:
            Formatted prompt string
        """
        metrics_str = "\n".join(
            [f"- {k}: {v}" for k, v in internal_metrics.items() if float(v) > 0]
        )
        workload_str = "\n".join([f"- {k}: {v}" for k, v in workload_features.items()])
        plan_str = "\n".join([f"- {k}: {v}" for k, v in query_plan_features.items()])

        user_prompt = f"""You are an expert PostgreSQL DBA. Analyze the workload state, features, and query plans below.
Determine the optimal database configuration knobs to maximize performance.
Output the configuration as a JSON object where values are Bin Indices (0-{N_BINS-1}).

### Workload Context ({self.benchmark_name}):
**Internal Metrics (State):**
{metrics_str}

**Workload Features (Stats):**
{workload_str}

**Query Plan Signals (Structure):**
{plan_str}

### Task:
Predict the optimal configuration bucket for each knob."""

        return f"<|im_start|>system\nYou are a database tuning assistant.<|im_end|>\n<|im_start|>user\n{user_prompt}<|im_end|>\n<|im_start|>assistant\n"

    def generate_prediction(self, prompt: str) -> str:
        """
        Generate model prediction for the given prompt.

        Args:
            prompt: Formatted instruction prompt

        Returns:
            Generated text
        """
        print("Generating prediction...")
        inputs = self.tokenizer(prompt, return_tensors="pt")

        # Move inputs to same device as model
        if hasattr(self.model, "device"):
            inputs = {k: v.to(self.model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs, max_new_tokens=256, temperature=0.0, do_sample=False
            )

        text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        return text

    def extract_json(self, text: str) -> Optional[Dict]:
        """
        Extract JSON configuration from model output.

        Args:
            text: Generated text from model

        Returns:
            Parsed JSON dict or None
        """
        import ast

        m = re.search(r"\{.*?\}", text, flags=re.DOTALL)
        if not m:
            return None
        block = m.group(0)
        try:
            return json.loads(block)
        except Exception:
            try:
                return ast.literal_eval(block)
            except Exception:
                return None

    def predict_configuration(
        self, analysis_dir: str, workload_name: str, output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate complete prediction for a workload.

        Args:
            analysis_dir: Directory with analysis results
            workload_name: Name of workload (without extension)
            output_file: Optional path to save results

        Returns:
            Dictionary with prediction results
        """
        print(f"\n{'='*80}")
        print(f"Generating LLM Prediction for: {workload_name}")
        print(f"{'='*80}\n")

        # Load workload data (includes internal metrics)
        workload_data = self.load_workload_data(analysis_dir, workload_name)

        # Format prompt
        prompt = self.format_instruction(
            workload_data["internal_metrics"],
            workload_data["workload_features"],
            workload_data["query_plan_features"],
        )

        # Generate prediction
        raw_output = self.generate_prediction(prompt)

        # Extract predicted bins
        predicted_bins = self.extract_json(raw_output)

        results = {
            "workload_name": workload_name,
            "benchmark": self.benchmark_name,
            "predicted_bins": predicted_bins,
            "raw_output": raw_output,
            "internal_metrics": workload_data["internal_metrics"],
            "workload_features": workload_data["workload_features"],
            "query_plan_features": workload_data["query_plan_features"],
        }

        # Save results
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(results, f, indent=4)
            print(f"\n✓ Results saved to: {output_path}")

        # Display results
        print(f"\n{'='*80}")
        print("Prediction Results")
        print(f"{'='*80}")
        print(f"Workload: {workload_name}")
        print(f"Predicted Bins: {predicted_bins}")
        print(f"\nRaw Output (first 500 chars):")
        print("-" * 80)
        print(raw_output[:500] + ("..." if len(raw_output) > 500 else ""))
        print("-" * 80)
        print(f"{'='*80}\n")

        return results

    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, "model"):
            del self.model
        if hasattr(self, "tokenizer"):
            del self.tokenizer
        gc.collect()
        torch.cuda.empty_cache() if torch.cuda.is_available() else None


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate LLM predictions for database knob configurations"
    )
    parser.add_argument(
        "workload_name",
        help="Name of the workload (without extension, e.g., 'job_0-3_first_3')",
    )
    parser.add_argument(
        "--analysis-dir",
        default="analysis_output",
        help="Directory containing analysis results (default: analysis_output)",
    )
    parser.add_argument("--output", help="Output file path for results (optional)")
    parser.add_argument(
        "--benchmark",
        default="unknown",
        help="Benchmark name for context (default: unknown)",
    )
    parser.add_argument(
        "--quantize",
        action="store_true",
        help="Use 4-bit quantization (requires less memory)",
    )

    args = parser.parse_args()

    try:
        # Initialize predictor
        predictor = LLMPredictor(
            benchmark_name=args.benchmark, use_quantization=args.quantize
        )

        # Generate prediction
        output_file = (
            args.output
            or f"{args.analysis_dir}/{args.workload_name}_llm_prediction.json"
        )
        results = predictor.predict_configuration(
            args.analysis_dir, args.workload_name, output_file
        )

        print("\n✓ Prediction complete!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        if "predictor" in locals():
            predictor.cleanup()


if __name__ == "__main__":
    main()
