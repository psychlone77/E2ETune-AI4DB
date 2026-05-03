import json
import sys
from pathlib import Path

import pandas as pd

if len(sys.argv) < 2:
    raise SystemExit("Usage: python scripts/log_run.py <output_dir> [run_tag]")

out_dir = Path(sys.argv[1])
run_tag = sys.argv[2] if len(sys.argv) > 2 else out_dir.name

with open(out_dir / "test_metrics.json", "r", encoding="utf-8") as f:
    metrics = json.load(f)

meta_path = out_dir / "meta.json"
cfg = {}
if meta_path.exists():
    with open(meta_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

row = {
    "run_id": run_tag,
    "ts": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    "cfg_hash": metrics.get("cfg_hash", ""),
    "branch_sha": cfg.get("git_sha", ""),
    "seed": metrics.get("seed", ""),
    "normalization": cfg.get("normalization_mode", ""),
    "log_target": cfg.get("log_target", False),
    "sampler": "balanced" if cfg.get("use_sampler") else "shuffle",
    "lambda_schedule": cfg.get("lambda_schedule", "warmup0.3"),
    "lambda_max": cfg.get("lambda_max", 0.1),
    "target_label_frac": cfg.get("labeled_target_frac", 0.0),
    "val_macro_mse": cfg.get("val_macro_mse", 0.0),
    "test_pooled_rmse": metrics.get("pooled", {}).get("rmse", 0),
    "test_pooled_mape": metrics.get("pooled", {}).get("mape", 0),
    "test_pooled_spearman": metrics.get("pooled", {}).get("spearman", 0),
    "test_d0_rmse": metrics.get("per_domain", {}).get("0", {}).get("rmse", 0),
    "test_d0_mape": metrics.get("per_domain", {}).get("0", {}).get("mape", 0),
    "test_d0_spearman": metrics.get("per_domain", {}).get("0", {}).get("spearman", 0),
    "test_d1_rmse": metrics.get("per_domain", {}).get("1", {}).get("rmse", 0),
    "test_d1_mape": metrics.get("per_domain", {}).get("1", {}).get("mape", 0),
    "test_d1_spearman": metrics.get("per_domain", {}).get("1", {}).get("spearman", 0),
    "test_d2_rmse": metrics.get("per_domain", {}).get("2", {}).get("rmse", 0),
    "test_d2_mape": metrics.get("per_domain", {}).get("2", {}).get("mape", 0),
    "test_d2_spearman": metrics.get("per_domain", {}).get("2", {}).get("spearman", 0),
    "test_d3_rmse": metrics.get("per_domain", {}).get("3", {}).get("rmse", 0),
    "test_d3_mape": metrics.get("per_domain", {}).get("3", {}).get("mape", 0),
    "test_d3_spearman": metrics.get("per_domain", {}).get("3", {}).get("spearman", 0),
    "domain_acc": metrics.get("domain_acc", 0),
    "train_minutes": metrics.get("train_minutes", 0),
    "notes": cfg.get("notes", "baseline"),
}

runs_csv = Path("experiments/runs.csv")
runs_csv.parent.mkdir(exist_ok=True)
write_header = not runs_csv.exists()

pd.DataFrame([row]).to_csv(runs_csv, index=False, mode="a" if not write_header else "w", header=write_header)

print(f"Logged to {runs_csv}")
