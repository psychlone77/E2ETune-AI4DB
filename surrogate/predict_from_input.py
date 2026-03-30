#!/usr/bin/env python3
"""
Simple wrapper that reads an input.json and outputs cost predictions.
Supports two modes:

1) Delegate mode (predict_cost.py)
     - input.json includes: {
             "model": "/path/to/cost_model.pkl",
             "input": "/path/to/config.json",
             "config_source": "raw" | "default" | "auto",
             "features": "/path/to/features.json" (optional),
             "plans": "/path/to/plans.json" (optional),
             "project_root": "/home/E2ETune-AI4DB" (optional),
             "debug": true/false (optional)
         }

2) Direct-features mode
     - input.json includes precomputed features: {
             "model": "/path/to/cost_model.pkl" (optional; defaults to /home/E2ETune-AI4DB/surrogate/cost_model.pkl),
             "internal_metrics": { ... },
             "workload_features": { ... },
             "query_plan_features": { ... },
             "config": { ... } (optional; knob values)
         }
     The script constructs the feature row (cfg__/metrics__/workload__/plan__) and predicts directly.
"""

import json
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

try:
    import joblib
except Exception:
    joblib = None

DEFAULT_MODEL = "/home/E2ETune-AI4DB/surrogate/cost_model.pkl"
DEFAULT_PROJECT_ROOT = "/home/E2ETune-AI4DB"


def _align_columns(X: pd.DataFrame, model: Any) -> pd.DataFrame:
    feat_list: List[str] = []
    try:
        if hasattr(model, "feature_names_in_") and isinstance(
            model.feature_names_in_, (list, np.ndarray)
        ):
            feat_list = list(model.feature_names_in_)
    except Exception:
        pass
    # Fallback to feature list file
    for p in [
        Path(DEFAULT_MODEL).parent / "best_r2_features.txt",
        Path(DEFAULT_MODEL).parent / "cost_model_features.txt",
    ]:
        if not feat_list and p.exists():
            try:
                with open(p, "r") as f:
                    feat_list = [line.strip() for line in f if line.strip()]
                break
            except Exception:
                pass
    if not feat_list:
        return X
    for c in feat_list:
        if c not in X.columns:
            X[c] = 0.0
    return X[feat_list]


def _load_knob_ranges(project_root: str) -> Dict[str, Dict[str, float]]:
    candidates = [
        Path(project_root) / "knob_config" / "knob_config.json",
        Path("knob_config/knob_config.json"),
    ]
    for p in candidates:
        if p.exists():
            try:
                with open(p, "r") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}


def _normalize_knob(
    orig_key: str, val: Any, knob_ranges: Dict[str, Dict[str, float]]
) -> float:
    rng = knob_ranges.get(orig_key)
    if rng is None:
        try:
            return float(val) if pd.notnull(val) else 0.0
        except Exception:
            return 0.0
    mn = float(rng.get("min", 0.0))
    mx = float(rng.get("max", mn))
    if not pd.notnull(val):
        return 0.0
    v = float(val)
    return 0.0 if mx == mn else (v - mn) / (mx - mn)


def main():
    # Determine input.json path
    if len(sys.argv) > 1:
        input_json_path = Path(sys.argv[1])
    else:
        input_json_path = Path(__file__).parent / "input.json"

    if not input_json_path.exists():
        print(json.dumps({"error": f"input.json not found at {input_json_path}"}))
        sys.exit(1)

    with open(input_json_path, "r") as f:
        cfg = json.load(f)

    # Mode switch: if 'input' present, delegate to predict_cost.py; else, direct-features mode
    input_path = cfg.get("input")
    if input_path:
        model = cfg.get("model", DEFAULT_MODEL)
        project_root = cfg.get("project_root", "/home/E2ETune-AI4DB")
        config_source = cfg.get("config_source", "auto")
        features_path = cfg.get("features")
        plans_path = cfg.get("plans")
        debug = bool(cfg.get("debug", False))

        args = [
            sys.executable,
            str(Path(project_root) / "surrogate" / "predict_cost.py"),
            "--model",
            str(model),
            "--inputs",
            str(input_path),
            "--project-root",
            str(project_root),
            "--config-source",
            str(config_source),
        ]
        if features_path:
            args += ["--features", str(features_path)]
        if plans_path:
            args += ["--plans", str(plans_path)]
        if debug:
            args += ["--debug"]

        try:
            proc = subprocess.run(
                args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
        except Exception as e:
            print(json.dumps({"error": f"Failed to run predictor: {e}"}))
            sys.exit(1)

        if proc.returncode != 0:
            print(json.dumps({"error": "predictor failed", "stderr": proc.stderr}))
            sys.exit(proc.returncode)
        print(proc.stdout)
        return

    # Direct-features mode
    model_path = cfg.get("model", DEFAULT_MODEL)
    if joblib is None:
        print(json.dumps({"error": "joblib not available to load model"}))
        sys.exit(1)
    try:
        model = joblib.load(model_path)
    except Exception as e:
        print(json.dumps({"error": f"Failed to load model at {model_path}: {e}"}))
        sys.exit(1)
    # Collect shared features
    metrics_src = cfg.get("llm_internal_metrics") or cfg.get("internal_metrics", {})
    metrics_flat = pd.json_normalize(metrics_src, sep="__").to_dict(orient="records")
    metrics_flat = metrics_flat[0] if metrics_flat else {}
    wl = cfg.get("workload_features", {})
    wl_numeric = {}
    for k, v in wl.items():
        try:
            wl_numeric[k] = float(v)
        except Exception:
            wl_numeric[k] = v
    plan_feats = cfg.get("query_plan_features", {})
    # Load knob ranges and prepare rows for raw/default/config
    project_root = cfg.get("project_root", DEFAULT_PROJECT_ROOT)
    knob_ranges = _load_knob_ranges(project_root)

    rows: List[Dict[str, Any]] = []
    labels: List[str] = []

    # Helper to build a row given a config dict
    def make_row(config_dict: Dict[str, Any]) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        cfg_flat = pd.json_normalize(config_dict or {}, sep="__").to_dict(
            orient="records"
        )
        cfg_flat = cfg_flat[0] if cfg_flat else {}
        # normalize knobs
        cfg_norm = {}
        for k, v in cfg_flat.items():
            cfg_norm[f"cfg__{k}"] = _normalize_knob(k, v, knob_ranges)
        row.update(cfg_norm)
        # shared features
        row.update({f"metrics__{k}": v for k, v in metrics_flat.items()})
        row.update({f"workload__{k}": v for k, v in wl_numeric.items()})
        if isinstance(plan_feats, dict):
            for k, v in plan_feats.items():
                row[k] = v
        return row

    # Build rows for raw and default if present
    if isinstance(cfg.get("raw_config"), dict):
        rows.append(make_row(cfg["raw_config"]))
        labels.append("llm")
    if isinstance(cfg.get("default_config"), dict):
        rows.append(make_row(cfg["default_config"]))
        labels.append("default")
    # Fallback to single 'config' if no raw/default provided
    if not rows and isinstance(cfg.get("config"), dict):
        rows.append(make_row(cfg["config"]))
        labels.append("config")
    if not rows:
        print(
            json.dumps(
                {
                    "error": "No config found in input.json (expected raw_config/default_config/config)."
                }
            )
        )
        sys.exit(1)

    df = pd.DataFrame(rows)
    # Handle objects and ensure numeric
    cat_cols = [c for c in df.columns if df[c].dtype == "object"]
    if cat_cols:
        df = pd.get_dummies(df, columns=cat_cols, dummy_na=True)
    for col in df.columns:
        if not np.issubdtype(df[col].dtype, np.number):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    X = _align_columns(df.copy(), model)
    X = (
        X.apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], 0.0)
        .fillna(0.0)
    )
    try:
        preds = model.predict(X.values.astype(np.float64))
    except Exception as e:
        print(json.dumps({"error": f"Prediction failed: {e}"}))
        sys.exit(1)

    results = []
    for lbl, p in zip(labels, preds):
        results.append(
            {
                "input": str(input_json_path.name),
                "variant": lbl,
                "predicted_cost": float(p),
            }
        )
    print(json.dumps({"results": results, "features_shape": list(X.shape)}, indent=2))


if __name__ == "__main__":
    main()
