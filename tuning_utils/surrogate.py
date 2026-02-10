import joblib
import json
import os
import re
import numpy as np
import pandas as pd


class Surrogate:
    def __init__(self, sur_config, workload_path) -> None:
        # Simplified config: only model_path is required
        self.model = joblib.load(sur_config["model_path"])
        self.workload_path = workload_path

        # Resolve project_root (three levels up from workload_path) and load train-only scaler stats
        try:
            self.project_root = os.path.dirname(
                os.path.dirname(os.path.dirname(self.workload_path))
            )
        except Exception:
            self.project_root = os.getcwd()

        scaler_path = os.path.join(
            self.project_root, "surrogate", "feature_scaler.json"
        )
        self.scaler_min = {}
        self.scaler_max = {}
        try:
            if os.path.exists(scaler_path):
                with open(scaler_path, "r") as f:
                    scaler = json.load(f)
                # Expected format: {"min": {col: val}, "max": {col: val}}
                self.scaler_min = scaler.get("min", {}) or {}
                self.scaler_max = scaler.get("max", {}) or {}
            else:
                # Proceed without normalization if scaler stats are missing
                self.scaler_min = {}
                self.scaler_max = {}
        except Exception:
            # Robust fallback: no normalization
            self.scaler_min = {}
            self.scaler_max = {}

        # Feature names in the exact order expected by the model (65 features)
        self.feature_names = [
            "cfg__shared_buffers",
            "cfg__work_mem",
            "cfg__maintenance_work_mem",
            "cfg__effective_cache_size",
            "cfg__max_connections",
            "cfg__wal_buffers",
            "cfg__checkpoint_completion_target",
            "cfg__checkpoint_timeout",
            "cfg__effective_io_concurrency",
            "cfg__join_collapse_limit",
            "cfg__from_collapse_limit",
            "cfg__bgwriter_delay",
            "cfg__bgwriter_lru_multiplier",
            "cfg__default_statistics_target",
            "cfg__max_parallel_workers_per_gather",
            "metrics__xact_commit",
            "metrics__xact_rollback",
            "metrics__blks_read",
            "metrics__blks_hit",
            "metrics__tup_returned",
            "metrics__tup_fetched",
            "metrics__tup_inserted",
            "metrics__conflicts",
            "metrics__tup_updated",
            "metrics__tup_deleted",
            "metrics__disk_read_count",
            "metrics__disk_write_count",
            "metrics__disk_read_bytes",
            "metrics__disk_write_bytes",
            "plan__count__sort",
            "plan__count__aggregate",
            "plan__count__gather",
            "plan__count__hash_join",
            "plan__count__seq_scan",
            "plan__count__hash",
            "plan__count__gather_merge",
            "plan__count__nested_loop",
            "plan__count__bitmap_heap_scan",
            "plan__count__bitmap_index_scan",
            "plan__count__index_scan",
            "plan__count__merge_join",
            "plan__ratio__sort",
            "plan__ratio__aggregate",
            "plan__ratio__gather",
            "plan__ratio__hash_join",
            "plan__ratio__seq_scan",
            "plan__ratio__hash",
            "plan__ratio__gather_merge",
            "plan__ratio__nested_loop",
            "plan__ratio__bitmap_heap_scan",
            "plan__ratio__bitmap_index_scan",
            "plan__ratio__index_scan",
            "plan__ratio__merge_join",
            "plan__cost_mean",
            "plan__cost_std",
            "plan__cost_min",
            "plan__cost_max",
            "plan__depth_mean",
            "plan__depth_max",
            "plan__count__index_only_scan",
            "plan__ratio__index_only_scan",
            "plan__count__materialize",
            "plan__ratio__materialize",
            "plan__count__limit",
            "plan__ratio__limit",
        ]

        self.op_re = re.compile(r"([A-Za-z ]+)\(cost=([0-9.]+)\)")

        # Load knob ranges for normalization
        self.knob_ranges = self._load_knob_ranges()

    def _load_knob_ranges(self):
        """Load knob configuration ranges for normalization."""
        candidates = [
            os.path.join(self.project_root, "knob_config", "knob_config.json"),
            "knob_config/knob_config.json",
        ]
        for p in candidates:
            if os.path.exists(p):
                with open(p, "r") as f:
                    return json.load(f)
        return {}

    def _normalize_knob(self, orig_key, val):
        """Normalize a knob value to [0, 1] range using knob_config ranges."""
        rng = self.knob_ranges.get(orig_key)
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

    def _align_columns(self, X):
        """Align dataframe columns to match model's expected features."""
        feat_list = []
        try:
            if hasattr(self.model, "feature_names_in_") and isinstance(
                self.model.feature_names_in_, (list, np.ndarray)
            ):
                feat_list = list(self.model.feature_names_in_)
        except Exception:
            pass

        if not feat_list:
            # Use the hardcoded feature_names as fallback
            feat_list = self.feature_names

        # Add missing columns
        for c in feat_list:
            if c not in X.columns:
                X[c] = 0.0

        return X[feat_list]

    def _vectorize_plans(self, plan_strings):
        op_counts = {}
        for s in plan_strings or []:
            ops = self.op_re.findall(s)
            for name, _ in ops:
                key = name.strip().lower().replace(" ", "_")
                op_counts[key] = op_counts.get(key, 0) + 1

        total_ops = sum(op_counts.values()) or 1

        # Expected operators for the 65-feature model
        expected_ops = [
            "aggregate",
            "bitmap_heap_scan",
            "bitmap_index_scan",
            "gather",
            "gather_merge",
            "hash",
            "hash_join",
            "index_only_scan",
            "index_scan",
            "limit",
            "materialize",
            "merge_join",
            "nested_loop",
            "seq_scan",
            "sort",
            "subquery_scan",
        ]

        feats = {}
        for op in expected_ops:
            count = op_counts.get(op, 0)
            feats[f"plan__count__{op}"] = float(count)
            feats[f"plan__ratio__{op}"] = float(count / total_ops)
        return feats

    def _apply_train_minmax(self, row: dict) -> dict:
        """
        Apply train-only min–max normalization to metrics__, workload__, and plan__* columns
        (excluding plan__has_* flags) using stats from feature_scaler.json.
        Missing or non-finite values are set to 0.0. Results clipped to [0, 1].
        """
        if not self.scaler_min or not self.scaler_max:
            return row

        out = dict(row)
        for k, v in row.items():
            if (
                k.startswith("metrics__")
                or k.startswith("workload__")
                or (k.startswith("plan__") and not k.startswith("plan__has_"))
            ):
                mn = self.scaler_min.get(k)
                mx = self.scaler_max.get(k)
                if mn is None or mx is None:
                    # No scaler stats for this column
                    continue
                try:
                    val = float(v)
                except Exception:
                    val = 0.0
                rng = mx - mn
                if rng <= 0:
                    out[k] = 0.0
                else:
                    scaled = (val - mn) / rng
                    # Clip to [0, 1]
                    if not np.isfinite(scaled):
                        scaled = 0.0
                    out[k] = min(1.0, max(0.0, float(scaled)))
        return out

    def _load_workload_features(self):
        # Extract bench and idx from workload_path
        # Example: /home/ubuntu/.../job/job_0_features.json
        try:
            filename = os.path.basename(self.workload_path)
            # Match patterns like job_0, tpch_1, etc.
            match = re.search(r"([a-z]+)_(\d+)", filename)
            if match:
                bench, idx = match.groups()
                # Construct path to features.json
                # Assuming the structure is project_root/workload_features/bench/bench_idx_features.json
                # We try to find the project root by going up from workload_path
                project_root = os.path.dirname(
                    os.path.dirname(os.path.dirname(self.workload_path))
                )
                feat_path = os.path.join(
                    project_root,
                    "workload_features",
                    bench,
                    f"{bench}_{idx}_features.json",
                )

                if os.path.exists(feat_path):
                    with open(feat_path, "r") as f:
                        return json.load(f)
        except Exception:
            pass
        return {}

    def _load_query_plans(self):
        try:
            filename = os.path.basename(self.workload_path)
            match = re.search(r"([a-z]+)_(\d+)", filename)
            if match:
                bench, idx = match.groups()
                project_root = os.path.dirname(
                    os.path.dirname(os.path.dirname(self.workload_path))
                )
                plan_path = os.path.join(
                    project_root, "query_plans", bench, f"{bench}_{idx}_plans.json"
                )

                if os.path.exists(plan_path):
                    with open(plan_path, "r") as f:
                        data = json.load(f)
                        return data.get("query_plans", [])
        except Exception:
            pass
        return []

    def run(
        self,
        inner_metrics_dict,
        normalized_knobs_dict,
        workload_features=None,
        query_plans=None,
    ):
        """
        Run prediction using pandas preprocessing pipeline (matching predict_from_input.py).

        Args:
            inner_metrics_dict: Internal metrics from database
            normalized_knobs_dict: Raw or normalized knob configuration
            workload_features: Optional workload features dict
            query_plans: Optional list of query plan strings

        Returns:
            Predicted performance value
        """
        # 1. Build feature row with pandas preprocessing
        row = {}

        # Config features - normalize if needed and flatten
        cfg_flat = pd.json_normalize(normalized_knobs_dict or {}, sep="__").to_dict(
            orient="records"
        )
        cfg_flat = cfg_flat[0] if cfg_flat else {}
        cfg_norm = {}
        for orig_key, raw_val in cfg_flat.items():
            norm_val = self._normalize_knob(orig_key, raw_val)
            cfg_norm[f"cfg__{orig_key}"] = norm_val
        row.update(cfg_norm)

        # Metrics features - flatten
        metrics_flat = pd.json_normalize(inner_metrics_dict or {}, sep="__").to_dict(
            orient="records"
        )
        metrics_flat = metrics_flat[0] if metrics_flat else {}
        for k, v in metrics_flat.items():
            row[f"metrics__{k}"] = v

        # Workload features - convert to numeric (currently commented out in both files)
        # if workload_features:
        #     wl_numeric = {}
        #     for k, v in workload_features.items():
        #         try:
        #             wl_numeric[k] = float(v) if pd.notnull(v) else 0.0
        #         except Exception:
        #             wl_numeric[k] = 0.0
        #     for k, v in wl_numeric.items():
        #         row[f"workload__{k}"] = v

        # Plan features - use provided or load from file
        if query_plans is None:
            query_plans = self._load_query_plans()
        plan_feats = self._vectorize_plans(query_plans)
        row.update(plan_feats)

        # 2. Convert to DataFrame and preprocess (matching predict_from_input.py)
        df = pd.DataFrame([row])

        # Handle categorical columns
        cat_cols = [c for c in df.columns if df[c].dtype == "object"]
        if cat_cols:
            df = pd.get_dummies(df, columns=cat_cols, dummy_na=True)

        # Convert all to numeric
        for col in df.columns:
            if not np.issubdtype(df[col].dtype, np.number):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. Align columns to model's expected features
        X = self._align_columns(df.copy())
        X = (
            X.apply(pd.to_numeric, errors="coerce")
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
        )

        # 4. Predict
        prediction_log = self.model.predict(X.values.astype(np.float64))[0]
        print(f"Surrogate model log1p prediction: {prediction_log}")
        prediction = np.expm1(prediction_log)

        return float(prediction)
