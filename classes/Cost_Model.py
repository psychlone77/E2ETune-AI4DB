import json
import joblib
import os
import pandas as pd
import numpy as np
import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional

from sklearn.impute import SimpleImputer

from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask, WorkloadRunner
from classes.base_classes.Surrogate_Strategy import SurrogateStrategy

# Create a "fake" module entry for numpy._core
if not hasattr(np, "_core"):
    sys.modules["numpy._core"] = np.core

class CostModelPredictor:
    """Inference helper for notebook-trained joblib artifacts."""

    def __init__(
        self,
        model_path: str = "surrogate/saved_models/knob_cost_models_v1",
        preferred_model: str = "lgbm",
        default_engine: Optional[str] = None,
        default_ram_gb: Optional[int] = None,
    ):
        self.preferred_model = preferred_model
        self.default_engine = default_engine.lower().strip() if default_engine else None
        self.default_ram_gb = default_ram_gb
        self.artifacts_by_engine: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.engine_names = []
        # Explicit best-model choices from evaluation table.
        self.best_model_by_bucket = {
            ("mysql", "32"): "lgbm_ranker",
            ("postgresql", "32"): "hgb",
            ("postgresql", "64"): "rf",
        }
        self._load_artifacts(model_path)

    @staticmethod
    def _to_float_or_zero(v: Any) -> float:
        try:
            if v is None:
                return 0.0
            return float(v)
        except Exception:
            return 0.0

    @staticmethod
    def _detect_system_ram_gb() -> Optional[int]:
        try:
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = float(line.split()[1])
                        return int(round(kb / (1024.0 * 1024.0)))
        except Exception:
            return None
        return None

    @staticmethod
    def _parse_engine_ram_from_filename(path: Path) -> tuple[Optional[str], Optional[str]]:
        m = re.match(r"model_engine=(.+?)_ram=(.+?)\.joblib$", path.name)
        if not m:
            return None, None
        return m.group(1).strip().lower(), m.group(2).strip().lower()

    def _register_artifact(self, artifact: Dict[str, Any], source_name: str) -> None:
        engine = str(artifact.get("engine", "")).strip().lower()
        ram_bucket = str(artifact.get("ram_bucket", "all")).strip().lower()
        if not engine:
            raise ValueError(f"Missing 'engine' in artifact loaded from {source_name}")

        if engine not in self.artifacts_by_engine:
            self.artifacts_by_engine[engine] = {}
        self.artifacts_by_engine[engine][ram_bucket] = artifact

    def _load_artifacts(self, model_path: str) -> None:
        p = Path(model_path)
        if p.is_file() and p.name == "manifest.joblib":
            manifest = joblib.load(p)
            model_dir = Path(manifest.get("model_dir", p.parent))
            for art in manifest.get("artifacts", []):
                art_path = Path(art)
                if not art_path.is_absolute():
                    art_path = model_dir / art_path
                if art_path.exists():
                    self._register_artifact(joblib.load(art_path), str(art_path))
        elif p.is_file() and p.suffix == ".joblib":
            obj = joblib.load(p)
            if isinstance(obj, dict) and "artifacts" in obj:
                model_dir = Path(obj.get("model_dir", p.parent))
                for art in obj.get("artifacts", []):
                    art_path = Path(art)
                    if not art_path.is_absolute():
                        art_path = model_dir / art_path
                    if art_path.exists():
                        self._register_artifact(joblib.load(art_path), str(art_path))
            elif isinstance(obj, dict) and "models" in obj and "feature_cols" in obj:
                self._register_artifact(obj, str(p))
            else:
                raise ValueError(
                    f"Unsupported model file format: {p}. Expected notebook artifact/manifest joblib files."
                )
        else:
            model_dir = p if p.exists() else Path("surrogate/saved_models/knob_cost_models_v1")
            files = sorted(model_dir.glob("model_engine=*_ram=*.joblib"))
            for art_path in files:
                self._register_artifact(joblib.load(art_path), str(art_path))

        self.engine_names = sorted(self.artifacts_by_engine.keys())
        if not self.engine_names:
            raise FileNotFoundError(
                "No model artifacts were found. Provide manifest.joblib, a model_engine=..._ram=....joblib file, "
                "or a directory containing those files."
            )

    @staticmethod
    def _normalize_context_features(context: Dict[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for k, v in (context or {}).items():
            out[str(k)] = CostModelPredictor._to_float_or_zero(v)

        for k, v in (context or {}).items():
            key = str(k)
            val = CostModelPredictor._to_float_or_zero(v)
            if key.startswith("wf_"):
                out[f"collected.workload_features.{key[3:]}"] = val
            elif key.startswith("op_"):
                out[f"collected.workload_features.{key[3:]}"] = val
            elif key.startswith("tbl_"):
                out[f"collected.workload_features.{key[4:]}"] = val
            elif key.startswith("im_"):
                out[f"collected.internal_metrics.{key[3:]}"] = val
        return out

    @staticmethod
    def _make_feature_frame(df_in: pd.DataFrame) -> pd.DataFrame:
        out = df_in.copy()
        log_knobs = [
            "features.shared_buffers",
            "features.work_mem",
            "features.maintenance_work_mem",
            "features.effective_cache_size",
            "features.temp_buffers",
            "features.wal_buffers",
            "features.innodb_buffer_pool_size",
        ]
        for c in log_knobs:
            if c in out.columns:
                out[c + "._log1p"] = np.log1p(np.maximum(pd.to_numeric(out[c], errors="coerce").fillna(0.0), 0.0))

        if "features.work_mem" in out.columns and "features.shared_buffers" in out.columns:
            denom = pd.to_numeric(out["features.shared_buffers"], errors="coerce").replace(0, np.nan)
            out["ratio.work_mem_over_shared_buffers"] = (
                pd.to_numeric(out["features.work_mem"], errors="coerce") / denom
            ).replace([np.inf, -np.inf], np.nan)

        if "features.effective_cache_size" in out.columns and "metadata.hardware_specs.ram_gb" in out.columns:
            denom = pd.to_numeric(out["metadata.hardware_specs.ram_gb"], errors="coerce").replace(0, np.nan)
            out["ratio.effective_cache_size_over_ram_gb"] = (
                pd.to_numeric(out["features.effective_cache_size"], errors="coerce") / denom
            ).replace([np.inf, -np.inf], np.nan)

        if "features.shared_buffers" in out.columns and "metadata.hardware_specs.ram_gb" in out.columns:
            denom = pd.to_numeric(out["metadata.hardware_specs.ram_gb"], errors="coerce").replace(0, np.nan)
            out["ratio.shared_buffers_over_ram_gb"] = (
                pd.to_numeric(out["features.shared_buffers"], errors="coerce") / denom
            ).replace([np.inf, -np.inf], np.nan)

        rw = (
            "collected.workload_features.read_write_ratio"
            if "collected.workload_features.read_write_ratio" in out.columns
            else None
        )
        ts = (
            "collected.workload_features.total_statements"
            if "collected.workload_features.total_statements" in out.columns
            else None
        )
        sb_log = "features.shared_buffers._log1p" if "features.shared_buffers._log1p" in out.columns else None
        wm_log = "features.work_mem._log1p" if "features.work_mem._log1p" in out.columns else None

        if rw and sb_log:
            out["x.rw_ratio_x_shared_buffers"] = pd.to_numeric(out[rw], errors="coerce") * pd.to_numeric(
                out[sb_log], errors="coerce"
            )
        if rw and wm_log:
            out["x.rw_ratio_x_work_mem"] = pd.to_numeric(out[rw], errors="coerce") * pd.to_numeric(
                out[wm_log], errors="coerce"
            )
        if ts and wm_log:
            out["x.total_statements_x_work_mem"] = pd.to_numeric(out[ts], errors="coerce") * pd.to_numeric(
                out[wm_log], errors="coerce"
            )
        if rw and "features.shared_buffers" in out.columns:
            out["x.rw_ratio_x_shared_buffers_raw"] = pd.to_numeric(out[rw], errors="coerce") * pd.to_numeric(
                out["features.shared_buffers"], errors="coerce"
            )

        return out

    def _resolve_engine(self, context: Dict[str, Any]) -> str:
        if self.default_engine and self.default_engine in self.artifacts_by_engine:
            return self.default_engine

        engine_hints = [
            str(context.get("metadata.db_engine", "")).strip().lower(),
            str(context.get("db_engine", "")).strip().lower(),
            str(context.get("engine", "")).strip().lower(),
            str(os.environ.get("DB_ENGINE", "")).strip().lower(),
        ]
        for hint in engine_hints:
            if hint and hint in self.artifacts_by_engine:
                return hint
        return self.engine_names[0]

    def _resolve_ram_bucket(self, ram_models: Dict[str, Dict[str, Any]], context: Dict[str, Any]) -> str:
        if "all" in ram_models:
            return "all"

        ram_candidates = [
            context.get("metadata.hardware_specs.ram_gb"),
            context.get("ram_gb"),
            context.get("hardware_ram_gb"),
            self.default_ram_gb,
            self._detect_system_ram_gb(),
        ]

        ram_val = None
        for c in ram_candidates:
            if c is None:
                continue
            try:
                ram_val = int(round(float(c)))
                break
            except Exception:
                continue

        numeric_buckets = []
        for b in ram_models.keys():
            try:
                numeric_buckets.append((b, int(float(b))))
            except Exception:
                continue

        if ram_val is not None and numeric_buckets:
            return min(numeric_buckets, key=lambda x: abs(x[1] - ram_val))[0]

        return sorted(ram_models.keys())[0]

    def _predict_from_artifact(
        self,
        artifact: Dict[str, Any],
        row: Dict[str, Any],
        workload_key: str,
        selected_model: Optional[str] = None,
    ) -> float:
        df = pd.DataFrame([row])
        df = self._make_feature_frame(df)

        feature_cols = list(artifact.get("feature_cols", []))
        for c in feature_cols:
            if c not in df.columns:
                df[c] = np.nan

        X = df[feature_cols].apply(pd.to_numeric, errors="coerce")

        imputer = artifact.get("imputer")
        if isinstance(imputer, SimpleImputer):
            # Patch for sklearn backward compatibility
            if not hasattr(imputer, "_fit_dtype"):
                imputer._fit_dtype = np.dtype(np.float64)
            Xv = imputer.transform(X)
        else:
            Xv = np.nan_to_num(X.to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        Xv = np.nan_to_num(Xv, nan=0.0, posinf=0.0, neginf=0.0)
        X_df = pd.DataFrame(Xv, columns=feature_cols)

        chosen = selected_model or self.preferred_model

        if chosen == "lgbm_ranker":
            ranker = artifact.get("ranker")
            if ranker is None:
                raise ValueError("Requested lgbm_ranker, but artifact['ranker'] is missing.")

            # Bypass sklearn wrapper if underlying booster is accessible (avoids NotFittedError & versions mismatch)
            b_model = getattr(ranker, "_booster", getattr(ranker, "booster_", getattr(ranker, "_Booster", None)))
            if b_model is not None and hasattr(b_model, "predict"):
                score = float(np.asarray(b_model.predict(X_df), dtype=np.float64).ravel()[0])
            else:
                score = float(np.asarray(ranker.predict(X_df), dtype=np.float64).ravel()[0])
            
            # Convert to a positive optimization objective where lower is better.
            return float(np.exp(-score))

        models = artifact.get("models", {})
        candidates = []
        if chosen in models:
            candidates.append(models[chosen])
        if self.preferred_model in models and models[self.preferred_model] not in candidates:
            candidates.append(models[self.preferred_model])
        if "hgb" in models and models["hgb"] not in candidates:
            candidates.append(models["hgb"])
        if "rf" in models and models["rf"] not in candidates:
            candidates.append(models["rf"])
        for k, m in models.items():
            if m not in candidates:
                candidates.append(m)

        if not candidates:
             raise ValueError("Artifact has no regression models under 'models'.")

        pred_log = None
        for model in candidates:
            b_model = getattr(model, "_booster", getattr(model, "booster_", getattr(model, "_Booster", None)))
            if b_model is not None and hasattr(b_model, "predict"):
                try:
                    pred_log = float(np.asarray(b_model.predict(X_df), dtype=np.float64).ravel()[0])
                    break
                except Exception:
                    pass
            try:
                pred_log = float(np.asarray(model.predict(X_df), dtype=np.float64).ravel()[0])
                break
            except Exception:
                continue
                
        if pred_log is None:
            raise RuntimeError("All models failed during prediction. Could not predict.")
        # For tuning we care about ranking configurations; pred_norm is sufficient.
        pred_norm = float(np.expm1(pred_log))
        return float(max(pred_norm, 1e-9))

    def predict(self, knob_values: Dict[str, Any], context: Dict[str, Any], workload_key: str) -> float:
        context = self._normalize_context_features(context)
        engine = self._resolve_engine(context)
        ram_models = self.artifacts_by_engine[engine]
        ram_bucket = self._resolve_ram_bucket(ram_models, context)
        artifact = ram_models[ram_bucket]
        selected_model = self.best_model_by_bucket.get((engine, ram_bucket), self.preferred_model)

        row: Dict[str, Any] = dict(context)
        row.setdefault("metadata.db_engine", engine)
        if "metadata.hardware_specs.ram_gb" not in row and self.default_ram_gb is not None:
            row["metadata.hardware_specs.ram_gb"] = float(self.default_ram_gb)
        row["metadata.workload_key"] = str(workload_key)

        for k, v in knob_values.items():
            row[f"features.{k}"] = self._to_float_or_zero(v)

        return self._predict_from_artifact(
            artifact,
            row,
            workload_key=str(workload_key),
            selected_model=selected_model,
        )

class CostModel(WorkloadRunner):
    """
    The Cost Model (Surrogate Model) for E2ETune.
    Responsible for predicting database performance (TPS/Latency) given 
    knob configurations and workload features.
    It implements the WorkloadRunner interface to act as a physical Database drop-in replacement.
    """
    
    def __init__(self, knob_settings: KnobSettingsSet):
        self.knob_settings = knob_settings
        self.predictor: Optional[CostModelPredictor] = None
        self.workload_features: Dict[str, Dict[str, Any]] = {}
        self.is_trained = False

    def load_model(self, path: str):
        """Load notebook-trained joblib artifacts (manifest/joblib/dir)."""
        self.predictor = CostModelPredictor(model_path=path)
        self.is_trained = True

    def run_workload(self, workload_task: BenchmarkTask, runs_per_iteration: int = 1) -> tuple[float, float]:
        """
        Run the workload via the Cost Model to return a performance tuple.
        Args:
            workload_task: The BenchmarkTask containing all necessary information to run the workload.
            runs_per_iteration: The number of times to run the workload (unused for Cost Model, for interface compatibility).
        Returns:
            A tuple containing the [latency, -throughput] performance metrics.
        """
        # 1. Map workload_path to workload_id to get its context features (im_*, wf_*, etc.)
        if self.predictor is None:
            raise RuntimeError("CostModel predictor is not loaded. Call load_model(path) first.")

        workload_id = str(workload_task.workload_path.stem)
        
        # We assume workload_features mapping might contain dictionaries for each workload
        workload_context_features = self.workload_features.get(workload_id, {})
        
        # If it's empty, we might just pass an empty dict and let preprocessing fill 0s
        if not isinstance(workload_context_features, dict):
            # Fallback if old format was a list of floats, or something else
            workload_context_features = {}

        # 2. Extract raw knob values; predictor maps them to features.<knob>
        knob_values = workload_task.knob_config.to_dict()
        predicted_cost = self.predictor.predict(
            knob_values=knob_values,
            context=workload_context_features,
            workload_key=workload_id,
        )
        
        # 5. Return tuple (latency, -throughput)
        # Cost is directly modeling elapsed time (latency)
        latency = float(predicted_cost)
        throughput = (1.0 / latency) if latency > 0 else 0
        
        return latency, -throughput
