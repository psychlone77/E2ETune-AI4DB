import json
import joblib
import os
import pandas as pd
import numpy as np
import sys
import re
import ast
from dataclasses import asdict
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
        model_path: str = "surrogate/artifacts/transfer_rank_surrogate",
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
        self._plan_embedder = None
        self._plan_embedder_name = "sentence-transformers/paraphrase-MiniLM-L3-v2"
        self._plan_embed_max_chars = 4000
        self._load_artifacts(model_path)

    @staticmethod
    def _canonical_engine_name(engine: Optional[str]) -> Optional[str]:
        if not engine:
            return None
        e = str(engine).strip().lower()
        if e in {"pg", "postgres", "postgresql", "postgre"}:
            return "postgresql"
        if e in {"mysql", "mariadb"}:
            return "mysql"
        return e

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

    def _register_notebook_v2_artifact(self, bundle: Dict[str, Any], source_name: str) -> None:
        """Register new notebook artifacts saved by transfer_rank_surrogate_improved.ipynb.

        Expected bundle shapes:
        - PG: {'model_type': 'pg_regressor', 'model': <LGBMRegressor>, 'feature_names': [...], ...}
        - MySQL: {'model_type': 'mysql_workload_standardized_regressor', 'model': <LGBMRegressor>,
                  'mu_by_wk': {...}, 'sigma_by_wk': {...}, 'fallback_mu': float, 'fallback_sigma': float, ...}
        """
        model_type = str(bundle.get("model_type", "")).strip().lower()
        if not model_type:
            raise ValueError(f"Missing 'model_type' in notebook artifact loaded from {source_name}")

        if model_type == "pg_regressor":
            engine = "postgresql"
        elif model_type == "mysql_workload_standardized_regressor":
            engine = "mysql"
        else:
            raise ValueError(f"Unsupported notebook artifact model_type={model_type!r} from {source_name}")

        feature_cols = bundle.get("feature_names") or bundle.get("feature_cols")
        if not isinstance(feature_cols, list) or not feature_cols:
            raise ValueError(f"Notebook artifact from {source_name} missing non-empty feature list")

        artifact: Dict[str, Any] = {
            "schema": "notebook_v2",
            "engine": engine,
            "ram_bucket": "all",
            "feature_cols": list(feature_cols),
            "bundle": bundle,
        }

        # Load preprocessing metadata saved by the notebook (if present next to the artifact).
        try:
            meta_path = Path(source_name).resolve().parent / "feature_meta.joblib"
            if meta_path.exists():
                feature_meta = joblib.load(meta_path)
                if isinstance(feature_meta, dict) and feature_meta.get("feature_names"):
                    artifact["feature_meta"] = feature_meta
        except Exception:
            # Metadata is optional; inference will fall back to numeric coercion and 0-filling.
            pass

        self._register_artifact(artifact, source_name)

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if v is None:
                return default
            out = float(v)
            if not np.isfinite(out):
                return default
            return out
        except Exception:
            return default

    @staticmethod
    def _apply_notebook_v2_preprocessing(df: pd.DataFrame, feature_meta: Dict[str, Any]) -> pd.DataFrame:
        """Replicate notebook preprocessing for the saved v2 artifacts.

        The notebook saves the exact columns it used:
        - knob_cols: list of raw knob feature columns (e.g., features.shared_buffers)
        - log_knob_cols: subset of knob_cols that were log1p-transformed IN-PLACE
        - knob_minmax: per-column min/max used for min-max scaling IN-PLACE

        This function mutates the knob columns in df to match that pipeline.
        """
        knob_cols = feature_meta.get("knob_cols") or []
        if not isinstance(knob_cols, list) or not knob_cols:
            return df

        log_knob_cols = set(feature_meta.get("log_knob_cols") or [])
        knob_minmax = feature_meta.get("knob_minmax") or {}
        if not isinstance(knob_minmax, dict):
            knob_minmax = {}

        # Work on a small aligned frame, then write back.
        kdf = df.reindex(columns=knob_cols).apply(pd.to_numeric, errors="coerce").fillna(0.0)

        # In-place log1p for specified knob columns.
        for c in log_knob_cols:
            if c in kdf.columns:
                kdf[c] = np.log1p(np.maximum(kdf[c].astype(float), 0.0))

        # In-place min-max scaling for knob columns.
        for c in knob_cols:
            mm = knob_minmax.get(c)
            if not isinstance(mm, dict):
                continue
            mn = CostModelPredictor._safe_float(mm.get("min"), default=0.0)
            mx = CostModelPredictor._safe_float(mm.get("max"), default=0.0)
            denom = mx - mn
            if denom <= 0 or not np.isfinite(denom):
                kdf[c] = 0.0
                continue
            kdf[c] = ((kdf[c].astype(float) - mn) / denom).clip(0.0, 1.0)

        # Write back the transformed knob columns.
        for c in knob_cols:
            df[c] = kdf[c]
        return df

    def _register_artifact(self, artifact: Dict[str, Any], source_name: str) -> None:
        engine = self._canonical_engine_name(artifact.get("engine")) or ""
        ram_bucket = str(artifact.get("ram_bucket", "all")).strip().lower()
        if not engine:
            raise ValueError(f"Missing 'engine' in artifact loaded from {source_name}")

        if engine not in self.artifacts_by_engine:
            self.artifacts_by_engine[engine] = {}
        self.artifacts_by_engine[engine][ram_bucket] = artifact

    def _load_artifacts(self, model_path: str) -> None:
        p = Path(model_path)

        def _maybe_pick_latest_notebook_dir(base: Path) -> Optional[Path]:
            if not base.exists() or not base.is_dir():
                return None

            # The legacy directory may also contain a pg_regressor.joblib with a different schema.
            # Use the presence of MySQL workload-standardized artifact (or meta JSON) as the
            # discriminator for the new notebook-v2 bundle directory.
            def is_notebook_v2_dir(d: Path) -> bool:
                return (d / "mysql_workload_standardized_regressor.joblib").exists() or (d / "saved_models_meta.json").exists()

            candidates: list[Path] = []
            if is_notebook_v2_dir(base):
                candidates.append(base)
            for d in base.iterdir():
                if d.is_dir() and is_notebook_v2_dir(d):
                    candidates.append(d)
            if not candidates:
                return None
            return max(candidates, key=lambda x: x.stat().st_mtime)

        # Prefer new notebook v2 artifacts if present.
        if p.is_dir():
            picked = _maybe_pick_latest_notebook_dir(p)
            if picked is not None:
                for fname in ["pg_regressor.joblib", "mysql_workload_standardized_regressor.joblib"]:
                    f = picked / fname
                    if f.exists():
                        obj = joblib.load(f)
                        if isinstance(obj, dict) and obj.get("model_type"):
                            self._register_notebook_v2_artifact(obj, str(f))

                # If we found at least one notebook-v2 artifact, we're done.
                if self.artifacts_by_engine:
                    self.engine_names = sorted(self.artifacts_by_engine.keys())
                    return

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
            # New notebook-v2 single artifact.
            if isinstance(obj, dict) and obj.get("model_type"):
                self._register_notebook_v2_artifact(obj, str(p))
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
                # If it wasn't notebook-v2 and wasn't the old supported formats, error.
                if not (isinstance(obj, dict) and obj.get("model_type")):
                    raise ValueError(
                        f"Unsupported model file format: {p}. Expected notebook-v2 artifact, manifest.joblib, "
                        "or legacy knob cost model joblib files."
                    )
        else:
            model_dir = p if p.exists() else Path("surrogate/artifacts/transfer_rank_surrogate")
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
    def _parse_plan_features(plan_list: list) -> Dict[str, float]:
        import re
        OP_ALIASES = {
            'seq scan': 'table_scan', 'table scan': 'table_scan',
            'index scan': 'index_scan', 'index only scan': 'index_scan',
            'hash join': 'join', 'merge join': 'join', 'nested loop': 'join', 'join': 'join',
            'sort': 'sort', 'aggregate': 'aggregate',
            'limit': 'limit', 'gather': 'gather', 'gather merge': 'gather',
        }
        def normalize_op(op: str) -> str:
            s = re.sub(r'\s+', ' ', (op or '').strip().lower())
            return OP_ALIASES.get(s, s)

        feats: Dict[str, float] = {}
        if not plan_list:
            feats['plan.parse_failed'] = 1.0
            return feats

        ops = []
        costs = []
        rows = []
        any_index = join_hash = join_merge = join_nested = 0

        for p in plan_list:
            if not isinstance(p, str) or not p.strip():
                continue
            for raw in re.findall(r'([A-Za-z][A-Za-z ]+?)\(', p):
                norm = normalize_op(raw)
                ops.append(norm)
                rl = raw.lower()
                join_hash   += int('hash join'   in rl)
                join_merge  += int('merge join'  in rl)
                join_nested += int('nested loop' in rl)
                any_index    = any_index or int('index' in rl)
            for m in re.finditer(r'cost=([0-9]+(?:\.[0-9]+)?)', p):
                costs.append(float(m.group(1)))
            for m in re.finditer(r'rows=([0-9]+(?:\.[0-9]+)?)', p):
                rows.append(float(m.group(1)))

        if not ops:
            feats['plan.parse_failed'] = 1.0
            return feats

        total = float(len(ops))
        feats['plan.parse_failed']     = 0.0
        feats['plan.num_plans']        = float(len(plan_list))
        feats['plan.total_nodes']      = total
        feats['plan.any_index']        = float(any_index)
        feats['plan.join_hash_prop']   = float(join_hash)   / total
        feats['plan.join_merge_prop']  = float(join_merge)  / total
        feats['plan.join_nested_prop'] = float(join_nested) / total
        feats['plan.avg_cost']         = float(np.mean(costs)) if costs else 0.0
        feats['plan.max_cost']         = float(np.max(costs))  if costs else 0.0
        feats['plan.avg_rows']         = float(np.mean(rows))  if rows  else 0.0
        feats['plan.max_rows']         = float(np.max(rows))   if rows  else 0.0

        counts = {}
        for op in ops:
            counts[op] = counts.get(op, 0) + 1
        for op, cnt in counts.items():
            feats[f'plan.op_prop.{op}'] = float(cnt) / total

        return feats

    @staticmethod
    def _parse_embedding_vector(value: Any) -> Optional[list[float]]:
        """Parse a stored embedding vector from JSON/list into a list[float]."""
        if value is None:
            return None
        if isinstance(value, float) and np.isnan(value):
            return None

        if isinstance(value, (list, tuple, np.ndarray)):
            try:
                parsed = [float(x) for x in value]
                if not parsed:
                    return None
                arr = np.asarray(parsed, dtype=np.float32)
                if not np.isfinite(arr).all():
                    return None
                return parsed
            except Exception:
                return None

        text = str(value).strip()
        if not text:
            return None

        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
                if isinstance(parsed, (list, tuple)):
                    as_float = [float(x) for x in parsed]
                    if not as_float:
                        return None
                    arr = np.asarray(as_float, dtype=np.float32)
                    if not np.isfinite(arr).all():
                        return None
                    return as_float
            except Exception:
                continue
        return None

    @staticmethod
    def _embedding_index_from_col(col: str) -> Optional[int]:
        """Return embedding dim index for known embedding column naming schemes."""
        s = str(col)
        for pat in (
            r"(?:^|\.)qp_emb_(\d{3})$",
            r"(?:^|\.)plan_emb_(\d{3})$",
        ):
            m = re.search(pat, s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
        return None

    def _inject_query_plan_embeddings(
        self,
        row: Dict[str, Any],
        raw_context: Dict[str, Any],
        artifact: Dict[str, Any],
    ) -> None:
        """Expand qp_emb_vector into numeric embedding feature columns if needed.

        We do not compute embeddings at runtime; we only consume already-computed
        vectors (e.g. written into collected_data.json as qp_emb_vector).
        """
        feature_cols = artifact.get("feature_cols")
        if not isinstance(feature_cols, list) or not feature_cols:
            return

        needed_cols: list[tuple[str, int]] = []
        for c in feature_cols:
            idx = self._embedding_index_from_col(c)
            if idx is not None:
                needed_cols.append((str(c), idx))
        if not needed_cols:
            return

        vec_raw = (
            raw_context.get("collected.qp_emb_vector")
            or raw_context.get("qp_emb_vector")
            or raw_context.get("collected.qp_emb")
        )
        vec = self._parse_embedding_vector(vec_raw)

        # If the model expects embeddings but we weren't given qp_emb_vector, compute
        # it from query_plans in the same style as add_query_plan_embeddings.py.
        if vec is None:
            qp = raw_context.get("collected.query_plans") or raw_context.get("query_plans")
            if qp is not None:
                if isinstance(qp, str):
                    try:
                        qp = json.loads(qp)
                    except Exception:
                        try:
                            qp = ast.literal_eval(qp)
                        except Exception:
                            qp = None
            if isinstance(qp, list) and qp:
                try:
                    vec = self._embed_query_plans(qp)
                except Exception:
                    vec = None
        if vec is None:
            return

        for col, idx in needed_cols:
            if 0 <= idx < len(vec):
                row[col] = float(vec[idx])
            else:
                row[col] = 0.0

    def _embed_query_plans(self, plans: list) -> Optional[list[float]]:
        """Generate a normalized SentenceTransformer embedding for a list of plans."""
        try:
            from sentence_transformers import SentenceTransformer
        except Exception:
            return None

        # Match add_query_plan_embeddings.py text formatting.
        joined = " [SEP] ".join(str(p).strip() for p in plans if p and str(p).strip())
        if len(joined) > self._plan_embed_max_chars:
            joined = joined[: self._plan_embed_max_chars]

        if self._plan_embedder is None:
            self._plan_embedder = SentenceTransformer(self._plan_embedder_name, device="cpu")

        vec = self._plan_embedder.encode(
            [joined],
            batch_size=1,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)
        out = vec.ravel().tolist()
        if not out:
            return None
        if not np.isfinite(np.asarray(out, dtype=np.float32)).all():
            return None
        return [float(x) for x in out]

    @staticmethod
    def _normalize_context_features(context: Dict[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}

        qp = context.get("collected.query_plans") or context.get("query_plans")
        qp_emb = context.get("collected.qp_emb_vector") or context.get("qp_emb_vector")
        if qp:
            if isinstance(qp, str):
                try:
                    qp = json.loads(qp)
                except Exception:
                    try:
                        qp = ast.literal_eval(qp)
                    except Exception:
                        pass
            if isinstance(qp, list):
                plan_feats = CostModelPredictor._parse_plan_features(qp)
                out.update(plan_feats)
        elif qp_emb:
            # If embeddings exist, do not treat plan parsing as failed.
            out["plan.parse_failed"] = 0.0
        else:
            out["plan.parse_failed"] = 1.0

        for k, v in (context or {}).items():
            if k in ("collected.query_plans", "query_plans"):
                continue
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
        if self.default_engine:
            canon = self._canonical_engine_name(self.default_engine)
            if canon and canon in self.artifacts_by_engine:
                return canon

        engine_hints = [
            context.get("metadata.db_engine"),
            context.get("db_engine"),
            context.get("engine"),
            os.environ.get("DB_ENGINE"),
        ]
        for hint in engine_hints:
            canon = self._canonical_engine_name(hint)
            if canon and canon in self.artifacts_by_engine:
                return canon
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
        # New notebook-v2 artifacts (transfer_rank_surrogate_improved.ipynb)
        if artifact.get("schema") == "notebook_v2":
            bundle = artifact.get("bundle", {})
            model_type = str(bundle.get("model_type", "")).strip().lower()
            model = bundle.get("model")
            if model is None:
                raise ValueError("Notebook-v2 artifact missing 'model'.")

            df = pd.DataFrame([row])
            df = self._make_feature_frame(df)

            # Apply the exact preprocessing used in the notebook training pipeline, if available.
            feature_meta = artifact.get("feature_meta")
            if isinstance(feature_meta, dict):
                df = self._apply_notebook_v2_preprocessing(df, feature_meta)

            feature_cols = list(artifact.get("feature_cols", []))
            # Reindexing adds any missing columns as NaN in one shot (avoids fragmentation).
            X = df.reindex(columns=feature_cols).apply(pd.to_numeric, errors="coerce").fillna(0.0)

            # LightGBM/sklearn wrappers accept DataFrame.
            pred = float(np.asarray(model.predict(X), dtype=np.float64).ravel()[0])

            if model_type == "pg_regressor":
                # Model predicts log1p(latency_like)
                pred_log = float(np.clip(pred, -20, 20))
                y_hat = float(np.expm1(pred_log))
                return float(max(y_hat, 1e-9))

            if model_type == "mysql_workload_standardized_regressor":
                # Model predicts standardized log residual: z = (y_log - mu_wk) / sigma_wk
                mu_by_wk = bundle.get("mu_by_wk", {}) or {}
                sigma_by_wk = bundle.get("sigma_by_wk", {}) or {}
                fallback_mu = float(bundle.get("fallback_mu", 0.0))
                fallback_sigma = float(bundle.get("fallback_sigma", 1.0))

                mu_raw = mu_by_wk.get(str(workload_key))
                sigma_raw = sigma_by_wk.get(str(workload_key))
                try:
                    mu = float(mu_raw)
                except Exception:
                    mu = fallback_mu
                try:
                    sigma = float(sigma_raw)
                except Exception:
                    sigma = fallback_sigma
                if sigma <= 0:
                    sigma = fallback_sigma
                sigma = max(float(sigma), 1e-6)

                pred_log = float(np.clip(pred * sigma + mu, -20, 20))
                y_hat = float(np.expm1(pred_log))
                return float(max(y_hat, 1e-9))

            raise ValueError(f"Unsupported notebook-v2 model_type={model_type!r}")

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
        raw_context = context or {}
        context = self._normalize_context_features(raw_context)
        engine = self._resolve_engine(context)
        ram_models = self.artifacts_by_engine[engine]
        ram_bucket = self._resolve_ram_bucket(ram_models, context)
        artifact = ram_models[ram_bucket]
        selected_model = self.best_model_by_bucket.get((engine, ram_bucket), self.preferred_model)

        row: Dict[str, Any] = dict(context)
        row.setdefault("metadata.db_engine", engine)

        # Match notebook training: explicit domain flag.
        row["db_type"] = 1.0 if engine == "mysql" else 0.0

        if "metadata.hardware_specs.ram_gb" not in row and self.default_ram_gb is not None:
            row["metadata.hardware_specs.ram_gb"] = float(self.default_ram_gb)
        row["metadata.workload_key"] = str(workload_key)

        for k, v in knob_values.items():
            row[f"features.{k}"] = self._to_float_or_zero(v)

        # If the selected model expects embedding columns, expand qp_emb_vector here.
        self._inject_query_plan_embeddings(row=row, raw_context=raw_context, artifact=artifact)

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
        self._collected_context_cache: Dict[str, Dict[str, Any]] = {}
        self.is_trained = False

    def load_model(self, path: str):
        """Load notebook-trained joblib artifacts (manifest/joblib/dir).

        Pass the directory that contains:
        - pg_regressor.joblib
        - mysql_workload_standardized_regressor.joblib
        (e.g. surrogate/artifacts/transfer_rank_surrogate/6ecfae00)
        """
        self.predictor = CostModelPredictor(model_path=path)
        self.is_trained = True

    @staticmethod
    def _parse_hardware_specs(hardware_name: str) -> Dict[str, Any]:
        parts = str(hardware_name).split("-")
        cores = threads = ram_gb = None
        for p in parts:
            low = p.lower().strip()
            if low.endswith("c") and low[:-1].isdigit():
                cores = int(low[:-1])
            elif low.endswith("t") and low[:-1].isdigit():
                threads = int(low[:-1])
            elif low.endswith("gb") and low[:-2].isdigit():
                ram_gb = int(low[:-2])
        return {
            "metadata.hardware_specs.cores": cores,
            "metadata.hardware_specs.threads": threads,
            "metadata.hardware_specs.ram_gb": ram_gb,
        }

    def _load_collected_context(self, workload_id: str) -> Dict[str, Any]:
        """Load and normalize collected_data.json for a workload_id.

        Returns a dict that can be passed directly as predictor context.
        """
        if workload_id in self._collected_context_cache:
            return dict(self._collected_context_cache[workload_id])

        ctx: Dict[str, Any] = {}
        data_root = Path("data")
        candidates = list(data_root.glob(f"*/*/*/{workload_id}/collected_data.json"))
        if not candidates:
            self._collected_context_cache[workload_id] = {}
            return {}

        picked = max(candidates, key=lambda p: p.stat().st_mtime)
        try:
            with open(picked, "r", encoding="utf-8") as f:
                cdata = json.load(f)
        except Exception:
            self._collected_context_cache[workload_id] = {}
            return {}

        # Derive metadata to match training keys.
        rel = picked.parent.as_posix()
        if rel.startswith("./"):
            rel = rel[2:]
        # Expected: data/<engine>/<hardware>/<benchmark>/<workload>
        parts = picked.parts
        try:
            i = parts.index("data")
            engine = parts[i + 1]
            hardware = parts[i + 2]
            benchmark = parts[i + 3]
            workload = parts[i + 4]
        except Exception:
            engine = hardware = benchmark = workload = None

        if engine:
            ctx["metadata.db_engine"] = engine
        if hardware:
            ctx["metadata.hardware"] = hardware
            ctx.update(self._parse_hardware_specs(hardware))
        if benchmark:
            ctx["metadata.benchmark"] = benchmark
        if workload:
            ctx["metadata.workload"] = workload
        # This is what the notebook used for mu/sigma lookup in MySQL standardized model.
        ctx["metadata.workload_key"] = picked.parent.as_posix()

        # Internal metrics -> im_* (as expected by CostModelPredictor._normalize_context_features)
        internal_metrics = cdata.get("internal_metrics", {}) or {}
        if hasattr(internal_metrics, "__dict__") and not isinstance(internal_metrics, dict):
            try:
                internal_metrics = asdict(internal_metrics)
            except Exception:
                internal_metrics = {}
        if isinstance(internal_metrics, dict):
            for k, v in internal_metrics.items():
                try:
                    ctx[f"im_{k}"] = float(v) if v is not None else 0.0
                except Exception:
                    ctx[f"im_{k}"] = 0.0

        # Workload features -> wf_ / op_ / tbl_
        wfe = cdata.get("workload_features", {}) or {}
        if isinstance(wfe, dict):
            for k, v in wfe.items():
                if isinstance(v, dict):
                    prefix = "op_" if "operator" in str(k).lower() else "tbl_"
                    for sub_k, sub_v in v.items():
                        formatted_k = (
                            str(sub_k).lower().replace(" ", "_") if prefix == "op_" else str(sub_k)
                        )
                        try:
                            ctx[f"{prefix}{formatted_k}"] = float(sub_v) if sub_v is not None else 0.0
                        except Exception:
                            ctx[f"{prefix}{formatted_k}"] = 0.0
                else:
                    try:
                        ctx[f"wf_{k}"] = float(v) if v is not None else 0.0
                    except Exception:
                        ctx[f"wf_{k}"] = 0.0

        # Query plans + precomputed embeddings (if present).
        qp = cdata.get("query_plans")
        if qp is not None:
            ctx["query_plans"] = qp
            ctx["collected.query_plans"] = qp
        qp_emb = cdata.get("qp_emb_vector")
        if qp_emb is not None:
            ctx["qp_emb_vector"] = qp_emb
            ctx["collected.qp_emb_vector"] = qp_emb

        self._collected_context_cache[workload_id] = dict(ctx)
        return ctx

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

        # Augment with collected_data.json context when available (plans/embeddings/metadata).
        collected_ctx = self._load_collected_context(workload_id)
        if collected_ctx:
            merged = dict(workload_context_features)
            # Prefer explicit keys from collected_data.json (plans + metadata) when present.
            merged.update(collected_ctx)
            workload_context_features = merged

        # 2. Extract raw knob values; predictor maps them to features.<knob>
        knob_values = workload_task.knob_config.to_dict()
        # Use the notebook training key for MySQL standardized inference if we have it.
        wk_key = workload_context_features.get("metadata.workload_key") or workload_id

        predicted_cost = self.predictor.predict(
            knob_values=knob_values,
            context=workload_context_features,
            workload_key=str(wk_key),
        )
        
        # 5. Return tuple (latency, -throughput)
        # Cost is directly modeling elapsed time (latency)
        latency = float(predicted_cost)
        throughput = (1.0 / latency) if latency > 0 else 0
        
        return latency, -throughput
