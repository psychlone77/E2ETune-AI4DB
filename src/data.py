import torch
import torch.nn as nn
from torch.autograd import Function
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader, WeightedRandomSampler
from sklearn.preprocessing import StandardScaler, RobustScaler, LabelEncoder
from pathlib import Path
import warnings
import json
import os
import matplotlib.pyplot as plt
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error

# Columns to log-scale (large numeric values)
# Columns to log-scale (large, positive-skew numeric values)
_LOG_SCALE_CANDIDATES = [
    # Memory / buffer sizes (both legacy + underscore variants)
    "buffer_pool_size", "cache_effective_size", "cache_effective_size_",
    "per_query_memory", "maintenance_memory", "temp_memory",
    "log_buffer_size", "log_capacity", "temp_file_limit_",
    
    # Resource / capacity knobs that can span wide ranges
    "max_connections_", "io_parallelism",
    
    # Workload counters
    "blks_hit", "blks_read",
    "disk_read_bytes", "disk_read_count",
    "disk_write_bytes", "disk_write_count",
    "tup_fetched", "tup_returned", "tup_inserted", "tup_updated", "tup_deleted",
    "xact_commit", "xact_rollback",
    "conflicts",
    
    # Optional timing/size-like knobs (keep if present)
    "commit_delay_",
    "vacuum_cost_limit_", "vacuum_cost_page_dirty_", "vacuum_cost_page_hit_", "vacuum_cost_page_miss_",
]

LABEL_COL = "cost"
DOMAIN_COL = "domain_id"
QP_EMB_COL = "qp_emb_vector"
SOURCE_DOMAIN = 3
META_COLS = ["db_engine", "hardware", "ram_gb"]


def get_log_scale_cols(df):
    return [c for c in _LOG_SCALE_CANDIDATES if c in df.columns]


def get_mask_cols(df):
    return [c for c in df.columns if c.startswith("mask_")]


def get_feature_cols(df, mask_cols):
    exclude = set(mask_cols + [LABEL_COL, DOMAIN_COL, QP_EMB_COL] + META_COLS)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    return [c for c in numeric_cols if c not in exclude]

def expand_qp_emb(df, col=None):
    """Expand qp_emb_vector list column into per-dimension float columns"""
    if col is None:
        col = QP_EMB_COL
    emb_matrix = np.vstack(df[col].values)
    qp_cols = [f"qp_emb_{i}" for i in range(emb_matrix.shape[1])]
    emb_df = pd.DataFrame(emb_matrix, columns=qp_cols, index=df.index)
    return emb_df, qp_cols


def preprocess(da, normalization_mode="per_engine", log_target=False):
    """
    Complete preprocessing pipeline:
    1. Convert embeddings from JSON strings
    2. Log-scale large numeric features
    3. Fill NaN values
    4. Filter zero-variance features
    5. Expand query plan embeddings
    6. Fit categorical encoders and feature scaler
    7. NORMALIZE COSTS with selected mode: per_engine or global
    
    Returns: processed df, feature columns, encoders, scaler, and normalization params
    """
    df = da.copy()

    # Convert qp_emb_vector from JSON strings if needed
    if df[QP_EMB_COL].dtype == object and isinstance(df[QP_EMB_COL].iloc[0], str):
        df[QP_EMB_COL] = df[QP_EMB_COL].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        print("INFO: Converted qp_emb_vector from JSON strings")

    log_scale_cols = get_log_scale_cols(df)
    if log_scale_cols:
        print(f"INFO: Log-scaling {len(log_scale_cols)} feature columns")
    # Log-scale large numeric features
    for col in log_scale_cols:
        if col in df.columns:
            df[col] = np.log1p(df[col].clip(lower=0))

    mask_cols = get_mask_cols(df)
    feat_cols = get_feature_cols(df, mask_cols)

    # Replace non-finite feature/mask values, then fill NaNs
    df[feat_cols] = df[feat_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df[mask_cols] = df[mask_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Filter zero-variance features
    feat_cols_filtered = [c for c in feat_cols if df[c].var() > 1e-10]
    dropped_zero_var = [c for c in feat_cols if c not in feat_cols_filtered]
    if dropped_zero_var:
        print(f"WARN: Dropped {len(dropped_zero_var)} zero-variance features: {dropped_zero_var[:5]}")

    # Clip extreme values to prevent scaling instability
    for col in feat_cols_filtered:
        df[col] = np.clip(df[col], -1e4, 1e4)

    # Expand query plan embeddings and concatenate
    emb_df, qp_cols = expand_qp_emb(df)
    emb_df = emb_df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df = pd.concat([df.reset_index(drop=True), emb_df.reset_index(drop=True)], axis=1)
    print(f"INFO: Expanded {len(qp_cols)} query plan embedding dimensions")

    # Clean cost column BEFORE computing normalization stats
    df[LABEL_COL] = pd.to_numeric(df[LABEL_COL], errors="coerce")
    bad_cost_mask = ~np.isfinite(df[LABEL_COL].to_numpy())
    bad_cost_count = int(bad_cost_mask.sum())
    if bad_cost_count > 0:
        df = df.loc[~bad_cost_mask].reset_index(drop=True)
        print(f"WARN: Dropped {bad_cost_count} rows with non-finite cost values before normalization")

    if len(df) == 0:
        raise ValueError("No rows left after removing non-finite cost values.")

    # Fit categorical encoders
    db_enc = LabelEncoder().fit(df["db_engine"])
    hw_enc = LabelEncoder().fit(df["hardware"])
    print(f"INFO: Fitted encoders: {len(db_enc.classes_)} DB engines, {len(hw_enc.classes_)} hardware configs")

    # Fit feature scaler on ALL domains (no target leakage since we only fit on features)
    fit_data = df[feat_cols_filtered].values
    print(f"INFO: Fitting RobustScaler on all {len(fit_data)} samples")
    feat_scaler = RobustScaler(quantile_range=(5.0, 95.0)).fit(fit_data)
    
    # Save the true raw cost prior to transform/normalization for MAPE/RMSE logging
    df["raw_cost"] = df[LABEL_COL].values

    # Optional target transform before normalization
    target_transform = {"type": "none"}
    if log_target:
        df[LABEL_COL] = np.log1p(np.maximum(df[LABEL_COL].values, 0.0))
        target_transform = {"type": "log1p"}

    # Cost normalization setup (finite-safe)
    global_mu = float(df[LABEL_COL].mean())
    global_sigma = float(df[LABEL_COL].std())
    if (not np.isfinite(global_mu)) or (not np.isfinite(global_sigma)) or (global_sigma < 1e-8):
        finite_cost = df[LABEL_COL].to_numpy()
        finite_cost = finite_cost[np.isfinite(finite_cost)]
        if finite_cost.size == 0:
            raise ValueError("Could not compute finite cost normalization stats.")
        global_mu = float(np.mean(finite_cost))
        global_sigma = float(np.std(finite_cost, ddof=1)) if finite_cost.size > 1 else 1.0
        if (not np.isfinite(global_sigma)) or (global_sigma < 1e-8):
            global_sigma = 1.0

    normalization_mode = normalization_mode.lower().strip()
    if normalization_mode == "global":
        cost_mu_map = {"__global__": global_mu}
        cost_sigma_map = {"__global__": global_sigma}
        print("\nINFO: Cost normalization mode: GLOBAL")
        print(f"   mu={global_mu:.4f}, sigma={global_sigma:.4f}")
    elif normalization_mode == "per_engine":
        stats_by_engine = df.groupby("db_engine")[LABEL_COL].agg(["mean", "std"])
        stats_by_engine["mean"] = stats_by_engine["mean"].replace([np.inf, -np.inf], np.nan).fillna(global_mu)
        stats_by_engine["std"] = stats_by_engine["std"].replace([np.inf, -np.inf], np.nan).fillna(global_sigma).clip(lower=1e-8)
        cost_mu_map = {k: float(v) for k, v in stats_by_engine["mean"].to_dict().items()}
        cost_sigma_map = {k: float(v) for k, v in stats_by_engine["std"].to_dict().items()}

        print("\nINFO: Cost normalization mode: PER_ENGINE")
        for engine in sorted(cost_mu_map.keys()):
            cnt = int((df["db_engine"] == engine).sum())
            print(f"   {engine}: n={cnt}, mu={cost_mu_map[engine]:.4f}, sigma={cost_sigma_map[engine]:.4f}")
    else:
        raise ValueError("normalization_mode must be 'global' or 'per_engine'")

    # Apply normalization
    if "__global__" in cost_mu_map:
        mu_series = pd.Series(cost_mu_map["__global__"], index=df.index)
        sigma_series = pd.Series(cost_sigma_map["__global__"], index=df.index).clip(lower=1e-8)
    else:
        mu_series = df["db_engine"].map(cost_mu_map).fillna(global_mu)
        sigma_series = df["db_engine"].map(cost_sigma_map).fillna(global_sigma).clip(lower=1e-8)
    df[LABEL_COL] = (df[LABEL_COL] - mu_series) / sigma_series

    # Final guard: remove any non-finite normalized targets
    bad_norm_mask = ~np.isfinite(df[LABEL_COL].to_numpy())
    bad_norm_count = int(bad_norm_mask.sum())
    if bad_norm_count > 0:
        df = df.loc[~bad_norm_mask].reset_index(drop=True)
        print(f"WARN: Dropped {bad_norm_count} rows with non-finite normalized costs")

    print(f"   Normalized overall: mu={df[LABEL_COL].mean():.4f}, sigma={df[LABEL_COL].std():.4f}")
    print(f"   Range: [{df[LABEL_COL].min():.4f}, {df[LABEL_COL].max():.4f}]")

    # Verification: normalized stats by engine family and domain groups
    mysql_mask = df[DOMAIN_COL].isin([0, 1])
    postgres_mask = df[DOMAIN_COL].isin([2, 3])
    if mysql_mask.any():
        print(
            f"   Normalized MySQL domains (0,1): mu={df.loc[mysql_mask, LABEL_COL].mean():.4f}, "
            f"sigma={df.loc[mysql_mask, LABEL_COL].std():.4f}"
        )
    if postgres_mask.any():
        print(
            f"   Normalized PostgreSQL domains (2,3): mu={df.loc[postgres_mask, LABEL_COL].mean():.4f}, "
            f"sigma={df.loc[postgres_mask, LABEL_COL].std():.4f}"
        )

    print("   Per-engine normalized mean/std:")
    print(df.groupby("db_engine")[LABEL_COL].agg(["mean", "std"]).round(4))

    return (
        df, feat_cols_filtered, mask_cols, qp_cols, db_enc, hw_enc, feat_scaler,
        cost_mu_map, cost_sigma_map, target_transform
    )

print("INFO: Preprocessing functions defined")

class CostModelDataset(Dataset):
    """
    PyTorch dataset for cost model training.
    
    IMPORTANT: Costs should be ALREADY NORMALIZED in preprocessing.
    This class just packages data for PyTorch - no transformation applied.
    """
    def __init__(self, df, feat_cols, mask_cols, qp_cols,
                 db_enc, hw_enc, feat_scaler,
                 labeled_target_idx=None, raw_costs=None):
        X_feat = feat_scaler.transform(df[feat_cols].values.astype(np.float32))
        X_mask = df[mask_cols].values.astype(np.float32)
        self.x_feat = X_feat
        self.x_mask = X_mask
        self.qp_emb = df[qp_cols].values.astype(np.float32)

        # Conditioning inputs for regression head
        self.db_oh = np.eye(len(db_enc.classes_), dtype=np.float32)[
                         db_enc.transform(df["db_engine"])]
        self.hw_oh = np.eye(len(hw_enc.classes_), dtype=np.float32)[
                         hw_enc.transform(df["hardware"])]
        self.ram = df["ram_gb"].values.astype(np.float32).reshape(-1, 1)

        # Labels and domain IDs (costs already normalized)
        self.cost = df[LABEL_COL].values.astype(np.float32)
        self.domain = df[DOMAIN_COL].values.astype(np.int64)
        
        # Raw costs for unnormalized metric evaluation during training
        if raw_costs is not None:
            self.raw_cost = raw_costs.astype(np.float32)
        else:
            self.raw_cost = self.cost

        # has_label: 1 = use this row in task loss
        self.has_label = (self.domain == SOURCE_DOMAIN).astype(np.float32)
        if labeled_target_idx is not None:
            self.has_label[list(labeled_target_idx)] = 1.0

    def __len__(self):
        return len(self.x_feat)

    def __getitem__(self, idx):
        return {
            "x_feat": torch.tensor(self.x_feat[idx]),
            "x_mask": torch.tensor(self.x_mask[idx]),
            "qp_emb": torch.tensor(self.qp_emb[idx]),
            "db_oh": torch.tensor(self.db_oh[idx]),
            "hw_oh": torch.tensor(self.hw_oh[idx]),
            "ram": torch.tensor(self.ram[idx]),
            "cost": torch.tensor(self.cost[idx]),
            "raw_cost": torch.tensor(self.raw_cost[idx]),
            "domain": torch.tensor(self.domain[idx]),
            "has_label": torch.tensor(self.has_label[idx]),
        }


def build_sampler(dataset):
    """
    WeightedRandomSampler for balanced domain batches.
    Prevents source domain (largest) from dominating every batch.
    """
    domains = dataset.domain
    class_counts = np.bincount(domains, minlength=4).astype(np.float32)
    weights = 1.0 / class_counts[domains]
    return WeightedRandomSampler(weights, num_samples=len(dataset), replacement=True)

print("INFO: Dataset and sampler defined")

