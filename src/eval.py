import json
import numpy as np
import pandas as pd
import torch
from scipy.stats import spearmanr
from torch.utils.data import DataLoader

try:
    from data import (
        CostModelDataset,
        DOMAIN_COL,
        LABEL_COL,
        QP_EMB_COL,
        expand_qp_emb,
        get_log_scale_cols,
    )
except ImportError:  # Support package-style imports (src.*)
    from src.data import (
        CostModelDataset,
        DOMAIN_COL,
        LABEL_COL,
        QP_EMB_COL,
        expand_qp_emb,
        get_log_scale_cols,
    )


def _robust_mape(yt, yp, cap_pct=200.0):
    """Compute MAPE with robust threshold and capping to prevent near-zero denominator explosion."""
    # Use 1% of median as threshold to exclude near-zero costs
    median_abs = np.median(np.abs(yt))
    threshold = max(median_abs * 0.01, 0.1)  # At least 0.1 to be safe
    nz = np.abs(yt) > threshold
    if not nz.any():
        return float("nan")
    ape = np.abs((yp[nz] - yt[nz]) / yt[nz]) * 100.0
    # Cap individual APE at cap_pct to prevent outlier explosion
    ape = np.minimum(ape, cap_pct)
    return float(np.mean(ape))


def _per_domain_metrics(cost_true_orig, cost_pred_orig, domain):
    per_domain = {}
    for d in sorted(set(domain.tolist())):
        mask = domain == d
        if mask.sum() == 0:
            continue
        yt = cost_true_orig[mask]
        yp = cost_pred_orig[mask]
        rmse = float(np.sqrt(np.mean((yp - yt) ** 2)))
        mape = _robust_mape(yt, yp)
        rho, _ = spearmanr(yt, yp) if len(yt) > 2 else (float("nan"), 1.0)
        per_domain[str(int(d))] = {
            "n": int(mask.sum()),
            "rmse": rmse,
            "mape": mape,
            "spearman": float(rho),
        }
    return per_domain


def evaluate_on_raw_dataframe(
    model,
    raw_df,
    db_enc,
    hw_enc,
    feat_scaler,
    feat_cols,
    mask_cols,
    qp_cols,
    cost_mu_map,
    cost_sigma_map,
    batch_size=256,
    target_transform=None,
):
    eval_df = raw_df.copy()

    if eval_df[QP_EMB_COL].dtype == object and isinstance(eval_df[QP_EMB_COL].iloc[0], str):
        eval_df[QP_EMB_COL] = eval_df[QP_EMB_COL].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

    log_scale_cols = get_log_scale_cols(eval_df)
    for col in log_scale_cols:
        eval_df[col] = np.log1p(eval_df[col].clip(lower=0))

    eval_df[feat_cols] = eval_df[feat_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    eval_df[mask_cols] = eval_df[mask_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    for col in feat_cols:
        eval_df[col] = np.clip(eval_df[col], -1e4, 1e4)

    emb_df_eval, _ = expand_qp_emb(eval_df)
    emb_df_eval = emb_df_eval.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    eval_df = pd.concat([eval_df.reset_index(drop=True), emb_df_eval.reset_index(drop=True)], axis=1)

    eval_df[LABEL_COL] = pd.to_numeric(eval_df[LABEL_COL], errors="coerce")
    finite_cost_mask = np.isfinite(eval_df[LABEL_COL].to_numpy())
    if not finite_cost_mask.all():
        eval_df = eval_df.loc[finite_cost_mask].reset_index(drop=True)

    if len(eval_df) == 0:
        return {
            "mse_orig": float("nan"),
            "rmse_orig": float("nan"),
            "mae_orig": float("nan"),
            "nrmse_pct": float("nan"),
            "mape_pct": float("nan"),
            "smape_pct": float("nan"),
            "spearman_rho": float("nan"),
            "spearman_pval": float("nan"),
            "domain_acc": float("nan"),
            "n_samples": 0,
            "pooled": {"rmse": float("nan"), "mape": float("nan"), "spearman": float("nan"), "nrmse": float("nan")},
            "per_domain": {},
        }

    # CRITICAL: Apply the same target transform as preprocess() BEFORE normalizing
    # The mu/sigma were computed on log-transformed costs, so we must log-transform here too
    if target_transform and target_transform.get("type") == "log1p":
        eval_df[LABEL_COL] = np.log1p(np.maximum(eval_df[LABEL_COL].values, 0.0))

    if "__global__" in cost_mu_map:
        global_mu = float(cost_mu_map["__global__"])
        global_sigma = float(max(cost_sigma_map["__global__"], 1e-8))
        mu_eval = pd.Series(global_mu, index=eval_df.index)
        sigma_eval = pd.Series(global_sigma, index=eval_df.index)
    else:
        global_mu = float(np.mean(list(cost_mu_map.values())))
        global_sigma = float(np.mean(list(cost_sigma_map.values())))
        mu_eval = eval_df["db_engine"].map(cost_mu_map).fillna(global_mu)
        sigma_eval = eval_df["db_engine"].map(cost_sigma_map).fillna(global_sigma).clip(lower=1e-8)
    eval_df[LABEL_COL] = (eval_df[LABEL_COL] - mu_eval) / sigma_eval

    finite_norm_mask = np.isfinite(eval_df[LABEL_COL].to_numpy())
    if not finite_norm_mask.all():
        eval_df = eval_df.loc[finite_norm_mask].reset_index(drop=True)

    if len(eval_df) == 0:
        return {
            "mse_orig": float("nan"),
            "rmse_orig": float("nan"),
            "mae_orig": float("nan"),
            "nrmse_pct": float("nan"),
            "mape_pct": float("nan"),
            "smape_pct": float("nan"),
            "spearman_rho": float("nan"),
            "spearman_pval": float("nan"),
            "domain_acc": float("nan"),
            "n_samples": 0,
            "pooled": {"rmse": float("nan"), "mape": float("nan"), "spearman": float("nan"), "nrmse": float("nan")},
            "per_domain": {},
        }

    device = next(model.parameters()).device
    eval_ds = CostModelDataset(eval_df, feat_cols, mask_cols, qp_cols, db_enc, hw_enc, feat_scaler)
    eval_loader = DataLoader(eval_ds, batch_size=batch_size, shuffle=False, num_workers=0)

    model.eval()
    all_cost_pred = []
    all_cost_true = []
    all_domain_pred = []
    all_domain_true = []

    with torch.no_grad():
        for batch in eval_loader:
            x_feat = batch["x_feat"].to(device)
            x_mask = batch["x_mask"].to(device)
            qp_emb = batch["qp_emb"].to(device)
            cost_pred, _, domain_logits = model(
                x_feat,
                x_mask,
                qp_emb,
                batch["db_oh"].to(device),
                batch["hw_oh"].to(device),
                batch["ram"].to(device),
                alpha_grl=0.0,
            )
            domain_pred = (domain_logits > 0).long()  # 1=source, 0=target
            all_cost_pred.append(cost_pred.cpu().numpy())
            all_cost_true.append(batch["cost"].numpy())
            all_domain_pred.append(domain_pred.cpu().numpy())
            all_domain_true.append(batch["domain"].numpy())

    cost_pred_norm = np.concatenate(all_cost_pred)
    cost_true_norm = np.concatenate(all_cost_true)
    domain_pred_all = np.concatenate(all_domain_pred)
    domain_true_all = np.concatenate(all_domain_true)

    if "__global__" in cost_mu_map:
        mu_eval_arr = np.full(len(eval_df), float(cost_mu_map["__global__"]), dtype=np.float32)
        sigma_eval_arr = np.full(
            len(eval_df), float(max(cost_sigma_map["__global__"], 1e-8)), dtype=np.float32
        )
    else:
        global_mu = float(np.mean(list(cost_mu_map.values())))
        global_sigma = float(np.mean(list(cost_sigma_map.values())))
        mu_eval_arr = eval_df["db_engine"].map(cost_mu_map).fillna(global_mu).to_numpy(dtype=np.float32)
        sigma_eval_arr = (
            eval_df["db_engine"].map(cost_sigma_map).fillna(global_sigma).clip(lower=1e-8).to_numpy(dtype=np.float32)
        )

    cost_pred_orig = cost_pred_norm * sigma_eval_arr + mu_eval_arr
    cost_true_orig = cost_true_norm * sigma_eval_arr + mu_eval_arr

    if target_transform and target_transform.get("type") == "log1p":
        cost_pred_orig = np.expm1(np.clip(cost_pred_orig, a_min=None, a_max=20.0))
        cost_true_orig = np.expm1(np.clip(cost_true_orig, a_min=None, a_max=20.0))

    finite_pair_mask = np.isfinite(cost_pred_orig) & np.isfinite(cost_true_orig)
    if not finite_pair_mask.all():
        cost_pred_orig = cost_pred_orig[finite_pair_mask]
        cost_true_orig = cost_true_orig[finite_pair_mask]
        domain_pred_all = domain_pred_all[finite_pair_mask]
        domain_true_all = domain_true_all[finite_pair_mask]

    if len(cost_true_orig) == 0:
        return {
            "mse_orig": float("nan"),
            "rmse_orig": float("nan"),
            "mae_orig": float("nan"),
            "nrmse_pct": float("nan"),
            "mape_pct": float("nan"),
            "smape_pct": float("nan"),
            "spearman_rho": float("nan"),
            "spearman_pval": float("nan"),
            "domain_acc": float("nan"),
            "n_samples": 0,
            "pooled": {"rmse": float("nan"), "mape": float("nan"), "spearman": float("nan"), "nrmse": float("nan")},
            "per_domain": {},
        }

    err = cost_pred_orig - cost_true_orig
    abs_err = np.abs(err)

    mse_orig = float(np.mean(err ** 2))
    rmse_orig = float(np.sqrt(mse_orig))
    mae_orig = float(np.mean(abs_err))

    orig_range = float(np.ptp(cost_true_orig))
    nrmse_pct = float((rmse_orig / (orig_range + 1e-8)) * 100.0)

    mape_pct = _robust_mape(cost_true_orig, cost_pred_orig)

    smape_pct = float(
        np.mean((2.0 * abs_err) / (np.abs(cost_true_orig) + np.abs(cost_pred_orig) + 1e-8)) * 100.0
    )

    spearman_rho, spearman_pval = spearmanr(cost_true_orig, cost_pred_orig)
    domain_acc = float(np.mean(domain_pred_all == domain_true_all))

    per_domain = _per_domain_metrics(cost_true_orig, cost_pred_orig, domain_true_all)

    return {
        "mse_orig": mse_orig,
        "rmse_orig": rmse_orig,
        "mae_orig": mae_orig,
        "nrmse_pct": nrmse_pct,
        "mape_pct": mape_pct,
        "smape_pct": smape_pct,
        "spearman_rho": float(spearman_rho),
        "spearman_pval": float(spearman_pval),
        "domain_acc": domain_acc,
        "n_samples": int(len(cost_true_orig)),
        "pooled": {
            "rmse": rmse_orig,
            "mape": mape_pct,
            "spearman": float(spearman_rho),
            "nrmse": nrmse_pct,
        },
        "per_domain": per_domain,
    }


print("INFO: evaluate_on_raw_dataframe ready")
