import random
import time

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
from scipy.stats import spearmanr
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader

try:
    from data import (
        CostModelDataset,
        DOMAIN_COL,
        SOURCE_DOMAIN,
        build_sampler,
        preprocess,
    )
    from eval import evaluate_on_raw_dataframe
    from models import DANNCostModel, coral_loss
except ImportError:  # Support package-style imports (src.*)
    from src.data import (
        CostModelDataset,
        DOMAIN_COL,
        SOURCE_DOMAIN,
        build_sampler,
        preprocess,
    )
    from src.eval import evaluate_on_raw_dataframe
    from src.models import DANNCostModel, coral_loss


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False


def compute_lambda_coral(p, max_lambda=0.05):
    """
    Lambda schedule for CORAL alignment.
    Conservative: warm up slowly and cap at a low max to avoid harming regression.
    """
    if p < 0.2:
        return 0.0  # Pure supervised warmup first
    elif p < 0.5:
        return ((p - 0.2) / 0.3) * max_lambda
    return max_lambda


def compute_lambda_dann(p, alpha=10.0, max_dann=0.3):
    """
    DANN lambda schedule (adapted from Ganin et al., JMLR 2016).
    Smoothly increases from 0 to max_dann.
    Delayed start: first 20% of training is pure supervised (GRL off).
    Capped at max_dann to prevent adversarial loss from overwhelming task loss.
    """
    import math
    if p < 0.2:
        return 0.0  # Pure supervised warmup
    p_adj = (p - 0.2) / 0.8  # Rescale remaining to [0, 1]
    raw = 2.0 / (1.0 + math.exp(-alpha * p_adj)) - 1.0
    return raw * max_dann


print("INFO: compute_lambda defined")


def _domain_count_map(df, col=DOMAIN_COL):
    if col not in df.columns or len(df) == 0:
        return {}
    vc = df[col].value_counts().sort_index()
    return {int(k): int(v) for k, v in vc.items()}


def _format_domain_counts(counts):
    return " | ".join(f"D{d}={n}" for d, n in sorted(counts.items()))


print("INFO: Training helpers defined")


def train_epoch(model, loader, optimizer, device, lambda_coral=0.0, alpha_grl=0.0,
                dann_weight=0.1, alpha_target=0.0, domain_class_weights=None):
    """Train for one epoch with Huber + CORAL + DANN (GRL) losses.
    
    dann_weight: multiplier on the DANN BCE loss to keep it proportional
                 to the task loss (~0.03). Without this, BCE (~0.6) drowns
                 out the regression signal.
    """
    model.train()

    total_loss = task_sum = coral_sum = dann_sum = 0.0
    domain_correct = domain_total = 0
    n = 0

    huber = torch.nn.SmoothL1Loss(beta=1.0)

    for batch in loader:
        x_feat = batch["x_feat"].to(device)
        x_mask = batch["x_mask"].to(device)
        qp_emb = batch["qp_emb"].to(device)
        db_oh = batch["db_oh"].to(device)
        hw_oh = batch["hw_oh"].to(device)
        ram = batch["ram"].to(device)
        cost = batch["cost"].to(device)
        domain = batch["domain"].to(device)
        has_label = batch["has_label"].to(device)

        # Forward pass with GRL alpha
        cost_pred, z, domain_logits = model(
            x_feat, x_mask, qp_emb, db_oh, hw_oh, ram, alpha_grl=alpha_grl
        )
        cost_pred = torch.clamp(cost_pred, min=-10, max=10)

        # --- Task loss: Huber on all labeled samples ---
        src_mask = has_label.bool()
        task_loss = (
            huber(cost_pred[src_mask], cost[src_mask])
            if src_mask.sum() > 0
            else torch.tensor(0.0, device=device)
        )

        # Also use labeled target samples if available
        tgt_mask = (~(domain == SOURCE_DOMAIN)) & has_label.bool()
        if alpha_target > 0 and tgt_mask.sum() > 0:
            task_loss = task_loss + alpha_target * huber(cost_pred[tgt_mask], cost[tgt_mask])

        # --- CORAL alignment loss on z ---
        src_z_mask = (domain == SOURCE_DOMAIN)
        tgt_z_mask = (~(domain == SOURCE_DOMAIN))
        if lambda_coral > 0 and src_z_mask.sum() > 2 and tgt_z_mask.sum() > 2:
            coral_l = coral_loss(z[src_z_mask], z[tgt_z_mask])
        else:
            coral_l = torch.tensor(0.0, device=device)

        # --- DANN domain classifier loss (BCE: source=1, target=0) ---
        if alpha_grl > 0:
            domain_labels = (domain == SOURCE_DOMAIN).float()  # 1=source, 0=target
            # Class-weight: target samples are ~25% of data, so upweight them
            n_src = (domain_labels == 1).sum().clamp(min=1)
            n_tgt = (domain_labels == 0).sum().clamp(min=1)
            pos_weight = (n_tgt.float() / n_src.float()).clamp(0.1, 10.0)
            bce_w = torch.nn.BCEWithLogitsLoss(pos_weight=pos_weight)
            dann_l = bce_w(domain_logits, domain_labels)
            # Track domain classifier accuracy
            with torch.no_grad():
                preds = (domain_logits > 0).float()
                domain_correct += (preds == domain_labels).sum().item()
                domain_total += len(domain_labels)
        else:
            dann_l = torch.tensor(0.0, device=device)

        loss = task_loss + lambda_coral * coral_l + dann_weight * dann_l

        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()

        total_loss += loss.item()
        task_sum += task_loss.item()
        coral_sum += coral_l.item()
        dann_sum += dann_l.item()
        n += 1

    if n == 0:
        return {"loss": float("nan"), "task_loss": float("nan"),
                "coral_loss": float("nan"), "dann_loss": float("nan"),
                "domain_acc": float("nan")}

    d_acc = domain_correct / max(domain_total, 1)
    return {
        "loss": total_loss / n, "task_loss": task_sum / n,
        "coral_loss": coral_sum / n, "dann_loss": dann_sum / n,
        "domain_acc": d_acc,
    }


def evaluate(model, loader, device):
    """Evaluate on labeled samples (for validation)."""
    model.eval()
    all_cp, all_ct, all_dt, all_hl, all_dl, all_dom = [], [], [], [], [], []

    with torch.no_grad():
        for batch in loader:
            x_feat = batch["x_feat"].to(device)
            x_mask = batch["x_mask"].to(device)
            qp_emb = batch["qp_emb"].to(device)
            domain = batch["domain"].to(device)
            cp, _, dl = model(
                x_feat, x_mask, qp_emb,
                batch["db_oh"].to(device),
                batch["hw_oh"].to(device),
                batch["ram"].to(device),
                alpha_grl=0.0,  # No GRL during eval
            )
            cp = torch.clamp(cp, min=-10, max=10)

            all_cp.append(cp.cpu())
            all_ct.append(batch["cost"])
            all_dt.append(batch["domain"])
            all_hl.append(batch["has_label"])
            all_dl.append(dl.cpu())
            all_dom.append(domain.cpu())

    cp = torch.cat(all_cp)
    ct = torch.cat(all_ct)
    dt = torch.cat(all_dt)
    hl = torch.cat(all_hl).bool()
    dl = torch.cat(all_dl)
    dom = torch.cat(all_dom)

    results = {}
    for d in range(4):
        mask = (dt == d) & hl
        if mask.sum() > 0:
            mse_val = F.mse_loss(cp[mask], ct[mask]).item()
            results[f"mse_domain_{d}"] = mse_val

    # Domain classifier accuracy (how well can we distinguish domains?)
    domain_labels = (dom == SOURCE_DOMAIN).float()
    domain_preds = (dl > 0).float()
    d_acc = (domain_preds == domain_labels).float().mean().item()
    results["domain_acc"] = d_acc
    results["h_divergence_approx"] = max(0.0, 2 * (2 * d_acc - 1))
    return results


def evaluate_all(model, loader, device, cost_mu_map=None, cost_sigma_map=None, db_classes=None, target_transform=None):
    """Evaluate on all samples (including unlabeled target)."""
    model.eval()
    all_cp, all_ct, all_dt, all_rt, all_db = [], [], [], [], []

    with torch.no_grad():
        for batch in loader:
            x_feat = batch["x_feat"].to(device)
            x_mask = batch["x_mask"].to(device)
            qp_emb = batch["qp_emb"].to(device)
            cp, _, _ = model(
                x_feat, x_mask, qp_emb,
                batch["db_oh"].to(device),
                batch["hw_oh"].to(device),
                batch["ram"].to(device),
                alpha_grl=0.0,
            )

            cp = torch.clamp(cp, min=-10, max=10)

            all_cp.append(cp.cpu().numpy())
            all_ct.append(batch["cost"].numpy())
            all_dt.append(batch["domain"].numpy())
            if "raw_cost" in batch:
                all_rt.append(batch["raw_cost"].numpy())
            all_db.append(batch["db_oh"].argmax(1).numpy())

    cp = np.concatenate(all_cp)
    ct = np.concatenate(all_ct)
    dt = np.concatenate(all_dt)

    if all_rt:
        rt = np.concatenate(all_rt)
        db = np.concatenate(all_db)
        
        cp_orig = np.zeros_like(cp)
        if cost_mu_map and "__global__" in cost_mu_map:
            cp_orig = cp * cost_sigma_map["__global__"] + cost_mu_map["__global__"]
        elif cost_mu_map and db_classes is not None:
            for idx, c in enumerate(db_classes):
                mask = db == idx
                sigma = cost_sigma_map.get(c, 1.0)
                mu = cost_mu_map.get(c, 0.0)
                cp_orig[mask] = cp[mask] * sigma + mu
        else:
            cp_orig = cp.copy()
            
        if target_transform and target_transform.get("type") == "log1p":
            cp_orig = np.expm1(np.clip(cp_orig, a_min=None, a_max=20.0))
            
        cp_orig = np.clip(cp_orig, 0.0, 1e12)

    results = {}
    for d in range(4):
        mask = dt == d
        if mask.sum() > 0:
            mse_val = float(np.mean((cp[mask] - ct[mask])**2))
            results[f"mse_domain_{d}_all"] = mse_val
            
            if all_rt:
                rt_d = rt[mask]
                cp_d = cp_orig[mask]
                rmse = float(np.sqrt(np.mean((rt_d - cp_d)**2)))
                # Robust MAPE: threshold at 1% of median, cap at 200%
                median_abs = np.median(np.abs(rt_d))
                threshold = max(median_abs * 0.01, 0.1)
                nz = np.abs(rt_d) > threshold
                if nz.any():
                    ape = np.abs((cp_d[nz] - rt_d[nz]) / rt_d[nz]) * 100.0
                    ape = np.minimum(ape, 200.0)
                    mape = float(np.mean(ape))
                else:
                    mape = float("nan")
                results[f"rmse_domain_{d}_all"] = rmse
                results[f"mape_domain_{d}_all"] = mape
                # Spearman rank correlation (key metric for ranking-based labeling)
                if len(rt_d) > 2:
                    rho, _ = spearmanr(rt_d, cp_d)
                    results[f"spearman_domain_{d}_all"] = float(rho) if np.isfinite(rho) else float("nan")
                else:
                    results[f"spearman_domain_{d}_all"] = float("nan")

    return results


print("INFO: Training helpers defined")


def train(
    da,
    n_epochs=100,
    batch_size=256,
    lr=5e-4,
    d_model=128,
    n_heads=4,
    n_layers=2,
    dropout=0.15,
    alpha_target=1.0,
    labeled_target_frac=1.0,
    normalization_mode="per_engine",
    log_target=True,
    lambda_coral_max=0.05,
    use_dann=True,
    use_sampler=True,
    seed=42,
    device_str="cpu",
):
    """
    Train the DANN cost model with CORAL + adversarial GRL alignment.

    Architecture (V6 - True DANN):
    1. SAINT encoder -> shared features z
    2. Regression head (Gy): z -> cost prediction (Huber loss)
    3. Domain classifier (Gd) via GRL: z -> source/target (BCE loss)
    4. CORAL: aligns covariance matrices of source/target z
    
    The GRL forces the encoder to learn domain-invariant features.
    CORAL provides complementary second-order statistical alignment.
    """
    set_seed(seed)
    device = torch.device(device_str if torch.cuda.is_available() or device_str == "cpu" else "cpu")

    (
        df,
        feat_cols,
        mask_cols,
        qp_cols,
        db_enc,
        hw_enc,
        feat_scaler,
        cost_mu_map,
        cost_sigma_map,
        target_transform,
    ) = preprocess(da, normalization_mode=normalization_mode, log_target=log_target)

    train_idx, val_idx = train_test_split(
        np.arange(len(df)),
        test_size=0.15,
        stratify=df[DOMAIN_COL].values,
        random_state=seed,
    )

    train_df = df.iloc[train_idx].reset_index(drop=True)
    val_df = df.iloc[val_idx].reset_index(drop=True)

    split_sizes = {"train": len(train_df), "val": len(val_df)}
    split_domain_counts = {
        "train": _domain_count_map(train_df),
        "val": _domain_count_map(val_df),
    }

    # Semi-supervised: label a fraction of target domain samples
    labeled_target_idx = None
    if labeled_target_frac > 0:
        rng = np.random.RandomState(seed)
        tgt_idx = np.where(train_df[DOMAIN_COL].values != SOURCE_DOMAIN)[0]
        if len(tgt_idx) > 0:
            n_label = max(1, int(len(tgt_idx) * labeled_target_frac))
            labeled_target_idx = rng.choice(tgt_idx, size=n_label, replace=False).tolist()
            print(f"INFO: Semi-supervised mode: labeling {n_label}/{len(tgt_idx)} target samples ({labeled_target_frac*100:.0f}%)")

    ds_kw = dict(
        feat_cols=feat_cols,
        mask_cols=mask_cols,
        qp_cols=qp_cols,
        db_enc=db_enc,
        hw_enc=hw_enc,
        feat_scaler=feat_scaler,
    )

    train_ds = CostModelDataset(train_df, **ds_kw, labeled_target_idx=labeled_target_idx, 
                                raw_costs=train_df["raw_cost"].values if "raw_cost" in train_df else None)
    val_ds = CostModelDataset(val_df, **ds_kw, 
                              raw_costs=val_df["raw_cost"].values if "raw_cost" in val_df else None)

    class_counts = np.bincount(train_ds.domain, minlength=4).astype(np.float32)
    class_counts = np.maximum(class_counts, 1.0)
    domain_class_weights = (len(train_ds) / (4.0 * class_counts)).astype(np.float32)
    domain_class_weights = domain_class_weights / domain_class_weights.mean()
    domain_class_weights = torch.tensor(domain_class_weights, dtype=torch.float32, device=device)

    if use_sampler:
        sampler = build_sampler(train_ds)
        train_loader = DataLoader(train_ds, batch_size=batch_size, sampler=sampler, num_workers=0)
    else:
        train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True, num_workers=0)

    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False, num_workers=0)

    num_feat_total = len(feat_cols)
    model = DANNCostModel(
        num_features=num_feat_total,
        n_masks=len(mask_cols),
        n_db_engines=len(db_enc.classes_),
        n_hardware=len(hw_enc.classes_),
        d_model=d_model,
        n_heads=n_heads,
        n_layers=n_layers,
        n_domains=4,
        dropout=dropout,
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=n_epochs, eta_min=1e-6)

    best_mse = float("inf")
    best_spearman = -float("inf")
    best_state = None
    best_epoch = 0
    patience = 40
    patience_counter = 0
    total_steps = n_epochs * max(len(train_loader), 1)
    step = 0
    history = []
    start_time = time.time()

    for epoch in range(1, n_epochs + 1):
        p = step / max(total_steps, 1)
        lam_coral = compute_lambda_coral(p, max_lambda=lambda_coral_max)
        lam_dann = compute_lambda_dann(p, alpha=10.0) if use_dann else 0.0
        tr = train_epoch(
            model,
            train_loader,
            optimizer,
            device,
            lambda_coral=lam_coral,
            alpha_grl=lam_dann,
            alpha_target=alpha_target,
            domain_class_weights=domain_class_weights,
        )
        step += len(train_loader)
        scheduler.step()

        val = evaluate(model, val_loader, device)
        val_all = evaluate_all(model, val_loader, device, 
                               cost_mu_map=cost_mu_map, 
                               cost_sigma_map=cost_sigma_map, 
                               db_classes=db_enc.classes_, 
                               target_transform=target_transform)

        src_mse = val.get("mse_domain_3", float("nan"))
        tgt_mse = np.nanmean(
            [val_all.get(f"mse_domain_{d}_all", float("nan")) for d in [0, 1, 2]]
        )
        
        src_rmse = val_all.get("rmse_domain_3_all", float("nan"))
        src_mape = val_all.get("mape_domain_3_all", float("nan"))
        tgt_rmse = np.nanmean([val_all.get(f"rmse_domain_{d}_all", float("nan")) for d in [0, 1, 2]])
        tgt_mape = np.nanmean([val_all.get(f"mape_domain_{d}_all", float("nan")) for d in [0, 1, 2]])
        
        # Macro validation MSE (for logging)
        all_val_mses = [val_all.get(f"mse_domain_{d}_all", float("nan")) for d in [0, 1, 2, 3]]
        macro_val_mse = np.nanmean(all_val_mses)

        # Macro Spearman (PRIMARY early stopping metric for ranking-based labeling)
        all_val_spearmans = [val_all.get(f"spearman_domain_{d}_all", float("nan")) for d in [0, 1, 2, 3]]
        macro_val_spearman = np.nanmean([s for s in all_val_spearmans if np.isfinite(s)]) if any(np.isfinite(s) for s in all_val_spearmans) else float("nan")

        # Print every epoch to monitor convergence
        if epoch % 5 == 0 or epoch == 1:
            d_acc_str = f"{tr.get('domain_acc', 0):.1%}" if use_dann else "off"
            spear_str = f"{macro_val_spearman:.4f}" if np.isfinite(macro_val_spearman) else "N/A"
            print(f"Epoch {epoch:03d}/{n_epochs} | "
                  f"TrLoss: {tr['loss']:.4f} (task={tr['task_loss']:.4f} "
                  f"coral={tr['coral_loss']:.4f} dann={tr['dann_loss']:.4f}) | "
                  f"Spear: {spear_str} | MSE: {macro_val_mse:.4f} | "
                  f"SrcMAPE: {src_mape:.1f}% | TgtMAPE: {tgt_mape:.1f}% | "
                  f"DomAcc: {d_acc_str} | "
                  f"lam_c: {lam_coral:.4f} lam_d: {lam_dann:.2f}")

        history.append(
            dict(
                epoch=epoch,
                lambda_coral=lam_coral,
                lambda_dann=lam_dann,
                train_loss=tr["loss"],
                task_loss=tr["task_loss"],
                coral_loss=tr["coral_loss"],
                dann_loss=tr["dann_loss"],
                train_domain_acc=tr.get("domain_acc", float("nan")),
                src_mse=src_mse,
                tgt_mse=tgt_mse,
                macro_val_mse=macro_val_mse,
                macro_val_spearman=macro_val_spearman,
                src_mape=src_mape,
                tgt_mape=tgt_mape,
                domain_acc=val["domain_acc"],
                h_div=val["h_divergence_approx"],
            )
        )

        # Early stopping: maximize Spearman (ranking accuracy) not minimize MSE
        if np.isfinite(macro_val_spearman) and macro_val_spearman > best_spearman:
            best_spearman = macro_val_spearman
            best_mse = macro_val_mse  # Track MSE too for logging
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            best_epoch = epoch
            patience_counter = 0
        else:
            patience_counter += 1

        if patience_counter >= patience and epoch > 30:
            print(f"Early stopping at epoch {epoch} (no improvement for {patience} epochs). Best epoch: {best_epoch}")
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    train_minutes = (time.time() - start_time) / 60.0
    history_df = pd.DataFrame(history)

    return {
        "model": model,
        "history_df": history_df,
        "best_epoch": best_epoch,
        "best_mse": best_mse,
        "train_minutes": train_minutes,
        "split_sizes": split_sizes,
        "split_domain_counts": split_domain_counts,
        "feat_cols": feat_cols,
        "mask_cols": mask_cols,
        "qp_cols": qp_cols,
        "db_enc": db_enc,
        "hw_enc": hw_enc,
        "feat_scaler": feat_scaler,
        "cost_mu_map": cost_mu_map,
        "cost_sigma_map": cost_sigma_map,
        "target_transform": target_transform,
    }


print("INFO: Main training function defined")


def train_dann_custom(
    da,
    n_epochs=100,
    batch_size=256,
    lr=5e-4,
    alpha_target=1.0,
    labeled_target_frac=1.0,
    normalization_mode="per_engine",
    log_target=True,
    seed=42,
    device_str="cpu",
    lambda_coral_max=0.05,
    use_dann=True,
    use_sampler=True,
):
    return train(
        da,
        n_epochs=n_epochs,
        batch_size=batch_size,
        lr=lr,
        alpha_target=alpha_target,
        labeled_target_frac=labeled_target_frac,
        normalization_mode=normalization_mode,
        log_target=log_target,
        lambda_coral_max=lambda_coral_max,
        use_dann=use_dann,
        use_sampler=use_sampler,
        seed=seed,
        device_str=device_str,
    )


def train_and_eval(
    da,
    normalization_mode="per_engine",
    n_epochs=100,
    batch_size=256,
    lr=5e-4,
    seed=42,
    device_str="cpu",
    log_target=True,
    use_sampler=True,
    n_folds=5,
    labeled_target_frac=1.0,
    use_dann=True,
):
    """
    Complete training + evaluation pipeline with k-fold cross-validation.
    
    For each fold:
      - 1 fold = test set
      - remaining folds = train+val (with internal 85/15 split for early stopping)
    
    Returns aggregated metrics (mean ± std across folds), combined history, best model.
    """
    from sklearn.model_selection import StratifiedKFold

    print(f"INFO: Starting {n_folds}-fold CV with normalization_mode={normalization_mode}, "
          f"log_target={log_target}, use_sampler={use_sampler}, n_epochs={n_epochs}, lr={lr}")
    print(f"INFO: Total samples: {len(da)}")

    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=seed)
    
    fold_metrics = []
    fold_histories = []
    best_fold_mape = float("inf")
    best_model = None
    best_fold_idx = -1
    total_train_minutes = 0.0

    for fold_idx, (train_val_idx, test_idx) in enumerate(skf.split(da, da[DOMAIN_COL])):
        fold_num = fold_idx + 1
        print(f"\n{'='*60}")
        print(f"FOLD {fold_num}/{n_folds}")
        print(f"{'='*60}")

        da_train_val = da.iloc[train_val_idx].reset_index(drop=True)
        da_test = da.iloc[test_idx].reset_index(drop=True)
        print(f"Train+Val: {len(da_train_val)}, Test: {len(da_test)}")

        # Use a different seed for each fold to get diverse models
        fold_seed = seed + fold_idx

        out = train_dann_custom(
            da_train_val,
            n_epochs=n_epochs,
            batch_size=batch_size,
            lr=lr,
            labeled_target_frac=labeled_target_frac,
            use_dann=use_dann,
            normalization_mode=normalization_mode,
            log_target=log_target,
            use_sampler=use_sampler,
            seed=fold_seed,
            device_str=device_str,
        )

        model = out["model"]
        total_train_minutes += out["train_minutes"]

        print(f"Fold {fold_num} best epoch: {out['best_epoch']} | "
              f"macro-val MSE: {out['best_mse']:.6f} | "
              f"trained in {out['train_minutes']:.1f} min")

        # Evaluate on held-out test fold
        test_metrics = evaluate_on_raw_dataframe(
            model=model,
            raw_df=da_test,
            db_enc=out["db_enc"],
            hw_enc=out["hw_enc"],
            feat_scaler=out["feat_scaler"],
            feat_cols=out["feat_cols"],
            mask_cols=out["mask_cols"],
            qp_cols=out["qp_cols"],
            cost_mu_map=out["cost_mu_map"],
            cost_sigma_map=out["cost_sigma_map"],
            target_transform=out["target_transform"],
        )

        fold_mape = test_metrics.get("mape_pct", float("nan"))
        fold_spearman = test_metrics.get("spearman_rho", float("nan"))

        print(f"Fold {fold_num} TEST: MAPE={fold_mape:.2f}%, "
              f"RMSE={test_metrics.get('rmse_orig', float('nan')):.4f}, "
              f"Spearman={fold_spearman:.4f}")

        if "per_domain" in test_metrics:
            for d, dm in sorted(test_metrics["per_domain"].items()):
                print(f"  Domain {d}: MAPE={dm.get('mape', float('nan')):.2f}%, "
                      f"RMSE={dm.get('rmse', float('nan')):.4f}, "
                      f"Spearman={dm.get('spearman', float('nan')):.4f}, "
                      f"n={dm.get('n', 0)}")

        fold_metrics.append({
            "fold": fold_num,
            "pooled_mape": fold_mape,
            "pooled_rmse": test_metrics.get("rmse_orig", float("nan")),
            "pooled_spearman": fold_spearman,
            "best_epoch": out["best_epoch"],
            "train_minutes": out["train_minutes"],
            "per_domain": test_metrics.get("per_domain", {}),
            "pooled": test_metrics.get("pooled", {}),
        })

        # Add fold column to history
        hist = out["history_df"].copy()
        hist["fold"] = fold_num
        fold_histories.append(hist)

        # Track best fold for model selection
        if np.isfinite(fold_mape) and fold_mape < best_fold_mape:
            best_fold_mape = fold_mape
            best_model = model
            best_fold_idx = fold_num

    # === Aggregate results across folds ===
    print(f"\n{'='*60}")
    print(f"CROSS-VALIDATION RESULTS ({n_folds}-fold)")
    print(f"{'='*60}")

    pooled_mapes = [m["pooled_mape"] for m in fold_metrics if np.isfinite(m["pooled_mape"])]
    pooled_rmses = [m["pooled_rmse"] for m in fold_metrics if np.isfinite(m["pooled_rmse"])]
    pooled_spearmans = [m["pooled_spearman"] for m in fold_metrics if np.isfinite(m["pooled_spearman"])]

    print(f"\nPooled MAPE:     {np.mean(pooled_mapes):.2f}% ± {np.std(pooled_mapes):.2f}%")
    print(f"Pooled RMSE:     {np.mean(pooled_rmses):.4f} ± {np.std(pooled_rmses):.4f}")
    print(f"Pooled Spearman: {np.mean(pooled_spearmans):.4f} ± {np.std(pooled_spearmans):.4f}")

    # Per-domain aggregation
    all_domains = set()
    for m in fold_metrics:
        all_domains.update(m["per_domain"].keys())

    print(f"\nPer-domain results (mean ± std across {n_folds} folds):")
    domain_summary = {}
    for d in sorted(all_domains):
        d_mapes = [m["per_domain"][d]["mape"] for m in fold_metrics if d in m["per_domain"] and np.isfinite(m["per_domain"][d].get("mape", float("nan")))]
        d_rmses = [m["per_domain"][d]["rmse"] for m in fold_metrics if d in m["per_domain"] and np.isfinite(m["per_domain"][d].get("rmse", float("nan")))]
        d_spearmans = [m["per_domain"][d]["spearman"] for m in fold_metrics if d in m["per_domain"] and np.isfinite(m["per_domain"][d].get("spearman", float("nan")))]
        d_ns = [m["per_domain"][d]["n"] for m in fold_metrics if d in m["per_domain"]]

        mape_str = f"{np.mean(d_mapes):.2f}% ± {np.std(d_mapes):.2f}%" if d_mapes else "N/A"
        rmse_str = f"{np.mean(d_rmses):.4f} ± {np.std(d_rmses):.4f}" if d_rmses else "N/A"
        spear_str = f"{np.mean(d_spearmans):.4f} ± {np.std(d_spearmans):.4f}" if d_spearmans else "N/A"
        n_str = f"{np.mean(d_ns):.0f}" if d_ns else "N/A"

        print(f"  Domain {d}: MAPE={mape_str} | RMSE={rmse_str} | Spearman={spear_str} | n≈{n_str}")

        domain_summary[d] = {
            "mape_mean": float(np.mean(d_mapes)) if d_mapes else float("nan"),
            "mape_std": float(np.std(d_mapes)) if d_mapes else float("nan"),
            "rmse_mean": float(np.mean(d_rmses)) if d_rmses else float("nan"),
            "rmse_std": float(np.std(d_rmses)) if d_rmses else float("nan"),
            "spearman_mean": float(np.mean(d_spearmans)) if d_spearmans else float("nan"),
            "spearman_std": float(np.std(d_spearmans)) if d_spearmans else float("nan"),
        }

    print(f"\nTotal training time: {total_train_minutes:.1f} minutes ({total_train_minutes/60:.1f} hours)")
    print(f"Best fold: {best_fold_idx} (MAPE={best_fold_mape:.2f}%)")

    # Combine histories
    history_df = pd.concat(fold_histories, ignore_index=True)

    return (
        {
            "pooled": {
                "mape": float(np.mean(pooled_mapes)) if pooled_mapes else float("nan"),
                "mape_std": float(np.std(pooled_mapes)) if pooled_mapes else float("nan"),
                "rmse": float(np.mean(pooled_rmses)) if pooled_rmses else float("nan"),
                "rmse_std": float(np.std(pooled_rmses)) if pooled_rmses else float("nan"),
                "spearman": float(np.mean(pooled_spearmans)) if pooled_spearmans else float("nan"),
                "spearman_std": float(np.std(pooled_spearmans)) if pooled_spearmans else float("nan"),
            },
            "per_domain": domain_summary,
            "fold_metrics": fold_metrics,
            "domain_acc": float("nan"),
            "best_epoch": fold_metrics[best_fold_idx - 1]["best_epoch"] if best_fold_idx > 0 else 0,
            "train_minutes": total_train_minutes,
            "n_folds": n_folds,
        },
        history_df,
        best_model,
        domain_summary,
    )


print("INFO: run_ablation_suite ready")
