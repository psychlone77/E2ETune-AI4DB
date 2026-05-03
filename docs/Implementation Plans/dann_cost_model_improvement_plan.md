# DANN Cost Model Improvement Plan

## Goal
Achieve **MAPE < 4%** on both source (PostgreSQL) and target (MySQL) domains, beating the SFT LightGBM baseline.

## SFT Baseline (Target to Beat)
| Metric | PostgreSQL (Source) | MySQL (Target) |
|--------|-------------------|----------------|
| MAPE | ~12.9% | ~15-20% |
| Spearman | 0.85+ | 0.75+ |

---

## Version History

### V1 (Copilot Baseline) — Kaggle Version 14
- **Architecture**: SAINT encoder + GRL adversarial domain classifier
- **Config**: `n_epochs=30, lr=1e-3, normalization=global, log_target=False, use_sampler=False`
- **Results**: SrcMAPE=33.2%, TgtMAPE=175.8%, DomAcc=0.413
- **Diagnosis**: Global normalization caused 6.5x scale gap. GRL was oscillating without helping regression.

### V2 (RSD + Tier 1 Fixes) — Kaggle Versions 15-19
- **Changes**: Per-engine norm, log-target, balanced sampler, RSD replacing GRL, DSBN path splitting
- **Config**: `n_epochs=50, lr=1e-3, normalization=per_engine, log_target=True, use_sampler=True`
- **Results**: NaN divergence from RSD eigendecomposition backward pass instability
- **Diagnosis**: RSD/eigh backward pass has div-by-zero when eigenvalues are degenerate. Fundamentally, adversarial/subspace alignment approaches are too fragile for regression.

### V3 (CORAL + Semi-Supervised + Huber) — Kaggle Version 20 [CURRENT]
- **Architecture**: SAINT encoder + CORAL covariance alignment + Huber loss + semi-supervised target labels
- **Config**: `n_epochs=100, lr=5e-4, normalization=per_engine, log_target=True, use_sampler=True`
- **Key Changes**:
  1. Replaced adversarial alignment with CORAL: Simple Frobenius norm of covariance difference. No eigendecomposition, no SVD, no GRL. Zero numerical instability risk.
  2. Huber loss (SmoothL1) instead of MSE: Robust to outliers in cost values.
  3. Semi-supervised learning: 30% of target domain samples are labeled and used in the task loss.
  4. Conservative CORAL schedule: No alignment for first 20% of training, then slowly ramp to lambda=0.05.
  5. Tighter prediction clamp [-10, 10]: Prevents log-space overflow during denormalization.
  6. 100 epochs + LR 5e-4 + CosineAnnealing to 1e-6.
  7. Early stopping with patience=20.
  8. Deeper regression head with extra LayerNorm.

---

## Research Insights

### Why DANN/GRL Fails for Regression
1. DANN assumes P_S(Y|X) = P_T(Y|X). In database tuning, MySQL and PostgreSQL have different cost-to-config mappings.
2. The GRL forces the encoder to make domains indistinguishable which destroys feature sensitivity for cost prediction.
3. Domain accuracy dropping to 8.7% (V2) confirmed the encoder was outputting noise.

### Why CORAL Works Better
1. CORAL only aligns second-order statistics (covariances), preserving task-relevant information.
2. Simple Frobenius norm — no adversarial dynamics, no gradient reversal instability.
3. Low lambda (0.05) acts as gentle regularization rather than a competing objective.

### Semi-Supervised Advantage
1. Even small amounts of labeled target data (10-30%) dramatically outperform pure unsupervised DA for regression.
2. We have labeled cost measurements for all domains — the unsupervised framing was artificially limiting us.

---

## Next Steps (if V3 doesn't reach <4% MAPE)

### Tier 4: Advanced Techniques
1. LambdaRank pairwise loss for ranking head.
2. Ensemble with LightGBM meta-learner.
3. Target fine-tuning: freeze encoder, fine-tune only regression head on target data.
4. Increase labeled target fraction to 50-100%.
