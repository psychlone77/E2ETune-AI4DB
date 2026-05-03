import torch
import torch.nn as nn
from torch.autograd import Function
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler, LabelEncoder
from pathlib import Path
import warnings
import json
import os
import matplotlib.pyplot as plt
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error


def coral_loss(source_features, target_features):
    """
    CORAL loss (Sun & Saenko, ECCV 2016).
    Aligns second-order statistics (covariance matrices) of source and target.
    Much more numerically stable than RSD/adversarial approaches for regression.
    """
    d = source_features.size(1)
    ns = source_features.size(0)
    nt = target_features.size(0)

    # Center
    src = source_features - source_features.mean(0, keepdim=True)
    tgt = target_features - target_features.mean(0, keepdim=True)

    # Covariance matrices
    cs = (src.T @ src) / max(ns - 1, 1)
    ct = (tgt.T @ tgt) / max(nt - 1, 1)

    # Frobenius norm of difference, normalized by 4*d^2
    loss = torch.sum((cs - ct) ** 2) / (4 * d * d)
    return loss

print("INFO: CORAL loss defined")


# ======================== Gradient Reversal Layer ========================
class GradientReversalFunction(Function):
    """Gradient Reversal Layer (Ganin et al., JMLR 2016).
    Forward: identity.  Backward: negate and scale by alpha."""
    @staticmethod
    def forward(ctx, x, alpha):
        ctx.alpha = alpha
        return x.clone()

    @staticmethod
    def backward(ctx, grad_output):
        return -ctx.alpha * grad_output, None


class GradientReversalLayer(nn.Module):
    """Wraps GradientReversalFunction for use in nn.Sequential / forward()."""
    def __init__(self, alpha=1.0):
        super().__init__()
        self.alpha = alpha

    def forward(self, x):
        return GradientReversalFunction.apply(x, self.alpha)

    def set_alpha(self, alpha):
        self.alpha = alpha

print("INFO: Gradient Reversal Layer defined")


# ======================== SAINT Encoder ========================
class SAINTBlock(nn.Module):
    """Single SAINT transformer block with feature attention only.
    Row attention removed for stability and speed on small datasets."""
    def __init__(self, d_model, n_heads, dropout=0.1):
        super().__init__()
        self.feat_attn = nn.MultiheadAttention(d_model, n_heads,
                                                dropout=dropout, batch_first=True)
        self.ln1 = nn.LayerNorm(d_model)
        self.ln3 = nn.LayerNorm(d_model)
        self.ff = nn.Sequential(
            nn.Linear(d_model, d_model * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 2, d_model),
            nn.Dropout(dropout),
        )

    def forward(self, x):
        # Feature attention (within sample, across features)
        res = x
        x2, _ = self.feat_attn(x, x, x)
        x = self.ln1(res + x2)

        # Feed-forward
        res = x
        x = self.ln3(res + self.ff(x))
        return x


class SAINTEncoder(nn.Module):
    """Gf: tabular features -> latent vector z"""
    def __init__(self, num_features, d_model=128, n_heads=4, n_layers=2, dropout=0.1):
        super().__init__()
        self.num_features = num_features
        self.feat_embed = nn.Embedding(num_features, d_model)
        self.val_proj = nn.Linear(1, d_model)
        self.blocks = nn.ModuleList([SAINTBlock(d_model, n_heads, dropout)
                                     for _ in range(n_layers)])
        self.ln_out = nn.LayerNorm(d_model)

    def forward(self, x):
        B, F = x.shape
        feat_ids = torch.arange(F, device=x.device).unsqueeze(0).expand(B, -1)
        tokens = self.feat_embed(feat_ids) + self.val_proj(x.unsqueeze(-1))
        for block in self.blocks:
            tokens = block(tokens)
        z = self.ln_out(tokens).mean(dim=1)
        return z

print("INFO: SAINT encoder defined")


# ======================== Regression Head (V4 — simpler, proven) ========================
class RegressionHead(nn.Module):
    """Gy: [z | plan_emb | masks | db_engine_oh | hardware_oh | ram_gb] -> predicted cost
    
    V4 architecture: 256->128->64->1 (simpler, better performance than V5's wider head).
    """
    def __init__(self, z_dim, plan_emb_dim, n_masks, n_db_engines, n_hardware, dropout=0.1, n_domains=4):
        super().__init__()
        cond_dim = z_dim + plan_emb_dim + n_masks + n_db_engines + n_hardware + 1
        self.net = nn.Sequential(
            nn.Linear(cond_dim, 256),
            nn.LayerNorm(256),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(256, 128),
            nn.LayerNorm(128),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(128, 64),
            nn.GELU(),
            nn.Linear(64, 1),
        )

    def forward(self, z, plan_emb, masks, db_engine_oh, hardware_oh, ram_gb, domain_ids=None):
        x = torch.cat([z, plan_emb, masks, db_engine_oh, hardware_oh, ram_gb], dim=1)
        return self.net(x).squeeze(1)


print("INFO: Regression head defined")


# ======================== Domain Classifier ========================
class DomainClassifier(nn.Module):
    """Gd: z -> domain prediction (binary: source vs target).
    
    Small MLP to classify whether features come from source or target domain.
    Connected via GRL so the encoder learns to confuse it.
    """
    def __init__(self, z_dim, hidden_dim=128, dropout=0.1):
        super().__init__()
        self.grl = GradientReversalLayer(alpha=1.0)
        self.net = nn.Sequential(
            nn.Linear(z_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.BatchNorm1d(hidden_dim // 2),
            nn.ReLU(),
            nn.Linear(hidden_dim // 2, 1),  # Binary: source vs target
        )

    def forward(self, z, alpha=1.0):
        self.grl.set_alpha(alpha)
        z_rev = self.grl(z)
        return self.net(z_rev).squeeze(1)

print("INFO: Domain classifier defined")


# ======================== DANN Cost Model ========================
class DANNCostModel(nn.Module):
    """Full DANN: SAINT encoder + regression head + domain classifier (GRL).
    
    Three-branch architecture:
    - Gf (SAINT encoder): shared feature extractor -> z
    - Gy (regression head): z -> cost prediction  
    - Gd (domain classifier via GRL): z -> source/target prediction
    
    CORAL alignment is computed externally on z during training.
    """
    def __init__(self, num_features, n_masks, n_db_engines, n_hardware,
                 d_model=128, n_heads=4, n_layers=2, n_domains=4, dropout=0.1):
        super().__init__()
        self.Gf = SAINTEncoder(num_features, d_model, n_heads, n_layers, dropout)
        self.plan_proj = nn.Sequential(
            nn.Linear(384, d_model // 2),
            nn.LayerNorm(d_model // 2),
            nn.GELU()
        )
        self.Gy = RegressionHead(d_model, d_model // 2, n_masks, n_db_engines, n_hardware, dropout, n_domains)
        self.Gd = DomainClassifier(d_model, hidden_dim=128, dropout=dropout)

    def forward(self, x_feat, x_mask, qp_emb, db_engine_oh, hardware_oh, ram_gb, 
                domain_ids=None, alpha_grl=0.0):
        """Forward pass returning cost prediction, latent z, and domain logits.
        
        Args:
            alpha_grl: GRL scaling factor. 0 = no adversarial training.
        """
        z = self.Gf(x_feat)
        p = self.plan_proj(qp_emb)
        cost_pred = self.Gy(z, p, x_mask, db_engine_oh, hardware_oh, ram_gb, domain_ids)
        domain_logits = self.Gd(z, alpha=alpha_grl)
        return cost_pred, z, domain_logits

    @torch.no_grad()
    def predict(self, x_feat, x_mask, qp_emb, db_engine_oh, hardware_oh, ram_gb, domain_ids=None):
        """Inference (no domain classifier needed)"""
        self.eval()
        z = self.Gf(x_feat)
        p = self.plan_proj(qp_emb)
        return self.Gy(z, p, x_mask, db_engine_oh, hardware_oh, ram_gb, domain_ids)

print("INFO: DANNCostModel defined")


