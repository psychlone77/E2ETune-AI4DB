import json
import joblib
import os
import pandas as pd
import numpy as np
import sys
import torch
import torch.nn as nn
from typing import List, Dict, Any

from sklearn.preprocessing import StandardScaler, MinMaxScaler

from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask, WorkloadRunner
from classes.base_classes.Surrogate_Strategy import SurrogateStrategy

# Create a "fake" module entry for numpy._core
if not hasattr(np, "_core"):
    sys.modules["numpy._core"] = np.core

# 1. Redefine the exact architecture from training
class DualStreamCostNN(nn.Module):
    def __init__(self, num_knobs, num_context, hidden_dim=128, dropout=0.2):
        super(DualStreamCostNN, self).__init__()
        
        self.context_stream = nn.Sequential(
            nn.Linear(num_context, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.Mish(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim)
        )
        
        self.knob_stream = nn.Sequential(
            nn.Linear(num_knobs, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.Mish(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim)
        )
        
        self.fusion_net = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.Mish(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.Mish(),
            nn.Linear(hidden_dim // 2, 1)
        )

    def forward(self, x_knobs, x_context):
        ctx_emb = self.context_stream(x_context)
        knob_emb = self.knob_stream(x_knobs)
        fused = torch.cat([ctx_emb, knob_emb], dim=1)
        return self.fusion_net(fused)

# 2. Define the Inference Predictor Class
class CostModelPredictor:
    def __init__(self, save_dir="surrogate"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # Load columns
        with open(os.path.join(save_dir, "feature_columns.json"), "r") as f:
            self.feature_columns = json.load(f)
        
        # Identify column splits for the dual-stream
        self.knob_cols = [c for c in self.feature_columns if c.startswith('knob_')]
        self.context_cols = [c for c in self.feature_columns if not c.startswith('knob_')]
        self.cols_to_minmax = [c for c in self.feature_columns if c.startswith('knob_') or c.startswith('im_')]
        self.other_cols = [c for c in self.feature_columns if not c.startswith('knob_') and not c.startswith('im_')]
        
        # Load scalers from JSON
        with open(os.path.join(save_dir, "scalers_config.json"), "r") as f:
            scaler_data = json.load(f)
        
        # Rehydrate the MinMaxScaler
        self.minmax_scaler = MinMaxScaler()
        self.minmax_scaler.min_ = np.array(scaler_data["minmax"]["min"])
        self.minmax_scaler.scale_ = np.array(scaler_data["minmax"]["scale"])
        self.minmax_scaler.data_min_ = np.array(scaler_data["minmax"]["data_min"])
        self.minmax_scaler.data_max_ = np.array(scaler_data["minmax"]["data_max"])
        
        # Rehydrate the StandardScaler
        self.std_scaler = StandardScaler()
        self.std_scaler.mean_ = np.array(scaler_data["std"]["mean"])
        self.std_scaler.scale_ = np.array(scaler_data["std"]["scale"])
        self.std_scaler.var_ = np.array(scaler_data["std"]["var"])
        
        # Initialize and load model weights
        self.model = DualStreamCostNN(
            num_knobs=len(self.knob_cols), 
            num_context=len(self.context_cols), 
            hidden_dim=256
        )
        model_path = os.path.join(save_dir, "cost_prediction_model.pth")
        
        # map_location ensures it loads correctly even if moving from GPU to CPU
        self.model.load_state_dict(torch.load(model_path, map_location=self.device, weights_only=True))
        self.model.to(self.device)
        self.model.eval() # CRITICAL: Sets Dropout and BatchNorm to evaluation mode

    def preprocess(self, raw_data_list):
        """Converts raw dictionary inputs into scaled PyTorch tensors."""
        df = pd.DataFrame(raw_data_list)
        
        # 1. Align features to training structure (add missing cols as 0)
        for col in self.feature_columns:
            if col not in df.columns:
                df[col] = 0.0
                
        # 2. Enforce exact column order
        df = df[self.feature_columns]
        
        # 3. Apply Scalers
        if self.cols_to_minmax:
            df[self.cols_to_minmax] = self.minmax_scaler.transform(df[self.cols_to_minmax])
            
        if self.other_cols:
            # Fixed a potential bug from user code: df[other_cols] to df[self.other_cols]
            df[self.other_cols] = self.std_scaler.transform(df[self.other_cols])
            
        # 4. Split into Dual-Stream Tensors
        knob_idx = [df.columns.get_loc(c) for c in self.knob_cols]
        ctx_idx = [df.columns.get_loc(c) for c in self.context_cols]
        
        x_knobs = torch.tensor(df.iloc[:, knob_idx].values, dtype=torch.float32).to(self.device)
        x_context = torch.tensor(df.iloc[:, ctx_idx].values, dtype=torch.float32).to(self.device)
        
        return x_knobs, x_context, df

    def predict(self, raw_data_list):
        """Returns the predicted cost for a list of configurations."""
        if not raw_data_list:
            return []
            
        x_knobs, x_context, _ = self.preprocess(raw_data_list)
        
        with torch.no_grad():
            log_preds = self.model(x_knobs, x_context).cpu().numpy().flatten()
            
        # CRITICAL: We applied np.log1p during training. 
        # We must apply np.expm1 (exponential minus 1) to convert back to raw execution cost!
        raw_preds = np.expm1(log_preds)
        
        return raw_preds

class CostModel(WorkloadRunner):
    """
    The Cost Model (Surrogate Model) for E2ETune.
    Responsible for predicting database performance (TPS/Latency) given 
    knob configurations and workload features.
    It implements the WorkloadRunner interface to act as a physical Database drop-in replacement.
    """
    
    def __init__(self, knob_settings: KnobSettingsSet, strategy: SurrogateStrategy = None, save_dir: str = "surrogate"):
        self.knob_settings = knob_settings
        self.predictor = CostModelPredictor(save_dir=save_dir)
        self.workload_features = {}
        self.is_trained = True

    def load_model(self, path: str):
        """Loads a persisted model.
        In the new version, the CostModelPredictor initializes everything from save_dir directly.
        """
        save_dir = os.path.dirname(path) if os.path.dirname(path) else "surrogate"
        self.predictor = CostModelPredictor(save_dir=save_dir)
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
        workload_id = str(workload_task.workload_path.stem)
        
        # We assume workload_features mapping might contain dictionaries for each workload
        workload_context_features = self.workload_features.get(workload_id, {})
        
        # If it's empty, we might just pass an empty dict and let preprocessing fill 0s
        if not isinstance(workload_context_features, dict):
            # Fallback if old format was a list of floats, or something else
            workload_context_features = {}

        # 2. Extract knobs from KnobConfig and add 'knob_' prefix
        knob_values = workload_task.knob_config.to_dict()
        prefixed_knobs = {f"knob_{k}": v for k, v in knob_values.items()}
        
        # 3. Combine context and knobs
        combined_features = {**workload_context_features, **prefixed_knobs}

        # 4. Predict cost (which is effectively latency)
        predicted_cost = self.predictor.predict([combined_features])[0]
        
        # 5. Return tuple (latency, -throughput)
        # Cost is directly modeling elapsed time (latency)
        latency = float(predicted_cost)
        throughput = (1.0 / latency) if latency > 0 else 0
        
        return latency, -throughput
