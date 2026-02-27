import json
import joblib
import numpy as np
from typing import List, Dict, Any

from sklearn.preprocessing import StandardScaler

from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask, WorkloadRunner
from classes.base_classes.Surrogate_Strategy import SurrogateStrategy


class CostModel(WorkloadRunner):
    """
    The Cost Model (Surrogate Model) for E2ETune.
    Responsible for predicting database performance (TPS/Latency) given 
    knob configurations and workload features.
    It implements the WorkloadRunner interface to act as a physical Database drop-in replacement.
    """
    
    def __init__(self, strategy: SurrogateStrategy, knob_settings: KnobSettingsSet):
        self.strategy = strategy
        self.knob_settings = knob_settings
        self.scaler = StandardScaler()
        self.is_trained = False
        self.workload_features = {}
        
    @staticmethod
    def _load_json(path: str) -> Dict:
        with open(path, 'r') as f:
            return json.load(f)

    def preprocess_knobs(self, knob_values: Dict[str, Any]) -> List[float]:
        """Normalizes knob values based on their min/max ranges."""
        normalized_x = []
        for key, value in knob_values.items():
            if key in self.knob_config:
                cfg = self.knob_config[key]
                denom = cfg.get('max', 0) - cfg.get('min', 0)
                if denom != 0:
                    normalized_x.append((value - cfg['min']) / denom)
                else:
                    normalized_x.append(0.0)
        return normalized_x

    def prepare_training_data(self, samples: List[Dict], workload_features: Dict[str, List[float]]):
        """
        Combines normalized knobs and workload features into a training matrix.
        """
        x, y = [], []
        for sample in samples:
            # 1. Extract and normalize knobs
            knob_part = self.preprocess_knobs(sample)
            
            # 2. Get workload features (pre-calculated via clustering/extraction)
            workload_id = sample.get('workload')
            feature_part = workload_features.get(workload_id, [])
            
            # 3. Combine parts
            full_vector = knob_part + feature_part
            x.append(full_vector)
            y.append(sample.get('tps', 0.0))
            
        return np.array(x), np.array(y)

    def train(self, samples: List[Dict], workload_features: Dict[str, List[float]]):
        """Trains the underlying surrogate strategy."""
        x, y = self.prepare_training_data(samples, workload_features)
        
        # Scale features for better performance (especially for non-tree models)
        x_scaled = self.scaler.fit_transform(x)
        
        print(f"Training cost model on {len(x)} samples...")
        self.strategy.train(x_scaled, y)
        self.is_trained = True
        
    def predict(self, knob_values: Dict[str, Any], workload_feature_vector: List[float]) -> float:
        """Predicts performance for a given configuration and workload."""
        if not self.is_trained:
            raise RuntimeError("Cost model must be trained or loaded before prediction.")
            
        knob_vector = self.preprocess_knobs(knob_values)
        full_vector = np.array([knob_vector + workload_feature_vector])
        
        # Apply the same scaling used during training
        full_vector_scaled = self.scaler.transform(full_vector)
        
        prediction = self.strategy.predict(full_vector_scaled)
        return float(prediction[0])

    def save_model(self, path: str):
        """Persists the model and the scaler."""
        model_data = {
            'strategy_path': f"{path}.strategy",
            'scaler': self.scaler,
            'is_trained': self.is_trained
        }
        self.strategy.save(model_data['strategy_path'])
        joblib.dump(model_data, f"{path}.meta")
        
    def load_model(self, path: str):
        """Loads a persisted model and its state."""
        meta = joblib.load(f"{path}.meta")
        self.strategy.load(meta['strategy_path'])
        self.scaler = meta['scaler']
        self.is_trained = meta['is_trained']

    def run_workload(self, workload_task: BenchmarkTask) -> tuple[float, float]:
        """
        Run the workload via the Cost Model to return a performance tuple.
        Returns:
            A tuple containing the [-average_latency, throughput_ps] performance metrics.
        """
        # 1. Map workload_path to workload_id to get its feature vector
        workload_id = str(workload_task.workload_path.stem)
        workload_feature_vector = self.workload_features.get(workload_id, [])

        if not workload_feature_vector:
            # Fallback for empty vector if clustering mapping isn't loaded
            # Use a zero vector of size 120 (based on previous dimensionality mentioned)
            workload_feature_vector = [0.0] * 120

        # 2. Extract knobs from KnobConfig
        knob_values = workload_task.knob_config.to_dict()

        # 3. Predict tps (assuming prediction is TPS)
        tps = self.predict(knob_values, workload_feature_vector)
        
        # 4. Return tuple (latency, -throughput)
        # Set a dummy latency if optimizing for throughput
        latency = (1.0 / tps) if tps > 0 else 0
        return latency, -tps
