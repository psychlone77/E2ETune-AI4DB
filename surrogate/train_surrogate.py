import os
import json
import jsonlines
import numpy as np

from classes.base_classes.Surrogate_Strategy import SurrogateFactory
from classes.Cost_Model import CostModel
from classes.Performance_DataManager import PerformanceAwareDataManager

def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return json.load(f)

def load_jsonl(path):
    samples = []
    if not os.path.exists(path):
        return samples
    with jsonlines.open(path, 'r') as f:
        for record in f:
            samples.append(record)
    return samples

def train_surrogate(database: str):
    """
    Trains the E2ETune CostModel for a given database type (e.g. 'imdb', 'tpch').
    """
    print(f"Initializing surrogate training for {database}...")

    # Paths
    knob_config_path = "knob_config/knob_config.json"
    features_path = f"SuperWG/feature/{database}.json"
    samples_path = f"offline_sample/offline_sample_{database}.jsonl"
    model_save_path = f"surrogate/models/surrogate_{database}"

    # 1. Initialize Strategy and CostModel
    strategy = SurrogateFactory.create_strategy("tree_ensemble")
    cost_model = CostModel(strategy, knob_config_path)

    # 2. Load Workload Features & Training Samples
    workload_features = load_json(features_path)
    samples = load_jsonl(samples_path)

    if not samples:
        print(f"No samples found at {samples_path}. Please collect data first.")
        return

    # Optional: Filter samples using PerformanceAwareDataManager 
    # if clustering results are available
    # data_manager = PerformanceAwareDataManager(f"picked_workloads/{database}.csv")
    # filtered_samples = data_manager.filter_samples(samples)

    # 3. Train
    cost_model.train(samples, workload_features)

    # 4. Save
    os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
    cost_model.save_model(model_save_path)
    print(f"Model saved successfully to {model_save_path}")

    # Optional Testing
    print("Testing predictions on a generic sample...")
    test_knobs = samples[0] # Pick any dictionary from samples
    test_workload_id = test_knobs.get('workload')
    test_vector = workload_features.get(test_workload_id, [0.0]*120)
    
    # We strip 'tps', 'y', 'inner_metrics', 'workload' since these are not knobs
    stripped_knobs = {k:v for k,v in test_knobs.items() if k not in ['tps', 'y', 'inner_metrics', 'workload']}
    predicted_tps = cost_model.predict(stripped_knobs, test_vector)
    
    print(f"Actual TPS: {test_knobs.get('tps')}")
    print(f"Predicted TPS: {predicted_tps:.4f}")

if __name__ == "__main__":
    train_surrogate("imdb")  # Replace with appropriate database flag logic