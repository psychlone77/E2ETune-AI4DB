import json
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
import argparse
import os

from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.base_classes.Script_Config import ScriptConfig
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Knob_Config import KnobConfig
from classes.WFE_OLAP import WorkloadFeatureExtractorOLAP
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

class ClusterEvaluator:
    def __init__(self, knob_config_path, clusters_path, config_yaml="config/config.yaml"):
        self.knob_config_path = knob_config_path
        self.clusters_path = clusters_path
        self.knobs = self._load_json(knob_config_path)
        self.clusters = self._load_json(clusters_path)
        self.results_df = pd.DataFrame(columns=['Cluster_ID', 'Workload_ID', 'Config_ID', 'Latency'])
        
        # We still load the script config to get the workload base path
        self.script_config = ScriptConfig.from_yaml_file(config_yaml)
        self.workload_base_path = Path(self.script_config.benchmark_config.path)
        
    def _load_json(self, path):
        with open(path, 'r') as f:
            return json.load(f)


    def analyze_cluster_cohesion(self):
        """Calculate the Cluster Cohesion Score using workload features."""
        wfe = WorkloadFeatureExtractorOLAP()
        cohesion_scores = {}
        
        for benchmark, bench_clusters in self.clusters.items():
            for cluster_id, workloads in bench_clusters.items():
                cluster_name = f"{benchmark}_{cluster_id}"
                
                features_list = []
                for w in workloads:
                    w_path = self.workload_base_path / w
                    if w_path.exists():
                        try:
                            f = wfe.extract(w_path)
                            features_list.append(f)
                        except Exception as e:
                            print(f"Failed to extract features for {w}: {e}")
                
                N = len(features_list)
                if N < 2:
                    cohesion_scores[cluster_name] = 1.0 # Cannot compute properly for < 2
                    continue
                    
                # 1. Parse and Flatten Input Data
                flattened = []
                for f in features_list:
                    flat = {}
                    for k, v in f.items():
                        if k == 'read_write_ratio':
                            continue
                        if isinstance(v, dict):
                            for sub_k, sub_v in v.items():
                                flat[f"{k}_{sub_k}"] = sub_v
                        else:
                            flat[k] = v
                    flattened.append(flat)
                    
                # 2. Vectorize Features & Handling Nulls
                df_features = pd.DataFrame(flattened).fillna(0)
                
                # 3. Normalize Feature Matrix
                # We use StandardScaler to ensure features don't artificially dominate
                scaler = MinMaxScaler()
                X_scaled = scaler.fit_transform(df_features)
                
                # 4. Calculate Pairwise Cosine Similarity
                sim_matrix = cosine_similarity(X_scaled)
                
                # Extract upper triangle 
                indices = np.triu_indices(N, k=1)
                upper_tri_sims = sim_matrix[indices]
                
                # 5. Aggregate Cluster Cohesion Score
                if len(upper_tri_sims) > 0:
                    c_score = np.mean(upper_tri_sims)
                else:
                    c_score = 1.0
                    
                cohesion_scores[cluster_name] = c_score
                
        print("\n--- Structural Cluster Cohesion Scores ---")
        for c, score in cohesion_scores.items():
            print(f"Cluster {c}: {score:.3f}")
        return cohesion_scores

    def generate_report(self):
        print("\n" + "="*50)
        print("B E N C H M A R K   S U M M A R Y   R E P O R T")
        print("="*50)
        
        self.analyze_cluster_cohesion()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Workload Clustering")
    parser.add_argument("--knobs", default="knob_config/knob_config.json", help="Path to knob_config.json")
    parser.add_argument("--clusters", default="clustering/workload_clusters3.json", help="Path to workload_clusters.json")
    parser.add_argument("--configs", type=int, default=10, help="Number of random configs to test")
    parser.add_argument("--samples", type=int, default=5, help="Number of workloads to sample per cluster")
    parser.add_argument("--workload", type=str, help="Specific workload ID to run (e.g. 'job_1.wg')")
    parser.add_argument("--benchmark", type=str, help="Specific benchmark to run (e.g. 'job')")
    
    args = parser.parse_args()
    
    evaluator = ClusterEvaluator(args.knobs, args.clusters)

    evaluator.generate_report()
