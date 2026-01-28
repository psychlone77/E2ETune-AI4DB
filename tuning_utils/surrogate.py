import joblib
import json
import os
import re
import numpy as np

class Surrogate:
    def __init__(self, sur_config, workload_path) -> None:
        # Simplified config: only model_path is required
        self.model = joblib.load(sur_config['model_path'])
        self.workload_path = workload_path
        
        # Feature names in the exact order expected by the model (65 features)
        self.feature_names = [
            'cfg__shared_buffers', 'cfg__work_mem', 'cfg__maintenance_work_mem', 
            'cfg__effective_cache_size', 'cfg__max_connections', 'cfg__wal_buffers', 
            'cfg__checkpoint_completion_target', 'cfg__checkpoint_timeout', 
            'cfg__effective_io_concurrency', 'cfg__join_collapse_limit',
            'metrics__xact_commit', 'metrics__xact_rollback', 'metrics__blks_read', 
            'metrics__blks_hit', 'metrics__tup_returned', 'metrics__tup_fetched', 
            'metrics__tup_inserted', 'metrics__conflicts', 'metrics__tup_updated', 
            'metrics__tup_deleted', 'metrics__disk_read_count', 'metrics__disk_write_count', 
            'metrics__disk_read_bytes', 'metrics__disk_write_bytes', 'metrics__temp_bytes',
            'workload__size', 'workload__read_ratio', 'workload__group_by_ratio', 
            'workload__order_by_ratio', 'workload__avg_query_length', 'workload__avg_joins', 
            'workload__filter_ratio', 'workload__complexity_score',
            'plan__count__aggregate', 'plan__count__bitmap_heap_scan', 'plan__count__bitmap_index_scan', 
            'plan__count__gather', 'plan__count__gather_merge', 'plan__count__hash', 
            'plan__count__hash_join', 'plan__count__index_only_scan', 'plan__count__index_scan', 
            'plan__count__limit', 'plan__count__materialize', 'plan__count__merge_join', 
            'plan__count__nested_loop', 'plan__count__seq_scan', 'plan__count__sort', 
            'plan__count__subquery_scan',
            'plan__ratio__aggregate', 'plan__ratio__bitmap_heap_scan', 'plan__ratio__bitmap_index_scan', 
            'plan__ratio__gather', 'plan__ratio__gather_merge', 'plan__ratio__hash', 
            'plan__ratio__hash_join', 'plan__ratio__index_only_scan', 'plan__ratio__index_scan', 
            'plan__ratio__limit', 'plan__ratio__materialize', 'plan__ratio__merge_join', 
            'plan__ratio__nested_loop', 'plan__ratio__seq_scan', 'plan__ratio__sort', 
            'plan__ratio__subquery_scan'
        ]
        
        self.op_re = re.compile(r'([A-Za-z ]+)\(cost=([0-9.]+)\)')

    def _vectorize_plans(self, plan_strings):
        op_counts = {}
        for s in plan_strings or []:
            ops = self.op_re.findall(s)
            for name, _ in ops:
                key = name.strip().lower().replace(' ', '_')
                op_counts[key] = op_counts.get(key, 0) + 1
        
        total_ops = sum(op_counts.values()) or 1
        
        # Expected operators for the 65-feature model
        expected_ops = [
            'aggregate', 'bitmap_heap_scan', 'bitmap_index_scan', 'gather', 
            'gather_merge', 'hash', 'hash_join', 'index_only_scan', 
            'index_scan', 'limit', 'materialize', 'merge_join', 
            'nested_loop', 'seq_scan', 'sort', 'subquery_scan'
        ]
        
        feats = {}
        for op in expected_ops:
            count = op_counts.get(op, 0)
            feats[f'plan__count__{op}'] = float(count)
            feats[f'plan__ratio__{op}'] = float(count / total_ops)
        return feats

    def _load_workload_features(self):
        # Extract bench and idx from workload_path
        # Example: /home/ubuntu/.../job/job_0_features.json
        try:
            filename = os.path.basename(self.workload_path)
            # Match patterns like job_0, tpch_1, etc.
            match = re.search(r'([a-z]+)_(\d+)', filename)
            if match:
                bench, idx = match.groups()
                # Construct path to features.json
                # Assuming the structure is project_root/workload_features/bench/bench_idx_features.json
                # We try to find the project root by going up from workload_path
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(self.workload_path)))
                feat_path = os.path.join(project_root, 'workload_features', bench, f"{bench}_{idx}_features.json")
                
                if os.path.exists(feat_path):
                    with open(feat_path, 'r') as f:
                        return json.load(f)
        except Exception:
            pass
        return {}

    def _load_query_plans(self):
        try:
            filename = os.path.basename(self.workload_path)
            match = re.search(r'([a-z]+)_(\d+)', filename)
            if match:
                bench, idx = match.groups()
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(self.workload_path)))
                plan_path = os.path.join(project_root, 'query_plans', bench, f"{bench}_{idx}_plans.json")
                
                if os.path.exists(plan_path):
                    with open(plan_path, 'r') as f:
                        data = json.load(f)
                        return data.get('query_plans', [])
        except Exception:
            pass
        return []

    def run(self, inner_metrics_dict, normalized_knobs_dict):
        # 1. Prepare features
        row = {}
        
        # Config features (normalized)
        for k, v in normalized_knobs_dict.items():
            row[f'cfg__{k}'] = v
            
        # Metrics features
        for k, v in inner_metrics_dict.items():
            row[f'metrics__{k}'] = v
            
        # Workload features
        workload_feats = self._load_workload_features()
        for k, v in workload_feats.items():
            row[f'workload__{k}'] = v
            
        # Plan features
        plans = self._load_query_plans()
        plan_feats = self._vectorize_plans(plans)
        row.update(plan_feats)
        
        # 2. Align with model's expected feature order
        x = []
        for name in self.feature_names:
            x.append(float(row.get(name, 0.0)))
            
        # 3. Predict
        # The model was trained on log1p(cost) (target 'y_log')
        prediction_log = self.model.predict([x])[0]
        prediction = np.expm1(prediction_log)
        
        return float(prediction)
