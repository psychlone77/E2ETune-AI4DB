import os
import json
import glob

# Define workloads to process
WORKLOADS = [
    {'data_dir': 'job', 'metrics_subdir': 'job', 'workload_prefix': './olap_workloads/job/'},
]


def load_runhistory(runhistory_path):
    """Load runhistory.json and return list of (config, cost, config_id) tuples."""
    with open(runhistory_path, 'r') as f:
        data = json.load(f)
    
    results = []
    configs = data['configs']
    for run in data['data']:
        conf_id = str(run[0][0])
        cost = run[1][0]
        if conf_id in configs:
            results.append((configs[conf_id], cost, conf_id))
    return results


def collect_offline_samples(base_dir, output_path):
    """Collect all historical data and write to JSONL file."""
    samples = []
    
    for workload in WORKLOADS:
        data_dir = os.path.join(base_dir, workload['data_dir'])
        metrics_dir = os.path.join(base_dir, 'internal_metrics', workload['metrics_subdir'])
        
        if not os.path.exists(data_dir):
            continue
            
        # Find all smac output directories
        for smac_dir in glob.glob(os.path.join(data_dir, '*_smac_output')):
            base_name = os.path.basename(smac_dir).replace('_smac_output', '')
            
            # Find runhistory
            run_dirs = glob.glob(os.path.join(smac_dir, 'run_*'))
            if not run_dirs:
                continue
            runhistory_path = os.path.join(run_dirs[0], 'runhistory.json')
            if not os.path.exists(runhistory_path):
                print(f"Runhistory not found: {runhistory_path}")
                continue
            
            # Load metrics
            metrics_path = os.path.join(metrics_dir, f"{base_name}_internal_metrics.json")
            print(f"Loading metrics from: {metrics_path}")
            inner_metrics = []
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    metrics_dict = json.load(f)
                    inner_metrics = list(metrics_dict.values())
            
            # Build workload path
            workload_path = workload['workload_prefix'] + base_name + '.xml'
            
            # Load all configs and find worst valid cost for capping crashes
            all_configs = load_runhistory(runhistory_path)
            valid_costs = [cost for _, cost, _ in all_configs if cost < 0]
            worst_valid_cost = max(valid_costs) if valid_costs else -1.0  # least negative = worst
            
            # Process all configs from runhistory
            for config, cost, config_id in all_configs:
                # Cap crashed configs (cost >= 0) with worst valid cost
                if cost >= 0:
                    cost = worst_valid_cost
                
                # Negate cost to get positive throughput (higher = better)
                throughput = -cost
                    
                sample = dict(config)  # Flat knob params
                sample['y'] = [throughput, 1.0 / throughput if throughput != 0 else 0.0]
                sample['inner_metrics'] = inner_metrics
                sample['workload'] = workload_path
                sample['config_id'] = config_id
                samples.append(sample)
    
    # Write as JSONL
    with open(output_path, 'w') as f:
        for sample in samples:
            f.write(json.dumps(sample) + '\n')
    
    print(f"Collected {len(samples)} samples to {output_path}")
    return samples


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'collected_samples.jsonl')
    collect_offline_samples(base_dir, output_path)


if __name__ == "__main__":
    main()