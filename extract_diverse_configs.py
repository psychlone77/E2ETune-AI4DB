import json
import os
import random

def main():
    base_path = r"c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\data\postgresql\hetzner-4c-8t-64gb"
    json_path = r"c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\representative_workloads_sampled.json"
    output_path = r"c:\Users\madus\Desktop\FYP\E2ETune-AI4DB\10_diverse_configs_all.json"

    # Load the sampled representative workloads
    with open(json_path, 'r', encoding='utf-8') as f:
        sampled_workloads = json.load(f)

    all_diverse_configs = {}
    global_pool = []

    # PHASE 1: Initial Extraction from strictly 64gb base path
    for category, clusters in sampled_workloads.items():
        if category not in all_diverse_configs:
            all_diverse_configs[category] = {}
            
        for cluster_name, workloads in clusters.items():
            for workload_file in workloads:
                workload_name = os.path.splitext(workload_file)[0]
                
                # Make sure every expected workload has an entry initialized
                if workload_name not in all_diverse_configs[category]:
                    all_diverse_configs[category][workload_name] = []
                
                run_history_file = os.path.join(base_path, category, workload_name, "run_history.jsonl")
                
                if not os.path.exists(run_history_file):
                    continue
                    
                entries = []
                with open(run_history_file, 'r', encoding='utf-8') as rf:
                    for line in rf:
                        if not line.strip():
                            continue
                        try:
                            entry = json.loads(line)
                            entries.append(entry)
                        except json.JSONDecodeError:
                            pass
                            
                if not entries:
                    continue

                # Sort the loaded configurations sequentially by their cost
                entries.sort(key=lambda x: x.get("cost", float('inf')))
                
                N = len(entries)
                selected_configs = []
                
                if N <= 10:
                    selected_configs = [entry.get("config", {}) for entry in entries]
                else:
                    # Select exactly 10 configurations evenly spaced across the sorted array
                    indices = [i * (N - 1) // 9 for i in range(10)]
                    selected_configs = [entries[idx].get("config", {}) for idx in indices]
                    
                all_diverse_configs[category][workload_name] = selected_configs
                # Add all successfully extracted configurations to master global pool
                global_pool.extend(selected_configs)

    # PHASE 2: Gap Filling (Random Substitution)
    # Applying random.seed() guarantees repeatable pseudo-random selection during repeats
    random.seed(42) 
    
    for category, workloads in all_diverse_configs.items():
        for workload_name, configs in workloads.items():
            shortfall = 10 - len(configs)
            if shortfall > 0:
                # Randomly sample exactly `shortfall` amount of configs from global_pool
                if global_pool:
                    fillers = random.choices(global_pool, k=shortfall)
                    configs.extend(fillers)
                else:
                    print("Error: Global pool is fundamentally empty! Cannot substitute configurations.")

    # PHASE 3: Write out aggregated 1,200 dict uniformly
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(all_diverse_configs, out_f, indent=2)

    total_configs = sum(len(configs) for category_data in all_diverse_configs.values() for configs in category_data.values())
    print(f"Extraction complete! Final total configs: {total_configs} exactly mapping original expectation.")
    print(f"Saved aggregated output to {output_path}")

if __name__ == "__main__":
    main()
