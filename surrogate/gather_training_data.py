import os
import json
import glob

# Benchmarks to aggregate from the project root (e.g., E2ETune-AI4DB/job, E2ETune-AI4DB/tpch)
BENCHMARK_DIRS = [
    'data/postgresql/hetzner-4c-8t-64gb/job',
    'data/postgresql/hetzner-4c-8t-64gb/ssb',
    'data/postgresql/hetzner-4c-8t-64gb/ssb_flat_tiny',
    'data/postgresql/hetzner-4c-8t-64gb/tpcds',
    'data/postgresql/hetzner-4c-8t-64gb/tpch',
    'data/postgresql/hetzner-4c-8t-64gb/twitter',
    'data/postgresql/hetzner-4c-8t-64gb/ycsb',
]


def read_jsonl_records(path):
    """Read a JSONL file and return a list of parsed objects."""
    records = []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Attach source metadata for traceability
                obj['__source'] = path
                records.append(obj)
            except json.JSONDecodeError:
                # Skip malformed lines but continue
                continue
    return records


def combine_workload_data(base_dir, output_path):
    """Combine run_history.jsonl and collected_data.json from each workload into one JSON file."""
    all_workloads = {}
    total_run_history_records = 0
    total_collected_data_records = 0
    
    for bench in BENCHMARK_DIRS:
        bench_dir = os.path.join(base_dir, bench)
        if not os.path.isdir(bench_dir):
            print(f"Warning: Benchmark directory not found: {bench_dir}")
            continue
        
        # Look for workload directories e.g., job_0, job_1...
        for workload_dir in os.listdir(bench_dir):
            full_dir = os.path.join(bench_dir, workload_dir)
            if not os.path.isdir(full_dir):
                continue
                
            run_history_path = os.path.join(full_dir, 'run_history.jsonl')
            collected_data_path = os.path.join(full_dir, 'collected_data.json')
            
            workload_data = {}
            if os.path.exists(collected_data_path):
                try:
                    with open(collected_data_path, 'r') as f:
                        workload_data['collected_data'] = json.load(f)
                    total_collected_data_records += 1
                except Exception as e:
                    print(f"Error reading {collected_data_path}: {e}")
                    
            if os.path.exists(run_history_path):
                records = read_jsonl_records(run_history_path)
                workload_data['run_history'] = records
                total_run_history_records += len(records)
                
            if workload_data:
                # Store it under the workload name
                all_workloads[workload_dir] = workload_data
                
    # Write a single JSON file
    with open(output_path, 'w') as out:
        json.dump({
            'workload_count': len(all_workloads),
            'total_run_history_records': total_run_history_records,
            'total_collected_data_records': total_collected_data_records,
            'workloads': all_workloads,
        }, out)
    print(f"Combined {len(all_workloads)} workloads -> {output_path}")
    return all_workloads


def main():
    # Project root (E2ETune-AI4DB)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Output combined JSON in the same folder as this script
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'combined_training_data_v2.json')
    combine_workload_data(base_dir, output_path)


if __name__ == "__main__":
    main()