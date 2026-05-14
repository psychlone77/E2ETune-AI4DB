import os
import json
import csv
from pathlib import Path

# Paths to the input data directories
RESULTS_DIR = Path("results/diverse_configs")
DATA_DIR = Path("data")

# Output CSV targets
DIVERSE_CONFIGS_CSV = "diverse_configs_data.csv"
DEFAULT_DATA_CSV = "default_data.csv"

def export_diverse_configs():
    print(f"Exporting diverse configs to {DIVERSE_CONFIGS_CSV}...")
    with open(DIVERSE_CONFIGS_CSV, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["server_name", "db_engine", "benchmark", "workload_name", "config", "performance"])
        
        if not RESULTS_DIR.exists():
            print(f"Warning: {RESULTS_DIR} does not exist.")
            return

        for server_dir in RESULTS_DIR.iterdir():
            if not server_dir.is_dir(): continue
            server_name = server_dir.name
            
            for db_dir in server_dir.iterdir():
                if not db_dir.is_dir(): continue
                db_engine = db_dir.name
                
                for benchmark_dir in db_dir.iterdir():
                    if not benchmark_dir.is_dir(): continue
                    benchmark = benchmark_dir.name
                    
                    for workload_dir in benchmark_dir.iterdir():
                        if not workload_dir.is_dir() or workload_dir.name == "logs": continue
                        workload_name = workload_dir.name
                        
                        history_file = workload_dir / "run_history.jsonl"
                        if history_file.exists():
                            with open(history_file, "r") as hf:
                                for line in hf:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    try:
                                        data = json.loads(line)
                                        config = json.dumps(data.get("config", {}))
                                        perf = data.get("performance", None)
                                        writer.writerow([server_name, db_engine, benchmark, workload_name, config, perf])
                                    except json.JSONDecodeError:
                                        pass

def export_default_data():
    print(f"Exporting default data to {DEFAULT_DATA_CSV}...")
    with open(DEFAULT_DATA_CSV, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["server_name", "db_engine", "benchmark", "workload_name", "internal_metrics", "query_plans", "workload_features"])
        
        if not DATA_DIR.exists():
            print(f"Warning: {DATA_DIR} does not exist.")
            return

        # Expecting directory structure: data/<db_engine>/<server_name>/<benchmark>/<workload_name>/collected_data.json
        for db_dir in DATA_DIR.iterdir():
            if not db_dir.is_dir(): continue
            
            # Since only database engine directories should contain server structures
            if db_dir.name not in ["mysql", "postgresql", "benchbase", "mariadb", "noisepage"]:
                continue
                
            db_engine = db_dir.name
            
            for server_dir in db_dir.iterdir():
                if not server_dir.is_dir(): continue
                server_name = server_dir.name
                
                for benchmark_dir in server_dir.iterdir():
                    if not benchmark_dir.is_dir(): continue
                    benchmark = benchmark_dir.name
                    
                    for workload_dir in benchmark_dir.iterdir():
                        if not workload_dir.is_dir() or workload_dir.name == "logs": continue
                        workload_name = workload_dir.name
                        
                        data_file = workload_dir / "collected_data.json"
                        if data_file.exists():
                            with open(data_file, "r") as df:
                                try:
                                    data = json.load(df)
                                    internal_metrics = json.dumps(data.get("internal_metrics", {}))
                                    unique_query_plans = list(dict.fromkeys(data.get("query_plans", [])))
                                    query_plans = json.dumps(unique_query_plans)
                                    workload_features = json.dumps(data.get("workload_features", {}))
                                    
                                    writer.writerow([
                                        server_name, 
                                        db_engine, 
                                        benchmark, 
                                        workload_name, 
                                        internal_metrics, 
                                        query_plans, 
                                        workload_features
                                    ])
                                except json.JSONDecodeError:
                                    pass

if __name__ == "__main__":
    export_diverse_configs()
    export_default_data()
    print("Export complete!")
