import argparse
import json
from pathlib import Path
import sys
import os

# Add parent directory to python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from classes.base_classes.Script_Config import ScriptConfig
from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.MySQL_Database import MySQLDatabase
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Workload_Runner import BenchmarkTask

def main():
    parser = argparse.ArgumentParser(description="Evaluate best config vs default config for a workload")
    parser.add_argument("sql_file", type=str, help="Path to the input SQL file")
    parser.add_argument("--best_config", type=str, default=None, 
                        help="Path to best_config.json (defaults to best_config.json in the same folder as sql_file)")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="Path to overall config.yaml")
    parser.add_argument("--knob_config", type=str, default="knob_config/mysql64_knob_config_op.json", help="Path to knob settings JSON file")
    parser.add_argument("--output", type=str, default=None, 
                        help="Path to save results.json (defaults to results.json in the same folder as sql_file)")
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    if not sql_path.is_absolute():
        sql_path = Path(os.getcwd()) / sql_path

    if not sql_path.exists():
        print(f"Error: SQL file '{sql_path}' does not exist.")
        sys.exit(1)

    # Determine paths and directories
    workload_dir = sql_path.parent
    
    best_config_path = Path(args.best_config) if args.best_config else workload_dir / "best_config.json"
    if not best_config_path.is_absolute() and args.best_config:
        best_config_path = Path(os.getcwd()) / best_config_path
        
    if not best_config_path.exists():
        print(f"Error: Best config file '{best_config_path}' does not exist.")
        sys.exit(1)
        
    output_path = Path(args.output) if args.output else workload_dir / "results.json"
    if not output_path.is_absolute() and args.output:
        output_path = Path(os.getcwd()) / output_path

    config_path = parent_dir / args.config
    script_config = ScriptConfig.from_yaml_file(str(config_path))

    # Initialize db
    print("Connecting to the database...")
    try:
        # Simple heuristic to choose database implementation
        if script_config.database_config.port == 3306 or "mysql" in getattr(script_config.database_config, "data_path", "").lower():
            db = MySQLDatabase(script_config.database_config)
        else:
            db = PostgresSQLDatabase(script_config.database_config)
        db.connect()
    except Exception as e:
        print(f"Error: Failed to connect to database. Details: {e}")
        sys.exit(1)

    # Load knob settings structure
    knob_settings_path = parent_dir / args.knob_config
    if not knob_settings_path.exists():
        print(f"Error: Knob config file '{knob_settings_path}' not found.")
        sys.exit(1)
    
    print("Loading configurations...")
    knob_settings = KnobSettingsSet.from_json_file(str(knob_settings_path))

    # 1. Prepare default knob config
    default_knob_config = knob_settings.get_default_knob_settings()

    # 2. Prepare best knob config from JSON file
    with open(best_config_path, "r") as f:
        best_config_dict = json.load(f)
    
    # Assuming best_config_dict might directly map to knob values, handle depending on format
    # In some cases, best_config values may be nested depending on the output
    # E.g., if it has an inner dictionary or wrapper, unwrap it (if necessary)
    if len(best_config_dict.keys()) == 1 and isinstance(list(best_config_dict.values())[0], dict):
        best_config_dict = list(best_config_dict.values())[0]

    best_knob_config = KnobConfig.from_dict(best_config_dict, knob_settings)

    # Note: run_workload applies the config behind the scenes
    runs_per_iteration = 1
    
    print("\n--- Running with DEFAULT configuration ---")
    default_task = BenchmarkTask(workload_path=sql_path, knob_config=default_knob_config)
    try:
        def_latency, def_neg_throughput = db.run_workload(default_task, runs_per_iteration=runs_per_iteration)
        def_throughput = -def_neg_throughput
        print(f"Default Latency (s): {def_latency:.3f}, Default Throughput (ops/s): {def_throughput:.3f}")
    except Exception as e:
        print(f"Error: Workload run with DEFAULT config failed: {e}")
        def_latency, def_throughput = 0.0, 0.0

    print("\n--- Running with BEST configuration ---")
    best_task = BenchmarkTask(workload_path=sql_path, knob_config=best_knob_config)
    try:
        best_latency, best_neg_throughput = db.run_workload(best_task, runs_per_iteration=runs_per_iteration)
        best_throughput = -best_neg_throughput
        print(f"Best Latency (s): {best_latency:.3f}, Best Throughput (ops/s): {best_throughput:.3f}")
    except Exception as e:
        print(f"Error: Workload run with BEST config failed: {e}")
        best_latency, best_throughput = 0.0, 0.0

    # Calculate improvements
    latency_improvement_pct = 0.0
    if def_latency > 0:
        latency_improvement_pct = ((def_latency - best_latency) / def_latency) * 100.0

    throughput_improvement_pct = 0.0
    if def_throughput > 0:
        throughput_improvement_pct = ((best_throughput - def_throughput) / def_throughput) * 100.0

    print(f"\n--- Results summary ---")
    print(f"Latency Improvement: {latency_improvement_pct:.2f}%")
    print(f"Throughput Improvement: {throughput_improvement_pct:.2f}%")

    results = {
        "workload": str(sql_path),
        "default_performance": {
            "latency_s": def_latency,
            "throughput_ops_per_s": def_throughput
        },
        "best_performance": {
            "latency_s": best_latency,
            "throughput_ops_per_s": best_throughput
        },
        "improvements": {
            "latency_improvement_percentage": latency_improvement_pct,
            "throughput_improvement_percentage": throughput_improvement_pct
        }
    }

    with open(output_path, "w") as f:
        json.dump(results, f, indent=4)
        
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    main()
