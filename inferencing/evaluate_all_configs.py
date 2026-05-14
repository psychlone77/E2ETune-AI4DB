import argparse
import json
from pathlib import Path
import sys
import os
import re
import concurrent.futures
import math

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

def parse_percentage(val_str):
    if not isinstance(val_str, str):
        return None
    if val_str == "middle":
        return 0.50
    elif val_str == "low":
        return 0.165
    elif val_str == "high":
        return 0.835
    
    match = re.match(r"(\d+)%\s+to\s+(\d+)%", val_str)
    # match = re.match(r"(\d+)-(\d+)%", val_str)

    if match:
        return (float(match.group(1)) + float(match.group(2))) / 200.0
    return None

def convert_config(config_dict, knob_settings):
    actual_config = {}
    for name, value_str in config_dict.items():
        try:
            knob_setting = knob_settings.get_knob(name)
        except KeyError:
            continue
            
        pct = parse_percentage(value_str)
        if pct is None:
            return None
            
        val = knob_setting.min + (knob_setting.max - knob_setting.min) * pct
        
        if knob_setting.type == "integer":
            val = int(round(val))
        else:
            val = float(val)
            
        actual_config[name] = val
    return actual_config

def load_existing_results(output_path):
    """Load existing results if they exist."""
    if output_path.exists():
        try:
            with open(output_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load existing results: {e}")
    return None

def should_skip_config(config_perf):
    """Check if a config has valid (non-Infinity) results."""
    v = config_perf.get("latency_s")
    try:
        fv = float(v)
    except Exception:
        return False
    return math.isfinite(fv)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql_file",  type=str, default="mysql/ssb/ssb.sql")
    parser.add_argument("--configs_json", type=str, default="/home/E2ETune-AI4DB/inferencing/mysql/ssb/qwen_ssb_mysql_configs.json")
    parser.add_argument("--config", type=str, default="config/config.yaml")
    parser.add_argument("--knob_config", type=str, default="knob_config/mysql64_knob_config.json")
    parser.add_argument("--output", type=str, default="mysql/ssb/results.json")
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    configs_path = Path(args.configs_json)
    output_path = Path(args.output)
    
    config_path = parent_dir / args.config
    knob_config_path = parent_dir / args.knob_config
    
    script_config = ScriptConfig.from_yaml_file(str(config_path))

    # Initialize db
    if script_config.database_config.port == 3306 or "mysql" in getattr(script_config.database_config, "data_path", "").lower():
        db = MySQLDatabase(script_config.database_config)
    else:
        db = PostgresSQLDatabase(script_config.database_config)
    db.connect()

    knob_settings = KnobSettingsSet.from_json_file(str(knob_config_path))

    with open(configs_path, "r") as f:
        configs = json.load(f)
    
    # Load existing results if available
    existing_results = load_existing_results(output_path)
    
    def_latency = None
    def_throughput = None
    all_perf = []
    
    if existing_results:
        print("Resuming from existing results...")
        def_latency = existing_results["default_performance"]["latency_s"]
        def_throughput = existing_results["default_performance"]["throughput_ops_per_s"]
        all_perf = existing_results.get("all_configs_performance", [])
        print(f"Loaded default performance: latency={def_latency}s, throughput={def_throughput} ops/s")
        print(f"Already tested {len(all_perf)} configs")
        # Recalculate best performance and improvement percentages if they are missing
        # or invalid (e.g., Infinity). Prefer deriving best from recorded per-config
        # results (all_perf) when possible.
        best_from_all = None
        for perf in all_perf:
            try:
                latv = float(perf.get("latency_s"))
            except Exception:
                continue
            if math.isfinite(latv):
                if best_from_all is None or latv < best_from_all[0]:
                    best_from_all = (latv, perf.get("throughput_ops_per_s"), perf.get("config_index"))

        if best_from_all:
            best_latency_calc, best_throughput_calc, best_idx_calc = best_from_all
        else:
            best_perf = existing_results.get("best_performance", {})
            best_latency_calc = best_perf.get("latency_s", float('inf'))
            best_throughput_calc = best_perf.get("throughput_ops_per_s", 0.0)
            best_idx_calc = best_perf.get("config_index", -1)

        # compute improvement percentages where possible
        try:
            def_lat_f = float(def_latency)
        except Exception:
            def_lat_f = None

        latency_improvement = None
        if def_lat_f is not None and math.isfinite(def_lat_f) and math.isfinite(best_latency_calc):
            latency_improvement = ((def_lat_f - best_latency_calc) / def_lat_f) * 100

        try:
            def_thr_f = float(def_throughput)
        except Exception:
            def_thr_f = None

        throughput_improvement = None
        if (
            def_thr_f is not None and def_thr_f != 0
            and math.isfinite(def_thr_f) and isinstance(best_throughput_calc, (int, float))
            and math.isfinite(float(best_throughput_calc))
        ):
            throughput_improvement = ((float(best_throughput_calc) - def_thr_f) / def_thr_f) * 100

        # Update loaded best_performance to reflect recalculated values
        existing_results.setdefault("best_performance", {})
        existing_results["best_performance"]["config_index"] = best_idx_calc
        existing_results["best_performance"]["latency_s"] = best_latency_calc
        existing_results["best_performance"]["throughput_ops_per_s"] = best_throughput_calc
        existing_results["best_performance"]["latency_improvement_percent"] = latency_improvement
        existing_results["best_performance"]["throughput_improvement_percent"] = throughput_improvement
        # set runtime variables from recalculated values
        best_idx = best_idx_calc
        best_latency = best_latency_calc
        best_throughput = best_throughput_calc
        best_config_actual = existing_results.get("best_performance", {}).get("actual_config")
        # Persist recalculated results back to the output file so resume is durable
        try:
            with open(output_path, "w") as wf:
                json.dump(existing_results, wf, indent=4)
            print(f"Wrote recalculated results to {output_path}")
        except Exception as e:
            print(f"Warning: failed to write recalculated results: {e}")
    else:
        print("Running with DEFAULT config...")
        default_knob_config = knob_settings.get_default_knob_settings()
        default_task = BenchmarkTask(workload_path=sql_path, knob_config=default_knob_config)
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(db.run_workload, default_task, 1, True)
                try:
                    def_latency, def_neg_throughput = future.result(timeout=1800)
                    def_throughput = -def_neg_throughput
                except concurrent.futures.TimeoutError:
                    print("Default run timed out after 30 minutes.")
                    with open(sql_path, "r") as sf:
                        sql_content = sf.read()
                    # Estimate number of queries based on semicolons
                    num_queries = sql_content.count(";")
                    if num_queries == 0:
                        num_queries = 1
                    def_latency = 1800.0 / num_queries
                    def_throughput = num_queries / 1800.0
        except Exception as e:
            print(f"Failed default: {e}")
            def_latency, def_throughput = float('inf'), 0.0

    # Build set of already-tested config indices
    tested_indices = set()
    for perf in all_perf:
        if should_skip_config(perf):
            tested_indices.add(perf["config_index"])

    best_idx = -1
    best_latency = float('inf')
    best_throughput = 0.0
    best_config_actual = None

    # Determine best from already-tested configs
    if existing_results and existing_results.get("best_performance"):
        best_info = existing_results["best_performance"]
        if best_info.get("config_index") is not None:
            best_idx = best_info["config_index"]
            best_latency = best_info.get("latency_s", float('inf'))
            best_throughput = best_info.get("throughput_ops_per_s", 0.0)
            best_config_actual = best_info.get("actual_config")

    # Test remaining configs
    configs_to_test = []
    for i, conf_dict in enumerate(configs):
        if i not in tested_indices:
            configs_to_test.append((i, conf_dict))
        else:
            print(f"Skipping config {i} (already tested)")

    print(f"Testing {len(configs_to_test)} remaining configs...")
    
    for i, conf_dict in configs_to_test:
        print(f"Running config {i}")
        actual_conf = convert_config(conf_dict, knob_settings)
        if actual_conf is None:
            print(f"Skipping config {i} due to conversion failure")
            all_perf.append({
                "config_index": i,
                "latency_s": float('inf'),
                "throughput_ops_per_s": 0.0
            })
            continue
        knob_cf = KnobConfig.from_dict(actual_conf, knob_settings)
        
        task = BenchmarkTask(workload_path=sql_path, knob_config=knob_cf)
        try:
            lat, neg_thr = db.run_workload(task, runs_per_iteration=1)
            thr = -neg_thr
        except Exception as e:
            print(f"Failed config {i}: {e}")
            lat, thr = float('inf'), 0.0
            
        all_perf.append({
            "config_index": i,
            "latency_s": lat,
            "throughput_ops_per_s": thr
        })
        
        if lat < best_latency:
            best_latency = lat
            best_throughput = thr
            best_idx = i
            best_config_actual = actual_conf
            print(f"New best config found: {i} with latency={lat}s")
        
        # Save intermediate results after each config
        latency_improvement = None
        if def_latency and def_latency != float('inf'):
            latency_improvement = ((def_latency - best_latency) / def_latency) * 100
            
        throughput_improvement = None
        if def_throughput:
            throughput_improvement = ((best_throughput - def_throughput) / def_throughput) * 100

        res = {
            "workload": str(sql_path),
            "default_performance": {
                "latency_s": def_latency,
                "throughput_ops_per_s": def_throughput
            },
            "best_performance": {
                "config_index": best_idx,
                "latency_s": best_latency,
                "latency_improvement_percent": latency_improvement,
                "throughput_ops_per_s": best_throughput,
                "throughput_improvement_percent": throughput_improvement,
                "actual_config": best_config_actual
            },
            "all_configs_performance": all_perf
        }
        
        with open(output_path, "w") as f:
            json.dump(res, f, indent=4)
            
if __name__ == "__main__":
    main()
