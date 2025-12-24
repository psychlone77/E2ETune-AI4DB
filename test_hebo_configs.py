"""
Test and evaluate HEBO-tuned configurations against default PostgreSQL configs.

This script:
1. Loads all best configurations from the job output folders
2. For each workload:
   - Runs queries with default database configuration
   - Runs queries with the tuned configuration
   - Measures average query execution time
   - Calculates performance improvement
3. Outputs comprehensive results to CSV and summary report
"""

import os
import json
import time
import glob
import csv
from typing import Dict, Any
from config import parse_config
from Database import Database
from stress_testing_tool import stress_testing_tool
import logging

# Path to HEBO output directory containing job_X_hebo_output folders
HEBO_OUTPUT_DIR = './job'

# Path to workload files
WORKLOAD_DIR = './olap_workloads'

# Output files - results written incrementally
RESULTS_FILE = './test_results/hebo_comparison_results.txt'
RESULTS_CSV = './test_results/hebo_comparison_results.csv'


def load_best_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load all best configurations from HEBO output folders.
    
    Returns:
        Dict mapping workload_name -> best_config_data
    """
    configs = {}
    
    pattern = os.path.join(HEBO_OUTPUT_DIR, '*_hebo_output', 'best_config.json')
    config_files = glob.glob(pattern)
    
    print(f"Found {len(config_files)} HEBO output configurations")
    
    for config_file in sorted(config_files):
        try:
            with open(config_file, 'r') as f:
                data = json.load(f)
            
            workload_name = data.get('workload', 'unknown')
            configs[workload_name] = data
            print(f"Loaded config for: {workload_name}")
            
        except Exception as e:
            print(f"ERROR loading {config_file}: {e}")
    
    return configs

if __name__ == "__main__":
    args = parse_config.parse_args("config/config.ini")
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    db = Database(config=args, knob_config_path=args['tuning_config']['knob_config'])
    stress_tester = stress_testing_tool(args, db, logger, records_log="logs/stress_testing/records.log")
    best_configs = load_best_configs()

    # Create output directory and initialize files
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    
    # Initialize text results file with header
    with open(RESULTS_FILE, 'w') as f:
        f.write("="*80 + "\n")
        f.write("HEBO Configuration Evaluation - Results\n")
        f.write("="*80 + "\n")
        f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total workloads to test: {len(best_configs)}\n")
        f.write("="*80 + "\n\n")
    
    # Initialize CSV file with header
    with open(RESULTS_CSV, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            'workload', 'default_avg_time', 'tuned_avg_time', 
            'speedup', 'improvement_pct', 'time_saved'
        ])
        writer.writeheader()
    
    print(f"\n{'='*80}")
    print(f"Starting evaluation of {len(best_configs)} workloads")
    print(f"Results being written to: {RESULTS_FILE}")
    print(f"CSV being written to: {RESULTS_CSV}")
    print(f"{'='*80}\n")
    
    all_results = []

    for idx, workload_name in enumerate(sorted(best_configs.keys()), 1):
        print(f"\n[{idx}/{len(best_configs)}] Testing: {workload_name}")
        
        workload_file = os.path.join(WORKLOAD_DIR, f"{workload_name}.wg")
        
        if not os.path.exists(workload_file):
            msg = f"WARNING: Workload file not found: {workload_file}\n"
            print(msg)
            with open(RESULTS_FILE, 'a') as f:
                f.write(f"[{idx}/{len(best_configs)}] {workload_name}\n")
                f.write(f"  {msg}\n")
            continue

        config_data = best_configs[workload_name]
        tuned_config = config_data.get('configuration', {})
        
        # Test with default configuration
        print(f"  Running with DEFAULT config...")
        db.reset_db_knobs()
        db.restart_db()

        default = stress_tester._test_by_dwg(
            workload_path=workload_file,
            log_file=f"logs/performance/workload_execution.log",
            iteration=0
        )['avg_time_per_query']

        # Test with tuned configuration
        print(f"  Running with TUNED config...")
        db.change_knob(tuned_config)
        db.restart_db()

        tuned = stress_tester._test_by_dwg(
            workload_path=workload_file,
            log_file=f"logs/performance/workload_execution.log",
            iteration=1
        )['avg_time_per_query']

        # Calculate metrics
        speedup = default / tuned if tuned > 0 else 0
        improvement = ((default - tuned) / default) * 100 if default > 0 else 0
        time_saved = default - tuned
        
        # Print to console
        print(f"  Default Avg Time/Query: {default:.4f} s")
        print(f"  Tuned   Avg Time/Query: {tuned:.4f} s")
        print(f"  Speedup: {speedup:.2f}x")
        print(f"  Improvement: {improvement:.2f}%")
        print(f"  Time Saved: {time_saved:.4f} s")
        
        # Immediately write to text results file
        with open(RESULTS_FILE, 'a') as f:
            f.write(f"[{idx}/{len(best_configs)}] Workload: {workload_name}\n")
            f.write(f"  Default Avg Time/Query: {default:.4f} s\n")
            f.write(f"  Tuned   Avg Time/Query: {tuned:.4f} s\n")
            f.write(f"  Speedup: {speedup:.2f}x\n")
            f.write(f"  Improvement: {improvement:.2f}%\n")
            f.write(f"  Time Saved: {time_saved:.4f} s\n")
            f.write(f"  Completed: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-"*80 + "\n\n")
        
        # Immediately append to CSV file
        result_row = {
            'workload': workload_name,
            'default_avg_time': f"{default:.6f}",
            'tuned_avg_time': f"{tuned:.6f}",
            'speedup': f"{speedup:.4f}",
            'improvement_pct': f"{improvement:.2f}",
            'time_saved': f"{time_saved:.6f}"
        }
        
        with open(RESULTS_CSV, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                'workload', 'default_avg_time', 'tuned_avg_time', 
                'speedup', 'improvement_pct', 'time_saved'
            ])
            writer.writerow(result_row)
        
        # Store for final summary
        all_results.append({
            'workload': workload_name,
            'default': default,
            'tuned': tuned,
            'speedup': speedup,
            'improvement': improvement,
            'time_saved': time_saved
        })
    
    # Write final summary
    print(f"\n{'='*80}")
    print("All tests complete! Writing summary...")
    print(f"{'='*80}\n")
    
    with open(RESULTS_FILE, 'a') as f:
        f.write("\n" + "="*80 + "\n")
        f.write("SUMMARY STATISTICS\n")
        f.write("="*80 + "\n")
        f.write(f"Completed: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total workloads tested: {len(all_results)}\n\n")
        
        if all_results:
            avg_speedup = sum(r['speedup'] for r in all_results) / len(all_results)
            avg_improvement = sum(r['improvement'] for r in all_results) / len(all_results)
            total_time_saved = sum(r['time_saved'] for r in all_results)
            
            f.write(f"Average Speedup: {avg_speedup:.2f}x\n")
            f.write(f"Average Improvement: {avg_improvement:.2f}%\n")
            f.write(f"Total Time Saved (per query): {total_time_saved:.4f} s\n\n")
            
            # Top 10 improvements
            f.write("Top 10 Improvements:\n")
            sorted_results = sorted(all_results, key=lambda x: x['improvement'], reverse=True)
            for i, r in enumerate(sorted_results[:10], 1):
                f.write(f"  {i}. {r['workload']}: {r['improvement']:.2f}% ({r['speedup']:.2f}x)\n")
            
            f.write("\nBottom 10 Improvements:\n")
            for i, r in enumerate(sorted_results[-10:], 1):
                f.write(f"  {i}. {r['workload']}: {r['improvement']:.2f}% ({r['speedup']:.2f}x)\n")
    
    print(f"Results written to: {RESULTS_FILE}")
    print(f"CSV written to: {RESULTS_CSV}")
    print("Evaluation complete!")


