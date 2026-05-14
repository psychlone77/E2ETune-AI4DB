import os
import json
import csv
from pathlib import Path

def collect_results(root_dirs, subfolders, result_files):
    rows = []
    for db in root_dirs:
        for bench in subfolders:
            folder = Path(db) / bench
            for result_file in result_files:
                file_path = folder / result_file
                if file_path.exists():
                    with open(file_path, 'r') as f:
                        try:
                            data = json.load(f)
                        except Exception as e:
                            print(f"Failed to load {file_path}: {e}")
                            continue
                        row = {
                            'db': os.path.basename(db),
                            'benchmark': bench,
                            'result': 'E2ETune++' if result_file == 'results.json' else 'E2ETune-Base',
                            'default_latency': round(data.get('default_performance', {}).get('latency_s', 0), 3) if isinstance(data.get('default_performance', {}).get('latency_s', None), (int, float)) else '',
                            'best_latency': round(data.get('best_performance', {}).get('latency_s', 0), 3) if isinstance(data.get('best_performance', {}).get('latency_s', None), (int, float)) else '',
                            'performance_improvement': round(data.get('best_performance', {}).get('latency_improvement_percent', 0), 2) if isinstance(data.get('best_performance', {}).get('latency_improvement_percent', None), (int, float)) else ''
                        }
                        rows.append(row)
    return rows

def main():
    # Folders to search
    db_folders = ['postgres', 'mysql', 'postgres-16', 'mysql-16']
    subfolders = ['job', 'ssb', 'ssb_flat', 'tpch', 'tpcds']
    result_files = ['results.json', 'base_results.json']
    base_dir = Path(__file__).resolve().parent
    root_dirs = [base_dir / db for db in db_folders]

    rows = collect_results(root_dirs, subfolders, result_files)
    if not rows:
        print("No results found.")
        return

    # Collect all unique columns
    columns = set()
    for row in rows:
        columns.update(row.keys())
    columns = sorted(columns)

    out_csv = base_dir / 'combined_results.csv'
    with open(out_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Combined results written to {out_csv}")

if __name__ == "__main__":
    main()
