import os
import json
import glob

# Benchmarks to aggregate from the project root (e.g., E2ETune-AI4DB/job, E2ETune-AI4DB/tpch)
BENCHMARK_DIRS = [
    'job',
    'tpch',
]


def find_runhistory_files(base_dir):
    """Find all runhistory.jsonl files under benchmark output folders."""
    files = []
    for bench in BENCHMARK_DIRS:
        bench_dir = os.path.join(base_dir, bench)
        if not os.path.isdir(bench_dir):
            continue
        # Look for *hebo_output directories that contain runhistory.jsonl
        pattern = os.path.join(bench_dir, '*_hebo_output', 'runhistory.jsonl')
        files.extend(glob.glob(pattern))
    return sorted(files)


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


def combine_runhistories(base_dir, output_path):
    """Combine all runhistory.jsonl records from benchmarks into one JSON file."""
    sources = find_runhistory_files(base_dir)
    all_records = []
    for src in sources:
        records = read_jsonl_records(src)
        all_records.extend(records)
    # Write a single JSON array file
    with open(output_path, 'w') as out:
        json.dump({
            'count': len(all_records),
            'files': sources,
            'records': all_records,
        }, out)
    print(f"Combined {len(sources)} files, {len(all_records)} records -> {output_path}")
    return all_records


def main():
    # Project root (E2ETune-AI4DB)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Output combined JSON in the same folder as this script
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'combined_runhistory.json')
    combine_runhistories(base_dir, output_path)


if __name__ == "__main__":
    main()