import json
import logging
import os
import sys
import time
import pandas as pd
import re


def get_logger(path: str, name: str = "E2ETune") -> logging.Logger:
    """Return a logger configured to write to `path` and stdout.
    
    Force re-configuration to ensure INFO logs are shown.
    """
    # Ensure directory exists for file handler
    if path:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except Exception:
            pass

    logger = logging.getLogger(name)
    
    # Reset any global disable that might have been set by other libraries
    logging.disable(logging.NOTSET)
    
    # Force the level to INFO
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to start fresh (avoids duplicates and bad configs)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # Prevent propagation to root logger
    logger.propagate = False

    fmt = logging.Formatter('[%(asctime)s:%(filename)s#L%(lineno)d:%(levelname)s]: %(message)s')

    # File handler
    if path:
        try:
            fh = logging.FileHandler(path, encoding='utf-8')
            fh.setLevel(logging.INFO)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
            # Print to stdout to confirm file creation (debug aid)
            print(f"Log file: {os.path.abspath(path)}")
        except Exception as e:
            print(f"Failed to create log file {path}: {e}")

    # Console handler - explicitly use stdout
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def natural_keys(text: str):
    """Split text into list of ints and non-digit strings for natural sorting.

    Example: 'job_10.wg' -> ['job_', 10, '.wg'] which sorts naturally.
    """
    parts = re.findall(r"\d+|\D+", text)
    key = [int(p) if p.isdigit() else p.lower() for p in parts]
    return key


def natural_sort(items: list) -> list:
    """Return a new list sorted in natural order.

    Use `natural_sort(['job_1.wg','job_10.wg','job_2.wg'])` -> ['job_1.wg','job_2.wg','job_10.wg']
    """
    return sorted(items, key=natural_keys)


def get_completed_workloads(perf_dir: str) -> set:
    """Scan `perf_dir` for records and return a set of completed workload identifiers.
    Returns a set of strings that can be matched against workload filenames
    (e.g. 'job_0' or 'job_0.wg').
    """
    completed = set()
    if not perf_dir:
        return completed
    try:
        if not os.path.isdir(perf_dir):
            return completed
        for fname in os.listdir(perf_dir):
            name_no_ext = os.path.splitext(fname)[0]
            completed.add(name_no_ext)
    except Exception:
        return completed

    return completed


def load_sampling_data(sampling_log):
    with open(sampling_log, 'r') as f:
        lines = f.readlines()

    records = []
    for line in lines:
        records.append(json.loads(line))

    data = pd.DataFrame(records)

    return data
