import json
import logging
import os
import sys
import requests
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

import pandas as pd
import re

load_dotenv() 

def get_logger(path: Optional[Path], name: str = "E2ETune") -> logging.Logger:
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

    fmt = logging.Formatter(
        "[%(asctime)s:%(filename)s#L%(lineno)d:%(levelname)s]: %(message)s"
    )

    # File handler
    if path:
        try:
            fh = logging.FileHandler(path, encoding="utf-8")
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


def get_completed_workloads(dbengine: str, servername: str, benchmark_name: str) -> set:
    """Scan benchmark directory for workload folders containing performance_record.txt.
    Returns a set of completed workload identifiers (e.g. 'job_0', 'job_1').
    """
    completed = set()
    perf_dir = Path("data") / dbengine / servername / benchmark_name
    print(f"Checking for completed workloads in: {perf_dir}")
    
    try:
        if not perf_dir.exists() or not perf_dir.is_dir():
            return completed
        
        # Iterate through subdirectories in the benchmark folder
        for item in perf_dir.iterdir():
            if item.is_dir():
                # Check if performance_record.txt exists in this workload folder
                perf_record_file = item / "performance_record.txt"
                if perf_record_file.exists():
                    try:
                        with open(perf_record_file, "r") as f:
                            content = f.read()
                            if "[Iteration 100]" in content:
                                completed.add(item.name)
                                print(f"  Found completed workload (reached iteration 100): {item.name}")
                    except Exception as e:
                        print(f"Error reading performance record for {item.name}: {e}")
    except Exception as e:
        print(f"Error scanning for completed workloads: {e}")
        return completed

    return completed


def load_sampling_data(sampling_log):
    with open(sampling_log, "r") as f:
        lines = f.readlines()

    records = []
    for line in lines:
        records.append(json.loads(line))

    data = pd.DataFrame(records)

    return data

def send_telegram(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("Telegram notifications disabled: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Failed to send Telegram notification: {e}")

def get_num_queries(workload_path: Path) -> int:
    """Count the number of queries in the workload file."""
    try:
        with open(workload_path, "r") as f:
            sql_script = f.read()
            num_queries = sql_script.count(";")
            return num_queries
    except Exception as e:
        print(f"Error counting queries in {workload_path}: {e}")
        return 1