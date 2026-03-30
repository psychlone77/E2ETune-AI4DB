"""
translate_knobs.py

Given an E2ETune output (PostgreSQL percentage ranges) and a MySQL knob
info file (min/max + pg_equivalent), compute a concrete MySQL knob value
for each MySQL knob by:
  1. Looking up the PG-equivalent knob in the E2ETune output.
  2. Parsing the percentage bucket string to get its midpoint.
  3. Mapping that midpoint percentage onto the MySQL knob's [min, max] range.
"""

import json
import math
import os
from typing import Optional


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_json(path: str) -> dict:
    """Load and return a JSON file as a dict."""
    with open(path, "r") as f:
        return json.load(f)


def save_json(data: dict, path: str, indent: int = 2) -> None:
    """Save a dict to a JSON file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=indent)
    print(f"[INFO] Saved output to: {path}")


# ---------------------------------------------------------------------------
# Percentage parsing
# ---------------------------------------------------------------------------

def parse_mid_percentage(range_str: str) -> Optional[float]:
    """
    Parse a percentage range string and return its midpoint as a float (0-100).

    Supported formats:
        "10% to 20%"  ->  15.0
        "00% to 10%"  ->   5.0
        "90% to 100%" ->  95.0
        "middle"      ->  50.0

    Returns None if the string cannot be parsed.
    """
    range_str = range_str.strip().lower()

    if range_str == "middle":
        return 50.0

    # Expect format like "X% to Y%"
    try:
        parts = range_str.replace("%", "").split("to")
        if len(parts) != 2:
            return None
        low = float(parts[0].strip())
        high = float(parts[1].strip())
        return (low + high) / 2.0
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Value calculation
# ---------------------------------------------------------------------------

def calculate_knob_value(min_val: float, max_val: float, mid_pct: float) -> float:
    """
    Map a midpoint percentage (0-100) onto [min_val, max_val].

    Formula: value = min + (mid_pct / 100) * (max - min)
    """
    return min_val + (mid_pct / 100.0) * (max_val - min_val)


def round_knob_value(value: float, min_val: float, max_val: float) -> float:
    """
    Round the computed value to an integer if both min and max are integers,
    otherwise keep as a float. Clamp to [min, max].
    """
    if isinstance(min_val, int) and isinstance(max_val, int):
        value = int(round(value))
    else:
        value = round(value, 6)
    return max(min_val, min(max_val, value))


# ---------------------------------------------------------------------------
# Core translation logic
# ---------------------------------------------------------------------------

def translate_mysql_knobs(e2etune_output: dict, mysql_knobs_info: dict) -> dict:
    """
    For each MySQL knob in mysql_knobs_info:
      - Retrieve its pg_equivalent.
      - Look up the percentage range in e2etune_output.
      - Compute the midpoint percentage.
      - Calculate and return the concrete MySQL knob value.

    Returns a dict: { mysql_knob_name: { "pg_equivalent", "pct_range",
                                         "mid_pct", "min", "max", "value" } }
    """
    results = {}

    for mysql_knob, info in mysql_knobs_info.items():
        pg_equiv = info.get("pg_equivalent")
        min_val  = info.get("min")
        max_val  = info.get("max")

        # Skip if essential fields are missing
        if pg_equiv is None:
            print(f"[SKIP] {mysql_knob}: no pg_equivalent defined.")
            results[mysql_knob] = {"pg_equivalent": None, "value": None, "reason": "no pg_equivalent"}
            continue

        pct_range = e2etune_output.get(pg_equiv)
        if pct_range is None:
            print(f"[SKIP] {mysql_knob}: pg_equivalent '{pg_equiv}' not found in E2ETune output.")
            results[mysql_knob] = {"pg_equivalent": pg_equiv, "value": None, "reason": "pg knob not in e2etune output"}
            continue

        mid_pct = parse_mid_percentage(pct_range)
        if mid_pct is None:
            print(f"[SKIP] {mysql_knob}: could not parse range '{pct_range}'.")
            results[mysql_knob] = {"pg_equivalent": pg_equiv, "pct_range": pct_range, "value": None, "reason": "unparseable range"}
            continue

        raw_value = calculate_knob_value(min_val, max_val, mid_pct)
        final_value = round_knob_value(raw_value, min_val, max_val)

        results[mysql_knob] = {
            "pg_equivalent": pg_equiv,
            "pct_range":     pct_range,
            "mid_pct":       mid_pct,
            "min":           min_val,
            "max":           max_val,
            "value":         final_value,
        }

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    config_dir  = os.path.join(base_dir, "config")

    e2etune_path     = os.path.join(base_dir,   "sample_e2e_output.json")
    mysql_knobs_path = os.path.join(config_dir, "mysql_knobs_with_ranges.json")
    output_path      = os.path.join(base_dir,   "mysql_translated_values.json")

    # Load inputs
    print("[INFO] Loading E2ETune output ...")
    e2etune_output = load_json(e2etune_path)

    print("[INFO] Loading MySQL knob ranges ...")
    mysql_knobs_info = load_json(mysql_knobs_path)

    # Translate
    print("[INFO] Translating knobs ...")
    results = translate_mysql_knobs(e2etune_output, mysql_knobs_info)

    # Flatten to {knob: value} pairs
    flat_results = {knob: data["value"] for knob, data in results.items() if data.get("value") is not None}

    # Save
    save_json(flat_results, output_path)

    # Pretty-print summary
    print("\n===== MySQL Knob Values =====")
    for knob, value in flat_results.items():
        print(f"  {knob:50s} = {value}")


if __name__ == "__main__":
    main()
