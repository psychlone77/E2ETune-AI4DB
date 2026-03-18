"""
pg_to_mysql_config_calculator.py
---------------------------------
Reads a PostgreSQL E2E-Tune output JSON (knob -> percentage-bucket string),
maps each PostgreSQL knob to its MySQL equivalent(s) using pgql_to_mysql_n_map.json,
then calculates a concrete MySQL knob value by applying the midpoint of the bucket
to the MySQL knob's [min, max] range.

Output: mysql_knob_config.json  (saved alongside this script)
"""

import json
import math
import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).parent                            # .../config_N_map/
MAPPING_FILE = BASE_DIR / "pgql_to_mysql_n_map.json"
INPUT_FILE   = BASE_DIR.parent / "sample_e2e_output.json"     # .../mysql/
OUTPUT_FILE  = BASE_DIR / "mysql_knob_config.json"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
_BUCKET_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*to\s*(\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)


def parse_bucket_midpoint(bucket_str: str) -> float:
    """
    Convert a bucket string such as '80% to 90%' or 'middle' to a fraction
    in [0.0, 1.0] representing the midpoint of that bucket.

    Examples
    --------
    '80% to 90%'  ->  0.85
    '00% to 10%'  ->  0.05
    'middle'      ->  0.50
    """
    bucket_str = bucket_str.strip().lower()

    if bucket_str == "middle":
        return 0.50

    match = _BUCKET_RE.search(bucket_str)
    if match:
        lo = float(match.group(1))
        hi = float(match.group(2))
        return ((lo + hi) / 2.0) / 100.0

    raise ValueError(
        f"Cannot parse bucket string: '{bucket_str}'. "
        "Expected format '00% to 10%' or 'middle'."
    )


# ---------------------------------------------------------------------------
# Value calculation
# ---------------------------------------------------------------------------
def calc_knob_value(midpoint_fraction: float, knob_min: float, knob_max: float) -> int:
    """
    Linearly interpolate within [knob_min, knob_max] at *midpoint_fraction*
    and return a rounded integer value.

    Formula:
        value = round( knob_min + midpoint_fraction * (knob_max - knob_min) )
    """
    raw = knob_min + midpoint_fraction * (knob_max - knob_min)
    return int(math.floor(raw))          # floor keeps the value inside the range


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------
def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_mysql_config(
    pg_output: dict,
    mapping: dict,
) -> dict:
    """
    For every PostgreSQL knob in *pg_output*, look up the MySQL mapping and
    compute a concrete value for every mapped MySQL knob.

    Returns a dict:  mysql_knob_name -> {value, min, max, source_pg_knob, bucket}
    """
    result: dict = {}
    unmapped: list[str] = []

    for pg_knob, bucket_str in pg_output.items():
        if pg_knob not in mapping:
            unmapped.append(pg_knob)
            continue

        try:
            midpoint = parse_bucket_midpoint(bucket_str)
        except ValueError as exc:
            print(f"[WARN] Skipping '{pg_knob}': {exc}")
            continue

        for mysql_knob in mapping[pg_knob]["mysql_knobs"]:
            name = mysql_knob["name"]
            knob_min = mysql_knob["min"]
            knob_max = mysql_knob["max"]

            value = calc_knob_value(midpoint, knob_min, knob_max)

            # If the same MySQL knob appears via multiple PG sources, we keep
            # the last computed value but record all sources.
            if name in result:
                result[name]["source_pg_knobs"].append(pg_knob)
                # Re-compute as average of all contributing midpoints
                result[name]["_midpoints"].append(midpoint)
                avg_midpoint = sum(result[name]["_midpoints"]) / len(result[name]["_midpoints"])
                result[name]["value"] = calc_knob_value(avg_midpoint, knob_min, knob_max)
            else:
                result[name] = {
                    "value": value,
                    "min": knob_min,
                    "max": knob_max,
                    "source_pg_knobs": [pg_knob],
                    "bucket": bucket_str,
                    "_midpoints": [midpoint],
                }

    # Flatten to {knob_name: value} and clean up internal helper key
    flat_result: dict = {}
    for name, entry in result.items():
        flat_result[name] = entry["value"]

    if unmapped:
        print(
            f"\n[INFO] {len(unmapped)} PostgreSQL knob(s) had no MySQL mapping "
            f"and were skipped:\n  " + "\n  ".join(unmapped)
        )

    return flat_result


def save_json(data: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    print(f"\n[OK] MySQL knob config saved to: {path}")


# ---------------------------------------------------------------------------
# Report helper
# ---------------------------------------------------------------------------
def print_summary(config: dict) -> None:
    print(f"\n{'='*65}")
    print(f"  MySQL Knob Configuration  ({len(config)} knobs)")
    print(f"{'='*65}")
    print(f"{'MySQL Knob':<45} {'Value':>20}")
    print(f"{'-'*65}")
    for name, value in sorted(config.items()):
        print(f"  {name:<43} {value:>20}")
    print(f"{'='*65}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[INFO] Loading PG output  : {INPUT_FILE}")
    print(f"[INFO] Loading mapping    : {MAPPING_FILE}")

    pg_output = load_json(INPUT_FILE)
    mapping   = load_json(MAPPING_FILE)

    mysql_config = build_mysql_config(pg_output, mapping)

    print_summary(mysql_config)
    save_json(mysql_config, OUTPUT_FILE)


if __name__ == "__main__":
    main()
