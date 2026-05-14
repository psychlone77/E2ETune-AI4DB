#!/usr/bin/env python3
"""Build a domain-adaptation-ready CSV from cost_model_run_history.csv.

Steps:
1) Map functionally similar PostgreSQL/MySQL knobs into universal columns.
2) Keep unmatched knobs as engine-specific columns (pg__*, mysql__*).
3) Standard-scale all numeric feature columns using available (non-missing) values.
4) Impute missing values with 0 *after* scaling so missing-side values are neutral.
"""

from __future__ import annotations

import argparse
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd


PG_KNOBS: List[str] = [
    "autovacuum_analyze_scale_factor",
    "autovacuum_analyze_threshold",
    "autovacuum_max_workers",
    "autovacuum_naptime",
    "autovacuum_vacuum_cost_delay",
    "autovacuum_vacuum_cost_limit",
    "autovacuum_vacuum_scale_factor",
    "autovacuum_vacuum_threshold",
    "backend_flush_after",
    "bgwriter_delay",
    "bgwriter_flush_after",
    "bgwriter_lru_maxpages",
    "bgwriter_lru_multiplier",
    "checkpoint_completion_target",
    "checkpoint_flush_after",
    "checkpoint_timeout",
    "commit_delay",
    "commit_siblings",
    "cursor_tuple_fraction",
    "deadlock_timeout",
    "default_statistics_target",
    "effective_cache_size",
    "effective_io_concurrency",
    "from_collapse_limit",
    "geqo_effort",
    "geqo_generations",
    "geqo_pool_size",
    "geqo_seed",
    "geqo_threshold",
    "join_collapse_limit",
    "maintenance_work_mem",
    "max_connections",
    "max_wal_senders",
    "shared_buffers",
    "temp_buffers",
    "temp_file_limit",
    "vacuum_cost_delay",
    "vacuum_cost_limit",
    "vacuum_cost_page_dirty",
    "vacuum_cost_page_hit",
    "vacuum_cost_page_miss",
    "wal_buffers",
    "wal_writer_delay",
    "work_mem",
]

MYSQL_KNOBS: List[str] = [
    "binlog_group_commit_sync_delay",
    "binlog_group_commit_sync_no_delay_count",
    "innodb_buffer_pool_size",
    "innodb_flush_log_at_timeout",
    "innodb_flush_neighbors",
    "innodb_io_capacity",
    "innodb_io_capacity_max",
    "innodb_lock_wait_timeout",
    "innodb_log_buffer_size",
    "innodb_lru_scan_depth",
    "innodb_max_dirty_pages_pct",
    "innodb_page_cleaners",
    "innodb_purge_batch_size",
    "innodb_purge_threads",
    "innodb_read_io_threads",
    "innodb_redo_log_capacity",
    "innodb_sort_buffer_size",
    "innodb_stats_persistent_sample_pages",
    "innodb_write_io_threads",
    "join_buffer_size",
    "max_connections",
    "max_heap_table_size",
    "myisam_sort_buffer_size",
    "optimizer_search_depth",
    "sort_buffer_size",
    "temptable_max_mmap",
    "temptable_max_ram",
    "tmp_table_size",
]


# (universal_name, postgres_knob, mysql_knob)
UNIVERSAL_MAPPING: List[Tuple[str, str, str]] = [
    ("universal_main_memory_pool", "shared_buffers", "innodb_buffer_pool_size"),
    ("universal_sort_memory", "work_mem", "sort_buffer_size"),
    ("universal_log_buffer", "wal_buffers", "innodb_log_buffer_size"),
    (
        "universal_maintenance_sort_memory",
        "maintenance_work_mem",
        "innodb_sort_buffer_size",
    ),
    ("universal_temp_memory", "temp_buffers", "tmp_table_size"),
    (
        "universal_temp_memory_cap",
        "temp_file_limit",
        "max_heap_table_size",
    ),
    (
        "universal_io_capacity",
        "effective_io_concurrency",
        "innodb_io_capacity",
    ),
    (
        "universal_optimizer_search_depth",
        "from_collapse_limit",
        "optimizer_search_depth",
    ),
    ("universal_max_connections", "max_connections", "max_connections"),
]


def prefixed_feature_col(knob_name: str) -> str:
    return f"features.{knob_name}"


def numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def standard_scale_with_post_fill_zero(series: pd.Series) -> pd.Series:
    non_missing = series.dropna()
    if non_missing.empty:
        return pd.Series(np.zeros(len(series), dtype=np.float32), index=series.index)

    mean = float(non_missing.mean())
    std = float(non_missing.std(ddof=0))
    if not np.isfinite(std) or std == 0.0:
        scaled = pd.Series(np.zeros(len(series), dtype=np.float32), index=series.index)
    else:
        scaled = (series - mean) / std
        scaled = scaled.astype(np.float32)

    # Missing side is neutralized to 0 only after scaling.
    scaled = scaled.where(series.notna(), other=0.0)
    return scaled.fillna(0.0).astype(np.float32)


def build_unified_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    engine_col = "metadata.db_engine"
    if engine_col not in df.columns:
        raise ValueError(f"Required column not found: {engine_col}")

    engine = df[engine_col].astype(str).str.lower()
    is_pg = engine.eq("postgresql")
    is_mysql = engine.eq("mysql")

    passthrough_cols = [
        c
        for c in [
            "target.cost",
            "metadata.benchmark",
            "metadata.db_engine",
            "metadata.hardware",
            "metadata.hardware_specs.cores",
            "metadata.hardware_specs.ram_gb",
            "metadata.hardware_specs.threads",
            "metadata.source_run_history",
            "metadata.workload",
            "metadata.workload_key",
        ]
        if c in df.columns
    ]

    out = df[passthrough_cols].copy()
    out["domain_label"] = np.where(is_mysql, 1, 0).astype(np.int32)

    used_pg: set[str] = set()
    used_mysql: set[str] = set()

    # 1) Universal mapped knobs
    for uni_name, pg_knob, my_knob in UNIVERSAL_MAPPING:
        pg_col = prefixed_feature_col(pg_knob)
        my_col = prefixed_feature_col(my_knob)

        pg_vals = numeric_series(df, pg_col)
        my_vals = numeric_series(df, my_col)

        uni_vals = pd.Series(np.nan, index=df.index, dtype=float)
        uni_vals = uni_vals.where(~is_pg, pg_vals)
        uni_vals = uni_vals.where(~is_mysql, my_vals)
        # Fallback if engine labels are unexpected
        uni_vals = uni_vals.where(uni_vals.notna(), pg_vals)
        uni_vals = uni_vals.where(uni_vals.notna(), my_vals)

        out[uni_name] = uni_vals
        used_pg.add(pg_knob)
        used_mysql.add(my_knob)

    # 2) Engine-specific unmatched knobs
    for knob in PG_KNOBS:
        if knob in used_pg:
            continue
        src = numeric_series(df, prefixed_feature_col(knob))
        out[f"pg__{knob}"] = src.where(is_pg, np.nan)

    for knob in MYSQL_KNOBS:
        if knob in used_mysql:
            continue
        src = numeric_series(df, prefixed_feature_col(knob))
        out[f"mysql__{knob}"] = src.where(is_mysql, np.nan)

    # 3) Standard-scale all numeric adaptation features and post-fill missing with 0
    protected = set(passthrough_cols + ["domain_label"])
    feature_cols = [c for c in out.columns if c not in protected]

    for col in feature_cols:
        out[col] = standard_scale_with_post_fill_zero(pd.to_numeric(out[col], errors="coerce"))

    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a domain-adaptation-ready knob CSV from run history."
    )
    parser.add_argument(
        "--input-csv",
        default="surrogate/cost_model_run_history.csv",
        help="Path to source run-history CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="surrogate/cost_model_run_history_domain_adaptation.csv",
        help="Path to output CSV.",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv, low_memory=False)
    unified = build_unified_dataframe(df)
    unified.to_csv(args.output_csv, index=False)

    print(f"Input rows: {len(df)}")
    print(f"Output rows: {len(unified)}")
    print(f"Output columns: {len(unified.columns)}")
    print(f"Wrote: {args.output_csv}")


if __name__ == "__main__":
    main()
