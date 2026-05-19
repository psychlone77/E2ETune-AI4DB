import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

import utils
from classes.base_classes.Script_Config import ScriptConfig
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Database import Database
from classes.Global_Vars import TuningParameter
from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.MySQL_Database import MySQLDatabase
from classes.BenchBase_Database import BenchBaseDatabase
from classes.base_classes.Knob_Config import KnobConfig

def main():
    parser = argparse.ArgumentParser(description="Run Diverse Configs")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--dbengine",
        default="mysql",
        help="Database engine type (default: postgresql)",
    )
    parser.add_argument(
        "--servername",
        default="hetzner-4c-8t-64gb",
        help="Server specifications for tuning (default: hetzner-4c-8t-64gb)",
    )
    parser.add_argument(
        "--skip-configs",
        action="store_true",
        help="Skip running diverse configs and only collect default data",
    )
    cli_args = parser.parse_args()

    # Load typed configuration
    script_config = ScriptConfig.from_yaml_file(cli_args.config)
    
    with open(cli_args.config, "r") as f:
        raw_config = yaml.safe_load(f)

    knob_config_path = raw_config.get("tuning", {}).get(
        "config", "knob_config/knob_config.json"
    )
    knob_settings = KnobSettingsSet.from_json_file(knob_config_path)

    olap_workloads = [
        {"database.name": "benchbase", "benchmark.name": "twitter", "benchmark.path": "./oltp_workloads/twitter"},
        # {"database.name": "ssb", "benchmark.name": "ssb", "benchmark.path": "./olap_workloads"},
        # {"database.name": "ssb", "benchmark.name": "ssb_flat_tiny", "benchmark.path": "./olap_workloads"},
        # {"database.name": "tpch", "benchmark.name": "tpch", "benchmark.path": "./olap_workloads"},
        # {"database.name": "tpcds", "benchmark.name": "tpcds", "benchmark.path": "./olap_workloads"},
    ]

    # Load the configs
    configs_file = "10_diverse_configs_all.json"
    if not os.path.exists(configs_file):
        print(f"Cannot find {configs_file}")
        return

    with open(configs_file, "r") as f:
        diverse_configs = json.load(f)

    for w_conf in olap_workloads:
        script_config.database_config.name = w_conf["database.name"]
        script_config.benchmark_config.name = w_conf["benchmark.name"]
        script_config.benchmark_config.path = w_conf["benchmark.path"]
        
        benchmark_config = script_config.benchmark_config

        base_output_dir = Path("results/diverse_configs") / cli_args.servername / cli_args.dbengine / benchmark_config.name
        base_output_dir.mkdir(parents=True, exist_ok=True)

        log_path = base_output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logger = utils.get_logger(log_path, name=f"DiverseConfigs_{benchmark_config.name}")

        logger.info("=" * 100)
        logger.info(f"E2ETune: Running Diverse Configs for {benchmark_config.name}")
        logger.info("=" * 100)
        logger.info(f"Config: {cli_args.config}")
        logger.info(f"Host: {script_config.database_config.host}  DB: {script_config.database_config.name}")
        logger.info(f"Benchmark: {benchmark_config.name}  Type: {benchmark_config.type}")
        logger.info(f"Workload path: {benchmark_config.path}")
        logger.info("=" * 100)

        db: Database
        if benchmark_config.type == "oltp":
            db = BenchBaseDatabase(db_config=script_config.database_config, benchmark_config=benchmark_config, log_path=log_path)
        else:
            if cli_args.dbengine.lower() == "postgresql":
                db = PostgresSQLDatabase(db_config=script_config.database_config, log_path=log_path)
            elif cli_args.dbengine.lower() == "mysql":
                db = MySQLDatabase(db_config=script_config.database_config, log_path=log_path)
            else:
                logger.error(f"Unsupported database engine: {cli_args.dbengine}")
                continue

        tuning_parameter = (
            TuningParameter.THROUGHPUT
            if benchmark_config.type == "oltp"
            else TuningParameter.LATENCY
        )

        benchmark_configs = diverse_configs.get(benchmark_config.name, {})

        from main import build_tuner, DefaultDataCollector, DataCollectorOLAP, DataCollectorOLTP
        
        # We will just simulate the saving for all found workloads in benchmark config
        for workload_name, configs in benchmark_configs.items():
            if not configs:
                continue
                
            # Determine actual file extension based on workload type
            if benchmark_config.type == "oltp":
                workload_file = f"{workload_name}.xml"
            else:
                workload_file = f"{workload_name}.wg"
                
            workload_path = Path(benchmark_config.path) / workload_file
            if not workload_path.exists():
                logger.warning(f"Could not find workload file: {workload_path}. Skipping.")
                continue

            output_dir = (
                Path("data")
                / cli_args.dbengine
                / cli_args.servername
                / benchmark_config.name
                / workload_name
            )
            os.makedirs(output_dir, exist_ok=True)

            logger.info(f"Starting default data collection for workload: {workload_name}")
            collector_cls: DefaultDataCollector = (
                DataCollectorOLTP if benchmark_config.type == "oltp" else DataCollectorOLAP
            )
            ddc = collector_cls(
                workload_path=workload_path,
                db=db,
                benchmark=benchmark_config.name,
                output_dir=output_dir,
                knob_settings_set=knob_settings,
                log_path=log_path,
            )
            ddc.collect()
            logger.info(f"Finished default data collection for workload: {workload_name}")

            if cli_args.skip_configs:
                continue

            logger.info(f"Running {len(configs)} configs for {workload_name}")
            workload_dir = base_output_dir / workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)
            
            history_file = workload_dir / "run_history.jsonl"
            performance_record_file = workload_dir / "performance_record.txt"
            
            for i, config_dict in enumerate(configs):
                logger.info(f"Running config {i+1}/{len(configs)} for {workload_name}")
                
                if cli_args.dbengine.lower() == "mysql":
                    n_map = {
                        "max_connections": {"mysql_knobs": [{"name": "max_connections", "min": 51, "max": 501}]},
                        "wal_buffers": {"mysql_knobs": [{"name": "innodb_log_buffer_size", "min": 16777216, "max": 268435456}]},
                        "shared_buffers": {"mysql_knobs": [{"name": "innodb_buffer_pool_size", "min": 134217728, "max": 21076410368}]},
                        "max_wal_size": {"mysql_knobs": [{"name": "innodb_redo_log_capacity", "min": 104857600, "max": 8426340352}]},
                        "commit_delay": {"mysql_knobs": [{"name": "binlog_group_commit_sync_delay", "min": 0, "max": 100000}]},
                        "commit_siblings": {"mysql_knobs": [{"name": "binlog_group_commit_sync_no_delay_count", "min": 0, "max": 1000}]},
                        "deadlock_timeout": {"mysql_knobs": [{"name": "innodb_lock_wait_timeout", "min": 10, "max": 310}]},
                        "default_statistics_target": {"mysql_knobs": [{"name": "innodb_stats_persistent_sample_pages", "min": 20, "max": 1020}]},
                        "effective_io_concurrency": {"mysql_knobs": [{"name": "innodb_read_io_threads", "min": 2, "max": 8}, {"name": "innodb_write_io_threads", "min": 2, "max": 8}]},
                        "join_collapse_limit": {"mysql_knobs": [{"name": "optimizer_search_depth", "min": 0, "max": 62}]},
                        "from_collapse_limit": {"mysql_knobs": [{"name": "optimizer_search_depth", "min": 0, "max": 62}]},
                        "maintenance_work_mem": {"mysql_knobs": [{"name": "innodb_sort_buffer_size", "min": 1048576, "max": 68157440}, {"name": "myisam_sort_buffer_size", "min": 8388608, "max": 545259520}]},
                        "work_mem": {"mysql_knobs": [{"name": "sort_buffer_size", "min": 262144, "max": 67371008}, {"name": "join_buffer_size", "min": 262144, "max": 67371008}]},
                        "temp_buffers": {"mysql_knobs": [{"name": "tmp_table_size", "min": 16777216, "max": 536870912}, {"name": "temptable_max_ram", "min": 1073741824, "max": 10737418240}]},
                        "temp_file_limit": {"mysql_knobs": [{"name": "max_heap_table_size", "min": 16777216, "max": 536870912}, {"name": "temptable_max_mmap", "min": 0, "max": 10737418240}]},
                        "bgwriter_delay": {"mysql_knobs": [{"name": "innodb_page_cleaners", "min": 2, "max": 8}]},
                        "bgwriter_lru_maxpages": {"mysql_knobs": [{"name": "innodb_lru_scan_depth", "min": 512, "max": 10752}]},
                        "checkpoint_completion_target": {"mysql_knobs": [{"name": "innodb_max_dirty_pages_pct", "min": 10, "max": 90}, {"name": "innodb_io_capacity", "min": 200, "max": 6200}]},
                        "wal_writer_delay": {"mysql_knobs": [{"name": "innodb_flush_log_at_timeout", "min": 1, "max": 61}]},
                        "autovacuum_max_workers": {"mysql_knobs": [{"name": "innodb_purge_threads", "min": 2, "max": 8}]},
                        "autovacuum_vacuum_cost_delay": {"mysql_knobs": [{"name": "innodb_io_capacity", "min": 200, "max": 6200}]},
                        "autovacuum_vacuum_cost_limit": {"mysql_knobs": [{"name": "innodb_io_capacity_max", "min": 2000, "max": 12000}]},
                        "autovacuum_naptime": {"mysql_knobs": [{"name": "innodb_purge_batch_size", "min": 100, "max": 1100}, {"name": "innodb_purge_threads", "min": 2, "max": 8}]},
                        "effective_cache_size": {"mysql_knobs": [{"name": "innodb_buffer_pool_size", "min": 134217728, "max": 21076410368}]},
                        "backend_flush_after": {"mysql_knobs": [{"name": "innodb_flush_neighbors", "min": 0, "max": 2}]}
                    }
                    
                    mysql_config = {}
                    for pg_knob, val in config_dict.items():
                        try:
                            pg_setting = knob_settings.get_knob(pg_knob)
                            pg_min = pg_setting.min
                            pg_max = pg_setting.max
                        except KeyError:
                            continue
                        
                        if pg_max == pg_min:
                            ratio = 0.0
                        else:
                            ratio = float(val - pg_min) / (pg_max - pg_min)
                            ratio = max(0.0, min(1.0, ratio))
                        
                        if pg_knob in n_map:
                            for mysql_knob_info in n_map[pg_knob]["mysql_knobs"]:
                                m_name = mysql_knob_info["name"]
                                m_min = mysql_knob_info["min"]
                                m_max = mysql_knob_info["max"]
                                m_val = int(m_min + ratio * (m_max - m_min))
                                mysql_config[m_name] = m_val
                        else:
                            # Fallback if mapping not in n_map, just try using it as is if needed, 
                            # but typically we only map what's found in n_map
                            pass
                    config_dict = mysql_config

                # create KnobConfig and BenchmarkTask
                knob_config = KnobConfig.from_dict(config_dict, knob_settings)
                workload_task = BenchmarkTask(
                    workload_path=workload_path,
                    knob_config=knob_config,
                )
                
                try:
                    results = db.run_workload(workload_task)
                    perf = results[tuning_parameter.value]
                    logger.info(f"Performance for config {i+1}: {perf}")
                except Exception as e:
                    logger.error(f"Error running config {i+1} for {workload_name}: {e}")
                    perf = -1.0 # Or some error indicating value
                
                with open(history_file, "a") as hf:
                    hf.write(json.dumps({"config": config_dict, "performance": perf}) + "\n")
                    
                with open(performance_record_file, "a") as pf:
                    pf.write(f"Config {i+1}: {perf}\n")
                
if __name__ == "__main__":
    main()
