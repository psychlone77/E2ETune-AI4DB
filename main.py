import argparse
import os
import sys
import yaml
import json
import concurrent.futures
from datetime import datetime
from pathlib import Path

import utils
from classes.base_classes.Script_Config import ScriptConfig
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.HEBO_Tuner import HEBOTuner
from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.MySQL_Database import MySQLDatabase
from classes.Cost_Model import CostModel
from classes.base_classes.Surrogate_Strategy import SurrogateFactory
from classes.Global_Vars import TuningParameter
from classes.base_classes.Database import Database
from classes.base_classes.Data_Collector import DefaultDataCollector
from classes.DataCollectorOLAP import DataCollectorOLAP
from classes.DataCollectorOLTP import DataCollectorOLTP
from classes.BenchBase_Database import BenchBaseDatabase

# from get_workload_features import process_olap_workload_features

REAL_TUNING_LIMIT = 13


def build_tuner(
    workload_runner,
    script_config: ScriptConfig,
    knob_settings: KnobSettingsSet,
    workload_path: Path,
    output_dir: Path,
    log_path: Path,
    tuning_parameter: TuningParameter,
) -> HEBOTuner:
    """Construct an HEBOTuner for a single workload."""
    return HEBOTuner(
        workload_runner=workload_runner,
        tuning_config=script_config.tuning_config,
        tuning_parameter=tuning_parameter,
        workload_task=BenchmarkTask(
            workload_path=workload_path,
            knob_config=knob_settings.get_default_knob_settings(),
        ),
        knob_settings=knob_settings,
        output_dir=output_dir,
        log_path=log_path,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2ETune: Database Tuning System")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to YAML configuration file (default: config/config.yaml)",
    )
    parser.add_argument(
        "--dbengine",
        default="postgresql",
        help="Database engine type (default: postgresql)",
    )
    parser.add_argument(
        "--servername",
        default="hetzner-4c-8t-32gb",
        help="Server specifications for tuning (default: hetzner-4c-8t-32gb)",
    )
    cli_args = parser.parse_args()

    # Load raw YAML to access fields not mapped into ScriptConfig dataclasses
    with open(cli_args.config, "r") as f:
        raw_config = yaml.safe_load(f)

    # Load typed configuration into ScriptConfig
    script_config = ScriptConfig.from_yaml_file(cli_args.config)
    db_config = script_config.database_config
    tuning_config = script_config.tuning_config
    benchmark_config = script_config.benchmark_config
    surrogate_config = script_config.surrogate_config

    # Knob config path lives under tuning.config in the YAML
    knob_config_path = raw_config.get("tuning", {}).get(
        "config", "knob_config/knob_config.json"
    )
    knob_settings = KnobSettingsSet.from_json_file(knob_config_path)

    # Setup main logger
    main_log_path = Path(
        f"logs/tuning/main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logger = utils.get_logger(main_log_path, name="Main")
    logger.info("=" * 100)
    logger.info("E2ETune: End-to-End Database Tuning System")
    logger.info("=" * 100)
    logger.info(f"Config: {cli_args.config}")
    logger.info(f"Host: {db_config.host}  DB: {db_config.name}")
    logger.info(f"Benchmark: {benchmark_config.name}  Type: {benchmark_config.type}")
    logger.info(f"Workload path: {benchmark_config.path}")
    logger.info("=" * 100)
    utils.send_telegram(f"E2ETune started for benchmark: *{benchmark_config.name}*")

    # Discover workloads
    workload_base_path = benchmark_config.path
    if not os.path.isdir(workload_base_path):
        logger.error(f"Workload path does not exist: {workload_base_path}")
        exit(1)

    all_files = os.listdir(workload_base_path)
    db: Database
    import re
    if benchmark_config.type == "oltp":
        workloads = [
            f for f in all_files if re.match(rf"^(?:sample_)?{benchmark_config.name}(?:_config)?\d*\.xml$", f) or re.match(rf"^{benchmark_config.name}(?:_\d+)?\.xml$", f)
        ]
        db = BenchBaseDatabase(db_config=db_config, benchmark_config=benchmark_config, log_path=main_log_path)
    else:
        workloads = [
            f
            for f in all_files
            if re.match(rf"^{benchmark_config.name}(?:_\d+)?\.wg$", f)
        ]
        if cli_args.dbengine == "postgresql":
            db = db = PostgresSQLDatabase(db_config=db_config, log_path=main_log_path)
        elif cli_args.dbengine == "mysql":
            db = MySQLDatabase(db_config=db_config, log_path=main_log_path)
        else:
            logger.error(f"Unsupported database engine: {cli_args.dbengine}")
            exit(1)

    tuning_parameter = (
        TuningParameter.THROUGHPUT
        if benchmark_config.type == "oltp"
        else TuningParameter.LATENCY
    )

    workloads = utils.natural_sort(workloads)
    total_workloads = len(workloads)
    logger.info(f"Found {total_workloads} workloads matching '{benchmark_config.name}'")

    phase1_workloads = []
    try:
        with open("representative_workloads_sampled.json", "r") as f:
            sampled_data = json.load(f)
            benchmark_clusters = sampled_data.get(benchmark_config.name, {})
            for cluster_name, cluster_wks in benchmark_clusters.items():
                phase1_workloads.extend(cluster_wks)
    except Exception as e:
        logger.error(f"Could not load representative workloads: {e}")
        # fallback
        phase1_workloads = workloads[:REAL_TUNING_LIMIT]

    force_surrogate = raw_config.get("tuning", {}).get("force_surrogate", False)

    if force_surrogate:
        logger.info("force_surrogate is enabled. Forcing all remaining workloads into Phase 2 (Surrogate).")
        phase1_workloads = []
        phase2_workloads = workloads
    else:
        # only keep workloads that actually exist in the folder just to be safe
        phase1_workloads = [wk for wk in phase1_workloads if wk in workloads]
        phase2_workloads = [w for w in workloads if w not in phase1_workloads]

    # Resume support: skip already-completed workloads
    completed = utils.get_completed_workloads(
        cli_args.dbengine, cli_args.servername, benchmark_config.name
    )
    if completed:
        logger.info(
            f"Found {len(completed)} completed workloads in: {benchmark_config.performance_record_path}"
        )

    successful, failed, skipped = 0, 0, 0

    # ------------------------------------------------------------------
    # Phase 1: Real database execution (sampled cluster workloads)
    # ------------------------------------------------------------------
    logger.info(f"Phase 1: Real execution – up to {len(phase1_workloads)} workloads")
    utils.send_telegram(f"Phase 1 started: Real execution of representative workloads ({len(phase1_workloads)} workloads)")

    log_path = Path(
        f"logs/tuning/{benchmark_config.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    for idx, workload in enumerate(phase1_workloads):
        workload_id = os.path.splitext(workload)[0]
        if workload_id in completed or workload in completed:
            skipped += 1
            logger.info(
                f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Skipping completed: {workload}"
            )
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Skipping completed: {workload}")
            continue

        workload_path = Path(workload_base_path) / workload
        output_dir = (
            Path("data")
            / cli_args.dbengine
            / cli_args.servername
            / benchmark_config.name
            / workload_id
        )
        os.makedirs(output_dir, exist_ok=True)

        utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Starting default data collection for workload: *{workload}*")
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
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ddc.collect)
                future.result(timeout=600)  # 10 minutes
        except concurrent.futures.TimeoutError:
            logger.error(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Default data collection timed out after 10m for {workload}. Skipping.")
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Default data collection timed out for: *{workload}*. Skipping.")
            skipped += 1
            continue
        except Exception as e:
            logger.error(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Default data collection failed for {workload}: {e}")
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Default data collection failed for: *{workload}* - {e}")
            failed += 1
            continue

        try:
            logger.info("-" * 80)
            logger.info(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Tuning: {workload}")
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Tuning started for workload: *{workload}*")
            tuner = build_tuner(
                db,
                script_config,
                knob_settings,
                workload_path,
                output_dir,
                log_path,
                tuning_parameter,
            )
            tuner.tune()
            successful += 1
            logger.info(
                f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Completed: {workload}"
            )
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Tuning completed for workload: *{workload}*")
        except Exception as e:
            failed += 1
            logger.error(
                f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Error tuning {workload}: {e}",
                exc_info=True,
            )
            utils.send_telegram(f"[Phase-1 {idx + 1}/{len(phase1_workloads)}] Error tuning workload: *{workload}* - {e}")
            if total_workloads >= 10:
                logger.error("Stopping due to error (large workload set)")
                utils.send_telegram("Stopping E2ETune due to error (large workload set)")
                break

    # ------------------------------------------------------------------
    # Phase 2: Surrogate model execution
    # ------------------------------------------------------------------
    if phase2_workloads:
        logger.info("=" * 80)
        logger.info(f"Phase 2: Surrogate-based tuning - up to {len(phase2_workloads)} workloads")
        logger.info("=" * 80)

        cost_model = CostModel(knob_settings=knob_settings)
        cost_model.load_model(surrogate_config.model_path)

        wk_feature_dir = Path("data/workload_features") / benchmark_config.name

        for idx, workload in enumerate(phase2_workloads):
            workload_id = os.path.splitext(workload)[0]
            # if workload_id in completed or workload in completed:
            #     skipped += 1
            #     logger.info(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Skipping completed: {workload}")
            #     continue

            workload_path = Path(workload_base_path) / workload
            output_dir = (
                Path("data")
                / cli_args.dbengine
                / cli_args.servername
                / benchmark_config.name
                / workload_id
            )
            os.makedirs(output_dir, exist_ok=True)
            log_path = Path(
                f"logs/tuning/{workload_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )

            # --- Start: Ported Default Data Collection from Phase 1 ---
            collected_data_file = output_dir / "collected_data.json"
            best_config_file = output_dir / "best_config.json"

            if best_config_file.exists():
                ## if the best config file configuration contains autovacuum_analyze_scale_factor skip
                try:
                    with open(best_config_file, "r") as f:
                        best_config = json.load(f)
                    if "autovacuum_analyze_scale_factor" in best_config.get("configuration", {}):
                        continue
                except Exception as e:
                    logger.warning(f"Error reading {best_config_file}: {e}")


            skip_collection = True
            if collected_data_file.exists():
                try:
                    with open(collected_data_file, "r") as f:
                        cdata = json.load(f)
                    
                    query_plans = cdata.get("query_plans", {})
                    internal_metrics = cdata.get("internal_metrics", {})
                    blks_read = internal_metrics.get("blks_read", 0)
                    
                    if query_plans and blks_read != 0:
                        skip_collection = True
                except Exception as e:
                    logger.warning(f"Error reading {collected_data_file}: {e}")

            if skip_collection:
                logger.info(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Found valid existing collected_data.json for {workload}. Skipping default data collection.")
            else:
                utils.send_telegram(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Starting default data collection for surrogate: *{workload}*")
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
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                future = executor.submit(ddc.collect)
                try:
                    future.result(timeout=600)  # 30 minutes
                    executor.shutdown(wait=False)
                except concurrent.futures.TimeoutError:
                    executor.shutdown(wait=False)
                    logger.error(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Default data collection timed out after 10m for {workload}. Skipping.")
                    utils.send_telegram(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Default data collection timed out for: *{workload}*. Skipping.")
                    skipped += 1
                    continue
                except Exception as e:
                    executor.shutdown(wait=False)
                    logger.error(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Default data collection failed for {workload}: {e}")
                    utils.send_telegram(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Default data collection failed for: *{workload}* - {e}")
                    failed += 1
                    continue
            
            # Load collected data to feed into the surrogate CostModel
            if collected_data_file.exists():
                with open(collected_data_file, "r") as f:
                    cdata = json.load(f)
                
                context_features = {}
                # Extract internal metrics (im_)
                for k, v in cdata.get("internal_metrics", {}).items():
                    context_features[f"im_{k}"] = float(v) if v is not None else 0.0
                
                # Extract workload features (wf_, op_)
                for k, v in cdata.get("workload_features", {}).items():
                    if isinstance(v, dict):
                        prefix = "op_" if "operator" in k.lower() else "tbl_"
                        for sub_k, sub_v in v.items():
                            # The model expects consistent naming, e.g. op_group_by
                            formatted_k = sub_k.lower().replace(" ", "_") if prefix == "op_" else sub_k
                            context_features[f"{prefix}{formatted_k}"] = float(sub_v) if sub_v is not None else 0.0
                    else:
                        context_features[f"wf_{k}"] = float(v) if v is not None else 0.0
                
                cost_model.workload_features[workload_id] = context_features
            else:
                logger.warning(f"No collected_data.json found for {workload_id}. Surrogate model will use zeroed context features.")
            # --- End: Ported Default Data Collection ---

            try:
                logger.info("-" * 80)
                logger.info(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Tuning (surrogate): {workload}")
                tuner = build_tuner(
                    cost_model,
                    script_config,
                    knob_settings,
                    workload_path,
                    output_dir,
                    log_path,
                    tuning_parameter,
                )
                tuner.tune()
                successful += 1
                logger.info(f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Completed: {workload}")
            except Exception as e:
                failed += 1
                logger.error(
                    f"[Phase-2 {idx + 1}/{len(phase2_workloads)}] Error tuning {workload}: {e}",
                    exc_info=True,
                )

    # Summary
    logger.info("=" * 100)
    logger.info("TUNING SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Total workloads: {total_workloads}")
    logger.info(f"Successful: {successful}  Skipped: {skipped}  Failed: {failed}")
    logger.info("=" * 100)
    logger.info("E2ETune session completed")
    logger.info("=" * 100)
    utils.send_telegram(f"E2ETune session completed for benchmark: *{benchmark_config.name}*")
