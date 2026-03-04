import argparse
import os
import yaml
from datetime import datetime
from pathlib import Path

import utils
from classes.base_classes.Script_Config import ScriptConfig
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.HEBO_Tuner import HEBOTuner
from classes.PostgreSQL_Database import PostgresSQLDatabase
from classes.Cost_Model import CostModel
from classes.base_classes.Surrogate_Strategy import SurrogateFactory
from classes.Global_Vars import TuningParameter
from classes.base_classes.Database import Database
from classes.Default_Data_Collector import DefaultDataCollector

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

    # Discover workloads
    workload_base_path = benchmark_config.path
    if not os.path.isdir(workload_base_path):
        logger.error(f"Workload path does not exist: {workload_base_path}")
        exit(1)

    all_files = os.listdir(workload_base_path)
    if benchmark_config.type == "oltp":
        workloads = [
            f for f in all_files if benchmark_config.name in f and f.endswith(".xml")
        ]
    else:
        workloads = [
            f
            for f in all_files
            if f.startswith(benchmark_config.name) and f.endswith(".wg")
        ]

    tuning_parameter = (
        TuningParameter.THROUGHPUT
        if benchmark_config.type == "oltp"
        else TuningParameter.LATENCY
    )

    workloads = utils.natural_sort(workloads)
    total_workloads = len(workloads)
    logger.info(f"Found {total_workloads} workloads matching '{benchmark_config.name}'")

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
    # Phase 1: Real database execution (first REAL_TUNING_LIMIT workloads)
    # ------------------------------------------------------------------
    logger.info(f"Phase 1: Real execution – up to {REAL_TUNING_LIMIT} workloads")
    db: Database = PostgresSQLDatabase(db_config=db_config, log_path=main_log_path)

    for idx, workload in enumerate(workloads[:REAL_TUNING_LIMIT]):
        if idx == 0:
            continue
        workload_id = os.path.splitext(workload)[0]
        if workload_id in completed or workload in completed:
            skipped += 1
            logger.info(
                f"[Phase-1 {idx + 1}/{REAL_TUNING_LIMIT}] Skipping completed: {workload}"
            )
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
        log_path = Path(
            f"logs/tuning/{workload_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        ddc = DefaultDataCollector(
            workload_path=workload_path,
            db=db,
            output_dir=output_dir,
            knob_settings_set=knob_settings,
            log_path=log_path,
        )
        ddc.collect()

        try:
            logger.info("-" * 80)
            logger.info(f"[Phase-1 {idx + 1}/{REAL_TUNING_LIMIT}] Tuning: {workload}")
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
                f"[Phase-1 {idx + 1}/{REAL_TUNING_LIMIT}] Completed: {workload}"
            )
        except Exception as e:
            failed += 1
            logger.error(
                f"[Phase-1 {idx + 1}/{REAL_TUNING_LIMIT}] Error tuning {workload}: {e}",
                exc_info=True,
            )
            if total_workloads >= 10:
                logger.error("Stopping due to error (large workload set)")
                break

    # ------------------------------------------------------------------
    # Phase 2: Surrogate model execution (workloads beyond REAL_TUNING_LIMIT)
    # ------------------------------------------------------------------
    # if total_workloads > REAL_TUNING_LIMIT:
    #     logger.info("=" * 80)
    #     logger.info("Phase 2: Surrogate-based tuning")
    #     logger.info("=" * 80)

    #     strategy = SurrogateFactory.create_strategy("tree_ensemble")
    #     cost_model = CostModel(strategy=strategy, knob_settings=knob_settings)
    #     cost_model.load_model(surrogate_config.model_path)

    #     wk_feature_dir = Path("data/workload_features") / benchmark_config.name

    #     for idx, workload in enumerate(workloads[REAL_TUNING_LIMIT:]):
    #         workload_id = os.path.splitext(workload)[0]
    #         if workload_id in completed or workload in completed:
    #             skipped += 1
    #             logger.info(f"[Phase-2 {idx + 1}] Skipping completed: {workload}")
    #             continue

    #         workload_path = Path(workload_base_path) / workload
    #         output_dir = Path("data/hebo_runs") / benchmark_config.name / workload_id
    #         log_path = Path(
    #             f"logs/tuning/{workload_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    #         )

    #         # # Extract workload features required by the cost model
    #         # process_olap_workload_features(
    #         #     workload_file=str(workload_path),
    #         #     benchmark_name=benchmark_config.name,
    #         #     output_dir=wk_feature_dir,
    #         #     workload_name=workload_id,
    #         # )

    #         try:
    #             logger.info("-" * 80)
    #             logger.info(f"[Phase-2 {idx + 1}] Tuning (surrogate): {workload}")
    #             tuner = build_tuner(
    #                 cost_model,
    #                 script_config,
    #                 knob_settings,
    #                 workload_path,
    #                 output_dir,
    #                 log_path,
    #             )
    #             tuner.tune()
    #             successful += 1
    #             logger.info(f"[Phase-2 {idx + 1}] Completed: {workload}")
    #         except Exception as e:
    #             failed += 1
    #             logger.error(
    #                 f"[Phase-2 {idx + 1}] Error tuning {workload}: {e}",
    #                 exc_info=True,
    #             )

    # Summary
    logger.info("=" * 100)
    logger.info("TUNING SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Total workloads: {total_workloads}")
    logger.info(f"Successful: {successful}  Skipped: {skipped}  Failed: {failed}")
    logger.info("=" * 100)
    logger.info("E2ETune session completed")
    logger.info("=" * 100)
