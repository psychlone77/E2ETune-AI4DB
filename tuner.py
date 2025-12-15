import os
import json
import time
import argparse
from typing import Dict, Any, List

import numpy as np
from smac.configspace import ConfigurationSpace
from smac.runhistory.runhistory import RunHistory
from smac.facade.smac_hpo_facade import SMAC4HPO
from smac.scenario.scenario import Scenario
from ConfigSpace.hyperparameters import UniformFloatHyperparameter, UniformIntegerHyperparameter

from knob_config import parse_knob_config
from Database import Database
from workload_executor import workload_executor
import utils


def default_run(workload_file: str, args: Dict[str, Any]) -> Dict[str, float]:
    """
    Run the default configuration once to reset stats, execute workload,
    and collect internal metrics for training/logging.
    """
    logger = utils.get_logger(args['tuning_config']['log_path'])
    db = Database(config=args, knob_config_path=args['tuning_config']['knob_config'])

    # Remove potentially bad auto.conf, reset metrics, run workload with defaults
    db.remove_auto_conf()
    print("Resetting inner metrics...")
    db.reset_inner_metrics()
    print(f"Running default workload: {workload_file} ...")
    db.run_workload_with_defaults(workload_file)

    internal_metrics = db.fetch_inner_metrics()
    print(f"Internal metrics collected: {internal_metrics}")

    # Persist internal metrics in an organized directory structure
    benchmark_name = args['benchmark_config']['benchmark']
    if benchmark_name in ['tpcc', 'ycsb', 'smallbank', 'wikipedia', 'twitter']:
        base_name = os.path.splitext(os.path.basename(workload_file))[0]
        metrics_dir = f"internal_metrics/{benchmark_name}"
        os.makedirs(metrics_dir, exist_ok=True)
        metrics_file = f"{metrics_dir}/{base_name}_internal_metrics.json"
    else:
        metrics_file = f"internal_metrics/{workload_file.split('.wg')[0]}_internal_metrics.json"

    os.makedirs(os.path.dirname(metrics_file), exist_ok=True)
    with open(metrics_file, "w") as f:
        json.dump(internal_metrics, f, indent=4)
    print(f"Internal metrics saved to: {metrics_file}")

    return internal_metrics


class Tuner:
    """
    SMAC-based tuner that evaluates PostgreSQL knob configurations either
    by real execution or via a surrogate model through workload_executor.
    """
    def __init__(self, args: Dict[str, Any], workload_file: str, internal_metrics: Dict[str, float], use_surrogate: bool = False):
        self.args = args
        self.workload_file = workload_file
        self.knobs_detail = parse_knob_config.get_knobs(args['tuning_config']['knob_config'])
        self.logger = utils.get_logger(args['tuning_config']['log_path'])
        self.internal_metrics = internal_metrics
        self.use_surrogate = use_surrogate

        # Executor handles running configs (real or surrogate)
        self.executor = workload_executor(args, self.logger, "training_records.log", self.internal_metrics)

    def tune(self) -> Dict[str, Any]:
        return self._smac(self.workload_file)

    def _smac(self, workload_file: str) -> Dict[str, Any]:
        def objective_function(config) -> float:
            """
            SMAC objective function — returns negative performance (SMAC minimizes).
            """
            config_dict = dict(config)
            print(f"Evaluating configuration: {config_dict}")
            perf = self.executor.run_config_surrogate(config_dict, workload_file) if self.use_surrogate \
                else self.executor.run_config(config_dict, workload_file)
            # SMAC minimizes, so use negative QPS/TPS (assuming higher is better)
            result = -perf if perf > 0 else perf
            print(f"Performance (QPS/TPS): {perf} -> objective {result}")
            return result

        # Build configuration space from knob definitions
        cs = ConfigurationSpace()
        print("Initializing configuration space")
        for name, detail in self.knobs_detail.items():
            if detail['type'] == 'integer':
                max_v = detail['max'] if detail['max'] != detail['min'] else detail['min'] + 1
                hp = UniformIntegerHyperparameter(name, detail['min'], max_v, default_value=detail['default'])
            elif detail['type'] == 'float':
                hp = UniformFloatHyperparameter(name, detail['min'], detail['max'], default_value=detail['default'])
            else:
                # Skip enums here; extend if needed with CategoricalHyperparameter
                continue
            cs.add_hyperparameter(hp)

        runhistory = RunHistory()

        # Determine save identifier
        if workload_file.endswith('.xml'):
            save_workload = os.path.splitext(os.path.basename(workload_file))[0]
        else:
            save_workload = workload_file.split('.wg')[0]

        benchmark_name = self.args['benchmark_config']['benchmark']
        os.makedirs(f"./{benchmark_name}", exist_ok=True)
        os.makedirs(f"./models/{benchmark_name}", exist_ok=True)
        os.makedirs("smac_his", exist_ok=True)

        scenario = Scenario({
            "run_obj": "quality",
            "runcount-limit": int(self.args['tuning_config'].get('suggest_num', 100)),
            "cs": cs,
            "deterministic": "true",
            "output_dir": f"./{benchmark_name}/{save_workload}_smac_output",
            "save_model": "true",
            "local_results_path": f"./models/{benchmark_name}/{save_workload}"
        })

        smac = SMAC4HPO(scenario=scenario, rng=np.random.RandomState(42), tae_runner=objective_function, runhistory=runhistory)
        incumbent = smac.optimize()
        print("SMAC optimization finished")
        print(f"Incumbent: {incumbent}")

        # Save runhistory for inspection
        def runhistory_to_json(rh: RunHistory) -> str:
            data_to_save = {}
            for run_key, run_value in rh.data.items():
                config_id, instance_id, seed, budget = run_key
                data_to_save[str(run_key)] = {
                    "cost": run_value.cost,
                    "time": run_value.time,
                    "status": run_value.status.name,
                    "additional_info": run_value.additional_info
                }
            return json.dumps(data_to_save, indent=4)

        with open(f"smac_his/{save_workload}_smac.json", "w") as f:
            f.write(runhistory_to_json(smac.runhistory))

        # Return best config as dict for downstream usage
        return dict(incumbent)


def run_tuning(args: Dict[str, Any], use_surrogate: bool = False) -> Dict[str, Any]:
    """
    Orchestrates one workload tuning pass:
    - Performs a default run and logs internal metrics
    - Runs SMAC to find the best configuration
    """
    workload_file = args['benchmark_config']['workload_path']
    internal_metrics = default_run(workload_file, args)
    print(f"Starting tuning for workload: {workload_file}")
    print("Using SURROGATE MODEL" if use_surrogate else "Using REAL EXECUTION")

    tuner_instance = Tuner(args, workload_file, internal_metrics, use_surrogate=use_surrogate)
    best_config = tuner_instance.tune()
    print(f"Tuning complete for {workload_file}")
    return best_config

