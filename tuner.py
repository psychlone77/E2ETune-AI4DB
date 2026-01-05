import os
import json
import time
import argparse
from typing import Dict, Any, List

import numpy as np
import pandas as pd
from smac.configspace import ConfigurationSpace
from smac.runhistory.runhistory import RunHistory
from smac.facade.smac_hpo_facade import SMAC4HPO
from smac.scenario.scenario import Scenario
from ConfigSpace.hyperparameters import UniformFloatHyperparameter, UniformIntegerHyperparameter, Constant
from hebo.design_space.design_space import DesignSpace
from hebo.optimizers.hebo import HEBO

from knob_config import parse_knob_config
from Database import Database
from stress_testing_tool import stress_testing_tool
from benchbase_runner import BenchBaseRunner
import utils

class EarlyStopSignal(BaseException):
    """Exception raised to stop SMAC optimization early."""
    pass

def run_tuning(args: Dict[str, Any], use_surrogate: bool = False) -> Dict[str, Any]:
    """
    Orchestrates one workload tuning pass:
    - Performs a default run and logs internal metrics
    - Runs SMAC to find the best configuration
    """
    logger = utils.get_logger(args['tuning_config']['log_path'])
    workload_file = args['benchmark_config']['workload_path']
    workload_name = os.path.basename(workload_file)
    
    logger.info(f"="*80)
    logger.info(f"[Tuning] Starting tuning for workload: {workload_name}")
    logger.info(f"[Tuning] Mode: {'SURROGATE MODEL' if use_surrogate else 'REAL EXECUTION'}")
    logger.info(f"="*80)
    
    internal_metrics = default_run(workload_file, args)

    tuner_instance = Tuner(args, workload_file, internal_metrics, use_surrogate=use_surrogate)
    best_config = tuner_instance.tune()
    
    logger.info(f"[Tuning] Tuning complete for {workload_name}")
    logger.info(f"[Tuning] Best configuration: {best_config}")
    logger.info(f"="*80)
    
    return best_config


def default_run(workload_file: str, args: Dict[str, Any]) -> Dict[str, float]:
    """
    Run the default configuration once to reset stats, execute workload,
    and collect internal metrics for training/logging.
    """
    logger = utils.get_logger(args['tuning_config']['log_path'])
    workload_name = os.path.basename(workload_file)
    
    logger.info(f"[Default Run] [Workload: {workload_name}] Starting default configuration run")
    db = Database(config=args, knob_config_path=args['tuning_config']['knob_config'])

    # Remove potentially bad auto.conf, reset metrics, run workload with defaults
    logger.info(f"[Default Run] Resetting database knobs and metrics to default values")
    db.reset_db_knobs()
    db.reset_inner_metrics()
    db.restart_db()
    
    logger.info(f"[Default Run] Running workload: {workload_file}")
    if args['benchmark_config'].get('tool', 'dwg') == 'benchbase':
        benchbase_runner = BenchBaseRunner(args, logger=logger)
        benchbase_runner.load_database(workload_file)
        benchbase_runner.run_benchmark(workload_file, args['benchmark_config'].get('log_path', 'logs/performance/workload_execution.log'))
    else:
        db.run_workload_with_defaults(workload_file)

    internal_metrics = db.fetch_inner_metrics()
    logger.info(f"[Default Run] Internal metrics collected: {len(internal_metrics)} metrics")

    metrics_file = f"internal_metrics/{args['benchmark_config']['benchmark']}/{args['benchmark_config']['workload_name'].split('.wg')[0]}_internal_metrics.json"

    os.makedirs(os.path.dirname(metrics_file), exist_ok=True)
    with open(metrics_file, "w") as f:
        json.dump(internal_metrics, f, indent=4)
        logger.info(f"[Default Run] Internal metrics saved to: {metrics_file}")

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
        self.workload_name = os.path.basename(workload_file)

        # Initialize database and stress testing tool
        self.db = Database(config=args, knob_config_path=args['tuning_config']['knob_config'])
        training_log = args['tuning_config'].get('training_records', 'logs/offline_sample/training_records.log')
        self.stress_tester = stress_testing_tool(args, self.db, self.logger, records_log=training_log)
        
        self.logger.info(f"[Tuner Init] Initialized for workload: {self.workload_name}")

    def tune(self) -> Dict[str, Any]:
        """Route to appropriate tuning method based on configuration."""
        tuning_method = self.args['tuning_config'].get('tuning_method', 'SMAC').upper()
        
        if tuning_method == 'HEBO':
            self.logger.info(f"[Tuner] Using HEBO optimization method")
            return self._hebo(self.workload_file)
        elif tuning_method == 'SMAC':
            self.logger.info(f"[Tuner] Using SMAC optimization method")
            return self._smac(self.workload_file)
        else:
            self.logger.warning(f"[Tuner] Unknown tuning method '{tuning_method}', defaulting to SMAC")
            return self._smac(self.workload_file)

    def _smac(self, workload_file: str) -> Dict[str, Any]:
        iteration_counter = {'count': 0}
        early_stop_state = {
            'best_cost': float('inf'),  # SMAC minimizes, so track minimum cost
            'iterations_without_improvement': 0,
            'should_stop': False,
        }
        PLATEAU_ITERATIONS = int(self.args['tuning_config'].get('early_stop_plateau', 50))
        
        def objective_function(config) -> float:
            """
            SMAC objective function — returns negative performance (SMAC minimizes).
            Implements early stopping when performance plateaus.
            """
            # Check if we should stop early
            if early_stop_state['should_stop']:
                self.logger.info(f"[SMAC Early Stop] Returning cached best to skip remaining iterations")
                raise EarlyStopSignal()
            
            iteration_counter['count'] += 1
            iteration = iteration_counter['count']
            
            config_dict = dict(config)
            self.logger.info(f"[SMAC Iteration {iteration}] [Workload: {self.workload_name}] Evaluating configuration")
            self.logger.debug(f"[SMAC Iteration {iteration}] Config: {config_dict}")
            
            perf = self.stress_tester.test_config(config_dict, iteration=iteration)
            
            # SMAC minimizes, so use negative QPS/TPS (assuming higher is better)
            result = -perf if perf > 0 else perf
            self.logger.info(f"[SMAC Iteration {iteration}] Performance: {perf:.4f} -> Objective: {result:.4f}")
            
            # Early stopping logic: check if we're making progress
            if result < early_stop_state['best_cost']:
                # Calculate relative improvement
                improvement = abs(result - early_stop_state['best_cost']) / (abs(early_stop_state['best_cost']) + 1e-10)
                early_stop_state['best_cost'] = result
                early_stop_state['iterations_without_improvement'] = 0
                self.logger.info(f"[SMAC Early Stop] New best: {result:.4f} (improvement: {improvement*100:.2f}%)")

            else:
                # No improvement
                early_stop_state['iterations_without_improvement'] += 1
                self.logger.info(f"[SMAC Early Stop] No improvement, "
                               f"plateau counter: {early_stop_state['iterations_without_improvement']}/{PLATEAU_ITERATIONS}")
            
            # Check if we should stop
            if early_stop_state['iterations_without_improvement'] >= PLATEAU_ITERATIONS:
                early_stop_state['should_stop'] = True
                self.logger.info(f"[SMAC Early Stop] Performance plateaued after {PLATEAU_ITERATIONS} iterations. "
                               f"Stopping optimization early at iteration {iteration}.")
            
            return result

        # Build configuration space from knob definitions
        cs = ConfigurationSpace()
        self.logger.info(f"[SMAC Setup] [Workload: {self.workload_name}] Initializing configuration space with {len(self.knobs_detail)} knobs")
        for name, detail in self.knobs_detail.items():
            # CASE 1: FIXED VALUE (Constant)
            if detail['min'] == detail['max']:
                hp = Constant(name, detail['min'])
                cs.add_hyperparameter(hp)
                continue
            # CASE 2: TUNABLE RANGE
            if detail['type'] == 'integer':
                hp = UniformIntegerHyperparameter(
                    name, 
                    lower=detail['min'], 
                    upper=detail['max'], 
                    default_value=detail['default']
                )
            elif detail['type'] == 'float':
                hp = UniformFloatHyperparameter(
                    name, 
                    lower=detail['min'], 
                    upper=detail['max'], 
                    default_value=detail['default']
                )
            else:
                continue
            cs.add_hyperparameter(hp)

        runhistory = RunHistory()

        # Determine save identifier
        if workload_file.endswith('.xml'):
            save_workload = os.path.splitext(os.path.basename(workload_file))[0]
        else:
            save_workload = self.args['benchmark_config']['workload_name'].split('.wg')[0]

        benchmark_name = self.args['benchmark_config']['benchmark']
        os.makedirs(f"./{benchmark_name}", exist_ok=True)
        os.makedirs(f"./models/{benchmark_name}", exist_ok=True)
        os.makedirs("smac_his", exist_ok=True)

        runcount = int(self.args['tuning_config'].get('suggest_num', 100))
        self.logger.info(f"[SMAC Setup] [Workload: {self.workload_name}] Configuring SMAC with {runcount} evaluations")
        
        scenario = Scenario({
            "run_obj": "quality",
            "runcount-limit": runcount,
            "cs": cs,
            "deterministic": "true",
            "output_dir": f"./{benchmark_name}/{save_workload}_smac_output",  
            "save_model": "true",
            "local_results_path": f"./models/{benchmark_name}/{save_workload}",
        })
        
        self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Starting SMAC optimization")

        ### Initialize SMAC4HPO facade
        smac = SMAC4HPO(scenario=scenario, 
                    rng=np.random.RandomState(42), 
                    tae_runner=objective_function, 
                    runhistory=runhistory
                )
        
        try:
            incumbent = smac.optimize()
        except EarlyStopSignal:
            self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Caught EarlyStopSignal, ending optimization early")
            incumbent = smac.solver.incumbent
        
        # Log whether optimization stopped early or ran to completion
        if early_stop_state['should_stop']:
            self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Optimization stopped early due to performance plateau")
            self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Completed {iteration_counter['count']} iterations (limit: {runcount})")
        else:
            self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Optimization completed all {runcount} iterations")
        
        self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Best configuration: {dict(incumbent)}")

        # Save best configuration to file
        best_config_dict = dict(incumbent)
        best_config_file = f"./{benchmark_name}/{save_workload}_smac_output/best_config.json"
        with open(best_config_file, "w") as f:
            json.dump({
                "workload": save_workload,
                "iterations": iteration_counter['count'],
                "early_stopped": early_stop_state['should_stop'],
                "best_cost": early_stop_state['best_cost'],
                "configuration": best_config_dict
            }, f, indent=4)
        self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Best configuration saved to: {best_config_file}")

        def runhistory_to_json(runhistory_obj) -> str:
            """Serialize a SMAC RunHistory to JSON."""
            data_to_save = {}
            # RunHistory.data maps run_key -> RunValue
            for run_key, run_value in runhistory_obj.data.items():
                try:
                    config_id, instance_id, seed, budget = run_key
                except Exception:
                    # Fallback if run_key is not a 4-tuple
                    config_id = run_key
                    instance_id = seed = budget = None

                data_to_save[str(run_key)] = {
                    "cost": getattr(run_value, "cost", None),
                    "time": getattr(run_value, "time", None),
                    "status": getattr(getattr(run_value, "status", None), "name", str(getattr(run_value, "status", None))),
                    "additional_info": getattr(run_value, "additional_info", None),
                }

            return json.dumps(data_to_save, indent=4)

        history_file = f"smac_his/{save_workload}_smac.json"
        with open(history_file, "w") as f:
            f.write(runhistory_to_json(smac.runhistory))
        self.logger.info(f"[SMAC] [Workload: {self.workload_name}] Runhistory saved to: {history_file}")

        # Return best config as dict for downstream usage
        return dict(incumbent)

    def _hebo(self, workload_file: str) -> Dict[str, Any]:
        """
        HEBO-based tuning that evaluates PostgreSQL knob configurations.
        Similar to SMAC but uses HEBO's Bayesian optimization.
        """
        iteration_counter = {'count': 0}
        
        # Build HEBO design space from knob definitions
        params = []
        self.logger.info(f"[HEBO Setup] [Workload: {self.workload_name}] Initializing design space with {len(self.knobs_detail)} knobs")
        
        for name, detail in self.knobs_detail.items():
            # CASE 1: FIXED VALUE (Constant)
            if detail['min'] == detail['max']:
                # HEBO doesn't handle constants well, we'll handle this separately
                continue
            
            # CASE 2: TUNABLE RANGE
            if detail['type'] == 'integer':
                params.append({
                    'name': name,
                    'type': 'int',
                    'lb': int(detail['min']),
                    'ub': int(detail['max'])
                })
            elif detail['type'] == 'float':
                params.append({
                    'name': name,
                    'type': 'num',
                    'lb': float(detail['min']),
                    'ub': float(detail['max'])
                })
        
        if not params:
            self.logger.error(f"[HEBO Setup] No tunable parameters found!")
            return {name: detail['default'] for name, detail in self.knobs_detail.items()}
        
        design_space = DesignSpace().parse(params)
        self.logger.info(f"[HEBO Setup] Design space created with {len(params)} tunable parameters")
        
        # Determine save identifier
        if workload_file.endswith('.xml'):
            save_workload = os.path.splitext(os.path.basename(workload_file))[0]
        else:
            save_workload = self.args['benchmark_config']['workload_name'].split('.wg')[0]
        
        benchmark_name = self.args['benchmark_config']['benchmark']
        os.makedirs(f"./{benchmark_name}", exist_ok=True)
        os.makedirs(f"./models/{benchmark_name}", exist_ok=True)
        
        output_dir = f"./{benchmark_name}/{save_workload}_hebo_output"
        os.makedirs(output_dir, exist_ok=True)
        
        runcount = int(self.args['tuning_config'].get('suggest_num', 100))
        self.logger.info(f"[HEBO Setup] [Workload: {self.workload_name}] Configuring HEBO with {runcount} evaluations")
        
        # Initialize HEBO optimizer with increased model noise for numerical stability
        # model_config can be passed to control GP behavior
        hebo = HEBO(design_space, 
            rand_sample=len(params)*2,
            model_config={
                "lr": 0.01,
                "num_epochs": 100,
                "verbose": False,
                "noise_lb": 1e-3,  # Increased from 8e-4
                "pred_likeli": False,
            }
        ) 
        
        # Open history file in JSONL format for writing iterations as they happen
        history_file = f"{output_dir}/runhistory.jsonl"
        best_config = None
        best_objective = float('inf')

        # EVALUATE DEFAULT CONFIGURATION FIRST (Iteration 0)
        self.logger.info(f"[HEBO] [Workload: {self.workload_name}] Starting HEBO optimization")

        self.logger.info(f"[HEBO Iteration 0] [Workload: {self.workload_name}] Evaluating DEFAULT configuration")
    
        # Build default config with all knobs (including constants)
        default_config = {name: detail['default'] for name, detail in self.knobs_detail.items()}
        self.logger.debug(f"[HEBO Iteration 0] Default Config: {default_config}")
        
        # Evaluate default configuration
        default_perf = self.stress_tester.test_config(default_config, iteration=0)
        default_objective = -default_perf if default_perf > 0 else default_perf
        self.logger.info(f"[HEBO Iteration 0] Default Perf: {default_perf:.4f} -> Objective: {default_objective:.4f}")
        
        # Extract only tunable parameters for HEBO observation
        default_tunable = {k: v for k, v in default_config.items() if k in [p['name'] for p in params]}
        default_df = pd.DataFrame([default_tunable])
        default_objective_df = np.array([[default_objective]])
        
        # Feed default config to HEBO
        hebo.observe(default_df, default_objective_df)
        
        # Initialize best with default
        best_config = default_config.copy()
        best_objective = default_objective
        
        # Write default to history
        with open(history_file, 'w') as f:
            json.dump({
                'iteration': 0,
                'config': default_config,
                'cost': default_objective,
                'performance': default_perf,
                'note': 'DEFAULT_CONFIG'
            }, f)
            f.write('\n')
        
        self.logger.info(f"[HEBO Iteration 0] Default config set as baseline (objective: {default_objective:.4f})")

        
        try:
            for iteration in range(runcount):

                iteration_counter['count'] = iteration + 1
                
                # Get suggestion from HEBO
                suggestion = hebo.suggest(n_suggestions=1)
                config_dict = suggestion.iloc[0].to_dict()
                
                # Add constant knobs back to config
                for name, detail in self.knobs_detail.items():
                    if detail['min'] == detail['max']:
                        config_dict[name] = detail['min']
                
                self.logger.info(f"[HEBO Iteration {iteration+1}] [Workload: {self.workload_name}] Evaluating configuration")
                self.logger.debug(f"[HEBO Iteration {iteration+1}] Config: {config_dict}")
                
                # Evaluate configuration
                perf = self.stress_tester.test_config(config_dict, iteration=iteration+1)
                
                # HEBO minimizes, so use negative QPS/TPS (assuming higher is better)
                objective = -perf if perf > 0 else perf
                self.logger.info(f"[HEBO Iteration {iteration+1}] Raw Perf: {perf:.4f} -> Log Objective: {objective:.4f}")
                
                # Observe the result (only include tunable parameters)
                observation_dict = {k: v for k, v in config_dict.items() if k in [p['name'] for p in params]}
                observation_df = pd.DataFrame([observation_dict])
                objective_df = np.array([[objective]])
                hebo.observe(observation_df, objective_df)
                
                # Write iteration to history file immediately in JSONL format
                with open(history_file, 'a') as f:
                    json.dump({
                        'config': config_dict,
                        'cost': objective
                    }, f)
                    f.write('\n')
                
                # Update best configuration
                if objective < best_objective:
                    improvement = abs(objective - best_objective) / (abs(best_objective) + 1e-10)
                    best_objective = objective
                    best_config = config_dict.copy()
                    self.logger.info(f"[HEBO Early Stop] New best: {objective:.4f} (improvement: {improvement*100:.2f}%)")
        
        except Exception as e:
            self.logger.error(f"[HEBO] Error during optimization: {e}")
            if best_config is None:
                best_config = {name: detail['default'] for name, detail in self.knobs_detail.items()}
        
        # Log completion status
        self.logger.info(f"[HEBO] [Workload: {self.workload_name}] Optimization completed all {runcount} iterations")
        
        self.logger.info(f"[HEBO] [Workload: {self.workload_name}] Best configuration: {best_config}")
        
        # Save best configuration to file
        best_config_file = f"{output_dir}/best_config.json"
        with open(best_config_file, "w") as f:
            json.dump({
                "workload": save_workload,
                "iterations": iteration_counter['count'],
                "best_cost": best_objective,
                "best_performance": -best_objective,
                "configuration": best_config
            }, f, indent=4)
        self.logger.info(f"[HEBO] [Workload: {self.workload_name}] Best configuration saved to: {best_config_file}")
        self.logger.info(f"[HEBO] [Workload: {self.workload_name}] Run history saved to: {history_file}")
        
        return best_config


