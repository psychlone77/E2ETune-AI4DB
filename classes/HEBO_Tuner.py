import json
import os

from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Script_Config import TuningConfig
from classes.base_classes.Tuner import Tuner
from classes.base_classes.Workload_Runner import BenchmarkTask, WorkloadRunner
from classes.base_classes.Knob_Settings import KnobSettingsSet
from classes.Global_Vars import TuningParameter
from typing import List, Optional, Literal
from pathlib import Path
import utils

import numpy as np
import pandas as pd
from math import ceil
from hebo.design_space.design_space import DesignSpace
from hebo.optimizers.hebo import HEBO


class HEBOTuner(Tuner):
    def __init__(
        self,
        workload_runner: WorkloadRunner,
        tuning_config: TuningConfig,
        tuning_parameter: TuningParameter,
        workload_task: BenchmarkTask,
        knob_settings: KnobSettingsSet,
        output_dir: Path,
        log_path: Optional[Path] = None,
    ) -> None:
        self.workload_runner = workload_runner
        self.tuning_config = tuning_config
        self.tuning_parameter = tuning_parameter
        self.workload_task = workload_task
        self.knob_settings = knob_settings
        self.output_dir = output_dir
        self.logger = utils.get_logger(log_path)

    @staticmethod
    def _get_tunable_knobs(config: KnobConfig, params) -> pd.DataFrame:
        param_names = [p["name"] for p in params]
        tunable = {knob: value for knob, value in config.items() if knob in param_names}
        dataframe = pd.DataFrame([tunable])
        return dataframe

    @staticmethod
    def _get_perf_ndarray(performance: float) -> np.ndarray:
        return np.array([[performance]])

    def tune(self) -> KnobConfig:
        self.logger.info("=" * 80)
        self.logger.info("Starting HEBO tuning process")
        self.logger.info(f"Workload: {self.workload_task.workload_path}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Tuning parameter: {self.tuning_parameter}")
        self.logger.info(f"Number of iterations: {self.tuning_config.iterations}")
        self.logger.info(f"Sample size: {self.tuning_config.sample_num}")
        self.logger.info("=" * 80)

        params = self._make_params()
        self.logger.info(f"Created parameter space with {len(params)} tunable knobs")

        self.logger.info(f"Created output directory: {self.output_dir}")

        design_space = DesignSpace().parse(params)
        self.logger.info("Initialized HEBO design space")

        hebo = HEBO(
            design_space,
            rand_sample=self.tuning_config.sample_num,
            model_config={
                "lr": 0.01,
                "num_epochs": 100,
                "verbose": False,
                "noise_lb": 1e-3,  # Increased from 8e-4
                "pred_likeli": False,
            },
        )
        self.logger.info("Initialized HEBO optimizer")

        history_file = f"{self.output_dir}/run_history.jsonl"
        best_config_file = f"{self.output_dir}/best_config.json"
        performance_record_file = f"{self.output_dir}/performance_record.txt"

        # Evaluate default configuration
        self.logger.info("-" * 80)
        self.logger.info("Evaluating default configuration...")
        default_config = self.knob_settings.get_default_knob_settings()
        self.workload_task.knob_config = default_config
        default_results = self.workload_runner.run_workload(self.workload_task)
        default_performance = default_results[self.tuning_parameter.value]
        self.logger.info(
            f"Default configuration performance: {default_performance:.6f}"
        )

        default_config_df = self._get_tunable_knobs(default_config, params)
        default_performance_array = self._get_perf_ndarray(default_performance)

        hebo.observe(
            default_config_df,
            default_performance_array,
        )
        self.logger.info("Registered default configuration with HEBO")

        best_config: KnobConfig = default_config
        best_objective: float = default_performance

        super().save_tuning_history(
            history_file, default_config, default_performance, True
        )
        super().save_best_config(
            best_config_file,
            self.tuning_parameter,
            default_performance,
            best_objective,
            best_config,
        )
        super().write_performance_record(
            performance_record_file, 0, self.tuning_parameter, default_performance, True
        )
        self.logger.info("Saved initial results to files")
        self.logger.info("-" * 80)

        if self.tuning_parameter == TuningParameter.LATENCY:
            avg_latency_sec = abs(default_results[TuningParameter.LATENCY.value])
            # Total observation time for the default run
            total_time_observed = avg_latency_sec * utils.get_num_queries(self.workload_task.workload_path)
            self.logger.info("Estimated time per iteration: "
                            f"{total_time_observed:.2f} seconds "
                            f"({avg_latency_sec:.4f}s latency * {utils.get_num_queries(self.workload_task.workload_path)} queries)")
            utils.send_telegram(
                f"Estimated time per iteration: {total_time_observed:.2f} seconds "
                f"({avg_latency_sec:.4f}s latency * {utils.get_num_queries(self.workload_task.workload_path)} queries)"
            )

            # Configuration
            TARGET_STABILITY_TIME = 10
            MAX_REPETITIONS = 15

            if total_time_observed < TARGET_STABILITY_TIME:
                # Calculate how many runs we need to hit our 5-second target
                needed_runs = ceil(TARGET_STABILITY_TIME / max(total_time_observed, 0.001))
                
                # Constrain the result to a reasonable range
                runs_per_iteration = max(2, min(needed_runs, MAX_REPETITIONS))
                
                self.logger.info(f"Short run detected ({total_time_observed:.2f}s). "
                                f"Adjusting to {runs_per_iteration} runs to hit "
                                f"{TARGET_STABILITY_TIME}s stability target.")
                utils.send_telegram(
                    f"Short run detected ({total_time_observed:.2f}s). "
                    f"Adjusting to {runs_per_iteration} runs to hit "
                    f"{TARGET_STABILITY_TIME}s stability target."
                )
            else:
                runs_per_iteration = 1
        else:
            runs_per_iteration = 1
            self.logger.info("Non-latency tuning parameter detected, using 1 run per iteration")

        try:
            for iteration in range(self.tuning_config.iterations):
                self.logger.info(
                    f"[HEBO Iteration {iteration + 1}/{self.tuning_config.iterations}]"
                )

                suggestion = hebo.suggest(n_suggestions=1)
                config_dict = suggestion.iloc[0].to_dict()

                self.workload_task.knob_config = KnobConfig.from_dict(
                    config_dict, self.knob_settings
                )

                cur_objective = self.workload_runner.run_workload(self.workload_task, runs_per_iteration)[
                    self.tuning_parameter.value
                ]
                self.logger.info(f"Performance: {cur_objective:.6f}")

                config_df = self._get_tunable_knobs(
                    self.workload_task.knob_config, params
                )
                performance_array = self._get_perf_ndarray(cur_objective)
                hebo.observe(config_df, performance_array)

                super().save_tuning_history(
                    history_file, self.workload_task.knob_config, cur_objective
                )
                super().write_performance_record(
                    performance_record_file,
                    iteration + 1,
                    self.tuning_parameter,
                    cur_objective,
                )

                if cur_objective < best_objective:
                    improvement = (
                        (best_objective - cur_objective) / best_objective
                    ) * 100
                    self.logger.info(
                        f"✓ NEW BEST! Improved by {improvement:.2f}% (previous: {best_objective:.6f}, current: {cur_objective:.6f})"
                    )
                    utils.send_telegram(
                        f"HEBO Iteration {iteration + 1}: New Best Configuration Found!\n"
                        f"Performance: {cur_objective:.6f} ({improvement:.2f}% improvement)"
                    )
                    best_config = self.workload_task.knob_config
                    best_objective = cur_objective

                self.logger.info("-" * 80)

        except Exception as e:
            self.logger.error(f"[HEBO] Error during optimization: {e}", exc_info=True)
            if best_config is None:
                self.logger.warning("No valid configuration found, using default")
                best_config = self.knob_settings.get_default_knob_settings()

        super().save_best_config(
            best_config_file,
            self.tuning_parameter,
            default_performance,
            best_objective,
            best_config,
        )
        self.logger.info(f"Saved final best configuration to: {best_config_file}")

        # Final summary
        self.logger.info("=" * 80)
        self.logger.info("HEBO Tuning Complete")
        self.logger.info(f"Best performance achieved: {best_objective:.6f}")
        if default_performance != 0:
            total_improvement = (
                (default_performance - best_objective) / default_performance
            ) * 100
            self.logger.info(
                f"Total improvement over default: {total_improvement:.2f}%"
            )
        self.logger.info(f"Results saved to: {self.output_dir}")
        self.logger.info("=" * 80)
        utils.send_telegram(
            f"HEBO Tuning Complete for {self.workload_task.workload_path}\n"
            f"Best {self.tuning_parameter}: {best_objective:.6f}\n"
            f"Improvement over default: {total_improvement:.2f}%"
        )

        return best_config

    def _make_params(self) -> List[dict]:
        params = []
        for knob in self.knob_settings.knobs:
            if knob.type == "integer":
                params.append(
                    {
                        "name": knob.name,
                        "type": "int",
                        "lb": int(knob.min),
                        "ub": int(knob.max),
                    }
                )
            elif knob.type == "float":
                params.append(
                    {
                        "name": knob.name,
                        "type": "num",
                        "lb": float(knob.min),
                        "ub": float(knob.max),
                    }
                )
        if not params:
            raise ValueError("No valid knobs found for tuning.")
        return params
