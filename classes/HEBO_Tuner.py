import json
import os

from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Tuner import Tuner
from classes.base_classes.Script_Config import TuningConfig
from classes.base_classes.Workload_Runner import BenchmarkTask, WorkloadRunner
from classes.base_classes.Knob_Settings import KnobSettingsSet
from typing import List, Optional, Literal
from pathlib import Path
import utils

import numpy as np
import pandas as pd
from hebo.design_space.design_space import DesignSpace
from hebo.optimizers.hebo import HEBO


class HEBOTuner(Tuner):
    def __init__(
        self,
        workload_runner: WorkloadRunner,
        tuning_config: TuningConfig,
        tuning_parameter: Literal[0, 1],
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
        self.logger = utils.get_logger(log_path) if log_path is not None else None

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
        params = self._make_params()
        os.makedirs(self.output_dir, exist_ok=True)
        design_space = DesignSpace().parse(params)
        hebo = HEBO(
            design_space,
            rand_sample=len(params) * 2,
            model_config={
                "lr": 0.01,
                "num_epochs": 100,
                "verbose": False,
                "noise_lb": 1e-3,  # Increased from 8e-4
                "pred_likeli": False,
            },
        )

        history_file = f"{self.output_dir}/run_history.jsonl"

        default_config = self.knob_settings.get_default_knob_settings()
        self.workload_task.knob_config = default_config
        default_performance = self.workload_runner.run_workload(self.workload_task)[
            self.tuning_parameter
        ]
        default_config_df = self._get_tunable_knobs(default_config, params)
        default_performance_array = self._get_perf_ndarray(default_performance)

        hebo.observe(
            default_config_df,
            default_performance_array,
        )

        best_config: KnobConfig = default_config
        best_objective: float = default_performance
        with open(history_file, "w") as f:
            json.dump(
                {
                    "config": default_config.to_dict(),
                    "cost": default_performance,
                },
                f,
            )
            f.write("\n")

        try:
            for iteration in range(self.tuning_config.suggest_num):
                suggestion = hebo.suggest(n_suggestions=1)
                config_dict = suggestion.iloc[0].to_dict()
                self.workload_task.knob_config = KnobConfig.from_dict(config_dict)
                cur_objective = self.workload_runner.run_workload(self.workload_task)[
                    self.tuning_parameter
                ]

                config_df = self._get_tunable_knobs(
                    self.workload_task.knob_config, params
                )
                performance_array = self._get_perf_ndarray(cur_objective)
                hebo.observe(config_df, performance_array)

                with open(history_file, "a") as f:
                    json.dump({"config": config_dict, "cost": cur_objective}, f)
                    f.write("\n")
                if cur_objective < best_objective:
                    best_config = self.workload_task.knob_config
                    best_objective = cur_objective

        except Exception as e:
            self.logger.error(f"[HEBO] Error during optimization: {e}")
            if best_config is None:
                best_config = self.knob_settings.get_default_knob_settings()

        best_config_file = f"{self.output_dir}/best_config.json"
        with open(best_config_file, "w") as f:
            json.dump(
                {
                    "workload": self.workload_task.workload_path,
                    "best_cost": best_objective,
                    "best_performance": -best_objective,
                    "configuration": best_config.to_dict(),
                },
                f,
                indent=4,
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
                        "type": "float",
                        "lb": float(knob.min),
                        "ub": float(knob.max),
                    }
                )
        if not params:
            raise ValueError("No valid knobs found for tuning.")
        return params
