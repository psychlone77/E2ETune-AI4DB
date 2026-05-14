from abc import ABC, abstractmethod
import json
from typing import Literal
from classes.base_classes.Knob_Config import KnobConfig
from classes.Global_Vars import TuningParameter


class Tuner(ABC):
    """A class representing a tuner that optimizes database configuration knobs for a given workload.
    This class serves as a base for specific tuning algorithms, such as HEBO, which will implement the actual tuning logic.

    Methods:
        tune: An abstract method that should be implemented by subclasses to perform the tuning process and return the optimized knob configuration.

    Example usage:
    ```
    class MyTuner(Tuner):
        def tune(self):
            # Implement tuning logic here
            return KnobConfig(knobs=[Knob(name="shared_buffers", value="128MB")])
    my_tuner = MyTuner()
    optimized_config = my_tuner.tune()
    print(optimized_config)
    ```
    """

    @abstractmethod
    def tune(self) -> KnobConfig:
        """Tune the database configuration knobs to optimize performance for the given workload.
        Returns:
            A KnobConfig object containing the optimized knob settings.
        """
        pass

    def save_best_config(
            self, 
            filepath: str, 
            tuning_parameter: TuningParameter, 
            default_objective: float, 
            best_objective: float, 
            best_config: KnobConfig
        ) -> None:
        """Save the optimized knob configuration to a JSON file for later use or analysis."""
        perf_improvement = 0
        if tuning_parameter == TuningParameter.LATENCY and default_objective != 0:  # Latency
            perf_improvement = (default_objective - best_objective) / default_objective * 100
        elif tuning_parameter == TuningParameter.THROUGHPUT and default_objective != 0:  # Throughput
            perf_improvement = (best_objective - default_objective) / default_objective * 100
        with open(filepath, "w") as f:
            json.dump(
                {
                    "default_objective": default_objective,
                    "best_cost": best_objective,
                    "performance_improvement": perf_improvement,
                    "configuration": best_config.to_dict(),
                },
                f,
                indent=4,
            )
    
    def save_tuning_history(self, history_file: str, knob_config: KnobConfig, cost: float, overwrite_file: bool = False) -> None:
        """Append tuning history to a JSON file."""
        mode = "w" if overwrite_file else "a"
        with open(history_file, mode) as f:
            json.dump({"config": knob_config.to_dict(), "cost": cost}, f)
            f.write("\n")

    def write_performance_record(self, performance_record_file: str, iteration: int, tuning_parameter: TuningParameter, performance: float, overwrite_file: bool = False) -> None:
        """Write performance record to a text file."""
        mode = "w" if overwrite_file else "a"
        unit = "Latency(s)" if tuning_parameter == TuningParameter.LATENCY else "Throughput(qps/tps)"
        with open(performance_record_file, mode) as f:
            f.write(f"[Iteration {iteration}] Performance: {performance:.4f} {unit}\n")