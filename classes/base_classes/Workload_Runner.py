from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from classes.base_classes.Internal_Metrics import InternalMetrics
from classes.base_classes.Knob_Config import KnobConfig


@dataclass
class BenchmarkTask:
    """
    A unified container for all data needed to run a benchmark.
    """

    workload_path: Path
    knob_config: KnobConfig
    query_plans: Optional[Dict[str, Any]] = None
    internal_metrics: InternalMetrics = None


class WorkloadRunner(ABC):
    """An abstract base class for running database workloads. This class defines the interface for executing workloads and fetching performance metrics, which can be implemented by specific database classes like PostgresSQLDatabase.

    Methods:
        run_workload: An abstract method that should be implemented by subclasses to execute the workload and return the performance metric.
    """

    @abstractmethod
    def run_workload(self, workload_task: BenchmarkTask) -> tuple[float, float]:
        """Run the workload with the given configuration and return the average latency and the negative throughput.
        Args:
            workload_task: The BenchmarkTask containing all necessary information to run the workload.
        Returns:
            A tuple containing the [average_latency, -throughput_ps] performance metrics.

        Example usage:
        ```
        workload_task = BenchmarkTask(...)
        runner = WorkloadRunner()
        average_latency, throughput_ps = runner.run_workload(workload_task)
        print(f"average_latency: {average_latency}, -Throughput_ps: {throughput_ps}")
        """
        pass
