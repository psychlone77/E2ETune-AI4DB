from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from classes.Knob_Settings import KnobSettingsSet


@dataclass
class BenchmarkTask:
    """
    A unified container for all data needed to run a benchmark.
    """

    workload_path: Path
    knob_config: KnobSettingsSet
    query_plans: Optional[Dict[str, Any]] = None
    internal_metrics: Optional[Dict[str, Any]] = None


class WorkloadRunner(ABC):
    @abstractmethod
    def __init__(self):
        super().__init__()

    @abstractmethod
    def run_workload(self, workload_task: BenchmarkTask) -> float:
        """Run the workload with the given configuration and return the performance metric.
        Args:
            workload_task: The BenchmarkTask containing all necessary information to run the workload.
        Returns:
            The performance metric (e.g., latency, throughput) obtained from running the workload.
        """
        pass
