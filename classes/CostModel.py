from abc import ABC, abstractmethod
from classes.Workload_Runner import BenchmarkTask
from classes.Workload_Runner import WorkloadRunner


class CostModel(WorkloadRunner):
    def __init__(self, config):
        super().__init__(config)

    def run_workload(self, workload_task: BenchmarkTask) -> float:
        """Run the workload with the given configuration and return the performance metric.
        Args:
            workload_task: The BenchmarkTask containing all necessary information to run the workload.
        Returns:
            The performance metric (e.g., latency, throughput) obtained from running the workload.
        """
        pass
