from abc import abstractmethod
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Workload_Runner import WorkloadRunner
from classes.base_classes.Internal_Metrics import InternalMetrics


class Database(WorkloadRunner):
    @abstractmethod
    def connect(self, max_retries: int = 3) -> None:
        """Establish a connection to the database, with retry logic."""
        pass

    @abstractmethod
    def set_knobs(self, knob_config):
        """Set the database configuration knobs based on the provided knob configuration."""
        pass

    @abstractmethod
    def fetch_internal_metrics(self) -> InternalMetrics:
        """Fetch internal metrics from the database.
        Returns:
            A dictionary containing internal metrics relevant to the database performance.
        """
        pass

    @abstractmethod
    def reset_internal_metrics(self):
        """Reset the internal metrics to prepare for the next workload run."""
        pass

    @abstractmethod
    def reset_knobs(self):
        """Reset the database configuration knobs to their default values."""
        pass

    @abstractmethod
    def run_workload(self, workload_task: BenchmarkTask) -> tuple[float, float]:
        pass
