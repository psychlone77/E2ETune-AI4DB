from abc import abstractmethod
from pathlib import Path
from typing import List, Optional
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
    def run_workload(self, workload_task: BenchmarkTask, runs_per_iteration: Optional[int] = 1) -> tuple[float, float]:
        pass

    @abstractmethod
    def extract_query_plans(self, workload_path: Path) -> List[str]:
        """Extract query plans for a list of SQL queries using EXPLAIN (FORMAT JSON) or equivalent.
        Loads queries from the file at workload_path and returns a list of formatted plan summaries
        in nested paranthetical notation (e.g., "Seq Scan on table1 (cost=0.00..431.00 rows=1000 width=4)").
        """
        pass
