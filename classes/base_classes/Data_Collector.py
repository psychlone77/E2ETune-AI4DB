from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

import utils
from classes.base_classes.Database import Database
from classes.base_classes.Internal_Metrics import InternalMetrics
from classes.base_classes.Knob_Settings import KnobSettingsSet


class DefaultDataCollector(ABC):
    """Abstract base class for workload data collectors."""

    def __init__(
        self,
        workload_path: Path,
        db: Database,
        benchmark: str,
        output_dir: Path,
        knob_settings_set: KnobSettingsSet,
        log_path: Optional[Path] = None,
    ):
        self.workload_path = workload_path
        self.db = db
        self.benchmark = benchmark
        self.output_dir = output_dir
        self.knob_settings_set = knob_settings_set
        self.log_path = utils.get_logger(log_path, f"{self.__class__.__name__}.log")

    @abstractmethod
    def collect(self) -> None:
        """Run the collector flow and persist collected outputs."""
        pass

    @abstractmethod
    def _collect_internal_metrics(self) -> InternalMetrics:
        """Collect internal database metrics."""
        pass

    @abstractmethod
    def _collect_query_plans(self) -> list[str]:
        """Collect query plans for the workload."""
        pass

    @abstractmethod
    def _collect_workload_features(self) -> Dict[str, Any]:
        """Collect macro-level workload features."""
        pass
