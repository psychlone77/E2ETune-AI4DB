from classes.base_classes.Database import Database
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Internal_Metrics import InternalMetrics
from classes.base_classes.Knob_Settings import KnobSettingsSet

from typing import Any, Dict, List, Optional
from pathlib import Path
import utils
import json


class DefaultDataCollector:
    def __init__(
        self,
        workload_path: Path,
        db: Database,
        output_dir: Path,
        knob_settings_set: KnobSettingsSet,
        log_path: Optional[Path] = None,
    ):
        self.workload_path = workload_path
        self.db = db
        self.output_dir = output_dir
        self.knob_settings_set = knob_settings_set
        self.log_path = utils.get_logger(log_path, "DefaultDataCollector.log")

    def _collect_internal_metrics(self) -> InternalMetrics:
        task = BenchmarkTask(
            workload_path=self.workload_path,
            knob_config=self.knob_settings_set.get_default_knob_settings(),
        )
        try:
            self.db.reset_internal_metrics()
            self.db.run_workload(task)
            return self.db.fetch_internal_metrics()
        except Exception as e:
            self.log_path.error(
                f"Error collecting internal metrics for task {task}: {e}"
            )
            return InternalMetrics(
                xact_commit=0.0,
                xact_rollback=0.0,
                blks_read=0.0,
                blks_hit=0.0,
                tup_returned=0.0,
                tup_fetched=0.0,
                tup_inserted=0.0,
                conflicts=0.0,
                tup_updated=0.0,
                tup_deleted=0.0,
                disk_read_count=0.0,
                disk_write_count=0.0,
                disk_read_bytes=0.0,
                disk_write_bytes=0.0,
            )

    def _collect_query_plans(self) -> List[str]:
        try:
            return self.db.extract_query_plans(self.workload_path)
        except Exception as e:
            self.log_path.error(f"Error collecting query plans: {e}")
            return []

    def _collect_workload_features(self) -> Dict[str, Any]:
        # try:
        #     return self.db.extract_workload_features()
        # except Exception as e:
        #     self.log_path.error(f"Error collecting workload features: {e}")
        return {}

    def collect(self) -> None:
        self.log_path.info(
            f"Starting data collection for workload: {self.workload_path}"
        )
        internal_metrics = self._collect_internal_metrics()
        query_plans = self._collect_query_plans()
        workload_features = self._collect_workload_features()

        # Save collected data
        output_data = {
            "internal_metrics": internal_metrics,
            "query_plans": query_plans,
            "workload_features": workload_features,
        }
        output_file = self.output_dir / "collected_data.json"
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)
        self.log_path.info(f"Data collection completed and saved to: {output_file}")
