from classes.Database import Database
from classes.Knob_Config import KnobConfig
from classes.Workload_Runner import WorkloadRunner, BenchmarkTask
from classes.Script_Config import DatabaseConfig
from typing import Optional
import utils
import psycopg2


class PostgreSQLDatabase(WorkloadRunner, Database):
    """
    A class representing a PostgreSQL database instance, responsible for managing the connection and executing workloads.
    """

    db_config: DatabaseConfig
    connection: psycopg2.extensions.connection

    def __init__(self, db_config: DatabaseConfig, log_path: Optional[str] = None):
        super().__init__()
        self.db_config = db_config
        self.logger = utils.get_logger(log_path) if log_path is not None else None
        self.connect(3)

    def connect(
        self,
        max_retries: int = 3,
    ) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                self.connection = psycopg2.connect(
                    dbname=self.db_config.name,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    host=self.db_config.host,
                    port=self.db_config.port,
                )
                self.logger.info("Connection established.")
                return
            except psycopg2.OperationalError as e:
                self.logger.error(f"Connection attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    self.logger.error(
                        "Max retries reached. Could not connect to the database."
                    )
                    raise ConnectionError("Could not connect to the database.")

    def set_knobs(self, knob_config: KnobConfig):
        """Set the database configuration knobs based on the provided knob configuration."""
        with self.connection.cursor() as cursor:
            for knob in knob_config.knobs:
                cursor.execute(f"ALTER SYSTEM SET {knob.name} TO '{knob.value}';")
            self.connection.commit()
        self.logger.info("Database knobs have been set.")

    def fetch_internal_metrics(self) -> dict:
        """Fetch internal metrics from the database."""
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    SELECT 
                        COALESCE(SUM(xact_commit), 0),
                        COALESCE(SUM(xact_rollback), 0),
                        COALESCE(SUM(blks_read), 0),
                        COALESCE(SUM(blks_hit), 0),
                        COALESCE(SUM(tup_returned), 0),
                        COALESCE(SUM(tup_fetched), 0),
                        COALESCE(SUM(tup_inserted), 0),
                        COALESCE(SUM(conflicts), 0),
                        COALESCE(SUM(tup_updated), 0),
                        COALESCE(SUM(tup_deleted), 0)
                    FROM pg_stat_database 
                    WHERE datname = %s;
                """,
                    (self.database,),
                )
                d = cursor.fetchone()
                metrics.update(
                    {
                        "xact_commit": float(d[0]),
                        "xact_rollback": float(d[1]),
                        "blks_read": float(d[2]),
                        "blks_hit": float(d[3]),
                        "tup_returned": float(d[4]),
                        "tup_fetched": float(d[5]),
                        "tup_inserted": float(d[6]),
                        "conflicts": float(d[7]),
                        "tup_updated": float(d[8]),
                        "tup_deleted": float(d[9]),
                    }
                )

                cursor.execute(
                    """
                    SELECT COALESCE(SUM(
                        COALESCE(heap_blks_read, 0) +
                        COALESCE(idx_blks_read, 0) +
                        COALESCE(toast_blks_read, 0) +
                        COALESCE(tidx_blks_read, 0)
                    ), 0)
                    FROM pg_statio_all_tables;
                """
                )
                disk_read_count = float(cursor.fetchone()[0])

                cursor.execute(
                    """
                    SELECT buffers_checkpoint + buffers_clean + buffers_backend
                    FROM pg_stat_bgwriter;
                """
                )
                disk_write_count = float(cursor.fetchone()[0])

                # 8KB per block
                metrics["disk_read_count"] = disk_read_count
                metrics["disk_write_count"] = disk_write_count
                metrics["disk_read_bytes"] = disk_read_count * 8192.0
                metrics["disk_write_bytes"] = disk_write_count * 8192.0

                print(f"Fetched {len(metrics)} internal metrics")
            except Exception as e:
                print(f"Error fetching internal metrics: {e}")
                metrics = {
                    "xact_commit": 0.0,
                    "xact_rollback": 0.0,
                    "blks_read": 0.0,
                    "blks_hit": 0.0,
                    "tup_returned": 0.0,
                    "tup_fetched": 0.0,
                    "tup_inserted": 0.0,
                    "conflicts": 0.0,
                    "tup_updated": 0.0,
                    "tup_deleted": 0.0,
                    "disk_read_count": 0.0,
                    "disk_write_count": 0.0,
                    "disk_read_bytes": 0.0,
                    "disk_write_bytes": 0.0,
                }

        return metrics

    def run_workload(self, workload_task: BenchmarkTask) -> float:
        print(
            f"Running workload for database {self.db_config.name} with configuration: {self.db_config}"
        )
        print(
            f"Workload path: {workload_task.workload_path} with knobs: {workload_task.knob_config}"
        )
        return 0.0  # Placeholder for actual performance metric
