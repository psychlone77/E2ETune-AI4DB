import subprocess
import re
import json

from classes.base_classes.Database import Database
from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Script_Config import DatabaseConfig
from classes.base_classes.Internal_Metrics import InternalMetrics
from typing import Any, Dict, List, Optional
import utils
import psycopg2
import time
from pathlib import Path


class PostgresSQLDatabase(Database):
    """
    A class representing a PostgresSQL database instance, responsible for managing the connection and executing workloads.
    """

    def __init__(self, db_config: DatabaseConfig, log_path: Optional[Path] = None):
        self.connection = None
        self.db_config: DatabaseConfig = db_config
        self.logger = utils.get_logger(log_path)
        self.connect(3)

    def connect(
        self,
        max_retries: int = 3,
    ) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                connection = psycopg2.connect(
                    dbname=self.db_config.name,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    host=self.db_config.host,
                    port=self.db_config.port,
                )
                self.logger.info("Connection established.")
                self.connection = connection
                return None
            except psycopg2.OperationalError as e:
                self.logger.error(f"Connection attempt {attempt} failed: {e}")

                # Try to start PostgreSQL service if connection failed
                if attempt < max_retries:
                    self.logger.info("Attempting to start PostgreSQL service...")
                    try:
                        subprocess.run(
                            ["sudo", "systemctl", "start", "postgresql"],
                            check=True,
                            timeout=30,
                            capture_output=True,
                            text=True,
                        )
                        self.logger.info(
                            "PostgreSQL service started. Waiting for it to be ready..."
                        )
                        time.sleep(3)
                    except subprocess.CalledProcessError as start_error:
                        self.logger.warning(
                            f"Failed to start PostgreSQL service: {start_error}"
                        )
                    except Exception as start_error:
                        self.logger.warning(
                            f"Error starting PostgreSQL service: {start_error}"
                        )

                if attempt == max_retries:
                    self.logger.error(
                        "Max retries reached. Could not connect to the database."
                    )
                    raise ConnectionError("Could not connect to the database.")

                time.sleep(2)
        return None

    def set_knobs(self, knob_config: KnobConfig):
        """Set the database configuration knobs based on the provided knob configuration."""
        old_autocommit = self.connection.autocommit
        try:
            # Ensure no active transaction before changing autocommit
            self.connection.commit()
            
            # ALTER SYSTEM cannot run inside a transaction block
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                for knob in knob_config.knobs:
                    cursor.execute(f"ALTER SYSTEM SET {knob.name} = %s;", (knob.value,))

            # Restart DB to apply changes
            if not self.restart_db():
                self.logger.warning(
                    "Database restart failed - configuration may be invalid. Continuing with previous/default settings."
                )
            else:
                self.logger.info(
                    "Database knobs have been set and DB restarted successfully."
                )
        finally:
            self.connection.autocommit = old_autocommit

    def fetch_internal_metrics(self) -> InternalMetrics:
        """Fetch internal metrics from the database."""
        metrics: InternalMetrics
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
                    (self.db_config.name,),
                )
                d = cursor.fetchone()
                metrics = {
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
                    "disk_read_count": 0,
                    "disk_write_count": 0,
                    "disk_read_bytes": 0,
                    "disk_write_bytes": 0,
                }

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
                metrics: InternalMetrics = {
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

    def reset_internal_metrics(self):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute("SELECT pg_stat_reset();")
                cursor.execute("SELECT pg_stat_reset_shared('bgwriter');")
                self.connection.commit()
                print("Internal metrics reset successfully")
            except Exception as e:
                print(f"Error resetting internal metrics: {e}")
            finally:
                cursor.close()
        self.logger.info("Internal metrics have been reset.")

    def reset_knobs(self):
        old_autocommit = self.connection.autocommit
        try:
            self.connection.commit()
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                cursor.execute("ALTER SYSTEM RESET ALL;")
                cursor.execute("SELECT pg_reload_conf();")
        except Exception as e:
            self.logger.error(f"Error resetting database knobs: {e}")
        finally:
            self.connection.autocommit = old_autocommit
        self.logger.info("Database knobs have been reset.")

    def restart_db(self, stop_timeout: int = 30, start_timeout: int = 30) -> bool:
        try:
            is_remote = self.db_config.host not in ["localhost", "127.0.0.1"]
            
            if is_remote and self.db_config.ssh_user:
                ssh_base = ["ssh"]
                if self.db_config.ssh_password:
                    ssh_base = ["sshpass", "-p", self.db_config.ssh_password, "ssh"]
                if self.db_config.ssh_key_path:
                    ssh_base.extend(["-i", self.db_config.ssh_key_path])
                ssh_base.extend([
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null",
                    f"{self.db_config.ssh_user}@{self.db_config.host}"
                ])
                restart_cmd = ssh_base + ["sudo", "systemctl", "restart", "postgresql"]
            else:
                restart_cmd = ["sudo", "systemctl", "restart", "postgresql"]

            print("Restarting PostgreSQL...")
            result = subprocess.run(
                restart_cmd,
                capture_output=True,
                text=True,
                timeout=start_timeout,
            )

            if result.returncode != 0:
                print(f"Restart failed: {result.stderr}")
                return False

            time.sleep(2)
            self.connect()
            return True
        except Exception as e:
            print(f"Failed to restart PostgreSQL: {e}")
            return False

    def run_workload(self, workload_task: BenchmarkTask, runs_per_iteration: Optional[int] = 1, default_run: Optional[bool] = False) -> tuple[float, float]:
        
        num_queries = 0
        with open(workload_task.workload_path, "r") as f:
            sql_script = f.read()
            num_queries = sql_script.count(";")
        if default_run:
            self.reset_knobs()
            self.restart_db()
        else:
            self.set_knobs(workload_task.knob_config)
        self.logger.info(f"Executing workload {workload_task.workload_path}...")
        with self.connection.cursor() as cursor:
            try:
                sum_latency = 0.0
                sum_throughput = 0.0
                for _ in range(runs_per_iteration):
                    start = time.perf_counter()
                    cursor.execute(sql_script)
                    self.connection.commit()
                    end = time.perf_counter()
                    sum_latency += (
                        (end - start) / num_queries if num_queries > 0 else 0.0
                    )
                    sum_throughput += num_queries / (end - start) if end > start else 0.0
                self.logger.info(
                    f"Workload {workload_task.workload_path} executed successfully."
                )
                average_latency = sum_latency / runs_per_iteration
                throughput_ps = sum_throughput / runs_per_iteration
                return average_latency, -throughput_ps
            except Exception as e:
                self.logger.error(f"Error executing workload: {e}")
                self.connection.rollback()
                return float("inf"), 0.0

    def extract_query_plans(self, workload_path: Path) -> List[str]:
        with open(workload_path, "r") as f:
            sql_script = f.read()

        workload_queries = [q.strip() for q in sql_script.split(";") if q.strip()]

        plans: List[str] = []

        with self.connection.cursor() as cursor:
            for i, query in enumerate(workload_queries):
                # Skip queries that are just comments or empty
                if not any(not line.strip().startswith('--') for line in query.split('\n') if line.strip()):
                    continue
                try:
                    self.logger.info(
                        f"Explaining query {i + 1}/{len(workload_queries)}"
                    )
                    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
                    row = cursor.fetchone()
                    self.connection.commit()
                    # EXPLAIN JSON result is a single JSON array; row[0] is that array
                    plan_json = row[0][0]  # [{"Plan": {...}}] -> take first element
                    formatted_plan = self._format_query_plan(plan_json)
                    plans.append(formatted_plan)
                except Exception as e:
                    self.connection.rollback()
                    self.logger.error(f"Error explaining query {i + 1}: {e}")
                    self.logger.debug(f"Query (truncated): {query[:100]}...")

        return plans

    @staticmethod
    def _format_query_plan(plan_json: Dict[str, Any]) -> str:
        """
        Parse and format a query plan into a compact summary string.
        Handles raw JSON strings (with '+' line continuations), lists, or dicts.

        Returns a compact representation: NodeType(cost=X.X)(child1; child2; ...)
        """
        # Clean and parse if the input is a raw JSON string
        if isinstance(plan_json, str):
            cleaned = re.sub(r"\+\s*\n", "", plan_json)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            plan_json = json.loads(cleaned)

        # Unwrap list wrapper: [{"Plan": {...}}] -> {"Plan": {...}}
        if isinstance(plan_json, list) and len(plan_json) > 0:
            plan_json = plan_json[0]

        def extract_node_summary(node: Dict[str, Any]) -> str:
            if not isinstance(node, dict):
                return ""
            node_type = node.get("Node Type", "Unknown")
            total_cost = node.get("Total Cost", 0)
            summary = f"{node_type}(cost={total_cost:.1f})"
            plans = node.get("Plans", [])
            if plans:
                child_summaries = [
                    s for s in (extract_node_summary(c) for c in plans) if s
                ]
                if child_summaries:
                    summary += "(" + "; ".join(child_summaries) + ")"
            return summary

        plan_node = plan_json.get("Plan", {}) if isinstance(plan_json, dict) else {}
        return extract_node_summary(plan_node)
