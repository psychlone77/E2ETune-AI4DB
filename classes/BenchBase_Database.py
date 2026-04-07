import os
import re
import json
import shutil
import subprocess
import time

import psycopg2
from pathlib import Path
from typing import Dict, List, Optional

from classes.base_classes.Database import Database
from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Script_Config import DatabaseConfig
from classes.base_classes.Internal_Metrics import InternalMetrics
from classes.base_classes.Script_Config import BenchmarkConfig
import utils


class BenchBaseDatabase(Database):
    """
    A Database implementation that uses BenchBase to run OLTP benchmarks.
    Maintains a direct psycopg2 connection for knob management and metrics collection,
    and delegates workload execution to BenchBase.
    """

    def __init__(
        self,
        db_config: DatabaseConfig,
        benchmark_config: BenchmarkConfig,
        log_path: Optional[Path] = None,
    ):
        self.connection = None
        self.db_config = db_config
        self.benchmark_config = benchmark_config
        self.logger = utils.get_logger(log_path)
        self.connect(3)

    # --- Database interface ---

    def connect(self, max_retries: int = 3) -> None:
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
                        continue
                    except subprocess.CalledProcessError as start_error:
                        self.logger.warning(
                            f"Failed to start PostgreSQL service: {start_error}"
                        )
                    except Exception as start_error:
                        self.logger.warning(
                            f"Error starting PostgreSQL service: {start_error}"
                        )
                        
                if attempt == max_retries:
                    raise ConnectionError("Could not connect to the database.")
                time.sleep(2)

    def set_knobs(self, knob_config: KnobConfig):
        old_autocommit = self.connection.autocommit
        try:
            self.connection.commit()
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                for knob in knob_config.knobs:
                    cursor.execute(f"ALTER SYSTEM SET {knob.name} = %s;", (knob.value,))
            if not self._restart_db():
                self.logger.warning(
                    "Database restart failed - continuing with previous settings."
                )
            else:
                self.logger.info("Database knobs set and DB restarted successfully.")
        finally:
            self.connection.autocommit = old_autocommit
            cursor.close()

    def fetch_internal_metrics(self) -> InternalMetrics:
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(SUM(xact_commit), 0), COALESCE(SUM(xact_rollback), 0),
                        COALESCE(SUM(blks_read), 0), COALESCE(SUM(blks_hit), 0),
                        COALESCE(SUM(tup_returned), 0), COALESCE(SUM(tup_fetched), 0),
                        COALESCE(SUM(tup_inserted), 0), COALESCE(SUM(conflicts), 0),
                        COALESCE(SUM(tup_updated), 0), COALESCE(SUM(tup_deleted), 0)
                    FROM pg_stat_database WHERE datname = %s;
                    """,
                    (self.db_config.name,),
                )
                d = cursor.fetchone()
                metrics: InternalMetrics = {
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
                    "disk_read_count": 0.0,
                    "disk_write_count": 0.0,
                    "disk_read_bytes": 0.0,
                    "disk_write_bytes": 0.0,
                }
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(
                        COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) +
                        COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)
                    ), 0) FROM pg_statio_all_tables;
                    """
                )
                disk_read_count = float(cursor.fetchone()[0])
                cursor.execute(
                    "SELECT buffers_checkpoint + buffers_clean + buffers_backend FROM pg_stat_bgwriter;"
                )
                disk_write_count = float(cursor.fetchone()[0])
                metrics["disk_read_count"] = disk_read_count
                metrics["disk_write_count"] = disk_write_count
                metrics["disk_read_bytes"] = disk_read_count * 8192.0
                metrics["disk_write_bytes"] = disk_write_count * 8192.0
                return metrics
            except Exception as e:
                self.logger.error(f"Error fetching internal metrics: {e}")
                return {k: 0.0 for k in InternalMetrics.__annotations__}

    def reset_internal_metrics(self):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute("SELECT pg_stat_reset();")
                cursor.execute("SELECT pg_stat_reset_shared('bgwriter');")
                self.connection.commit()
                self.logger.info("Internal metrics reset successfully.")
            except Exception as e:
                self.logger.error(f"Error resetting internal metrics: {e}")
            finally:
                cursor.close()

    def reset_knobs(self):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute("ALTER SYSTEM RESET ALL;")
                cursor.execute("SELECT pg_reload_conf();")
                self.connection.commit()
                self.logger.info("Database knobs have been reset.")
            except Exception as e:
                self.logger.error(f"Error resetting database knobs: {e}")
            finally:
                cursor.close()

    def run_workload(self, workload_task: BenchmarkTask, runs_per_iteration: int = 1) -> tuple[float, float]:
        """Run the BenchBase benchmark against the given workload config file.
        Returns (0.0, -throughput) to match the minimization convention used elsewhere.
        """
        self.connection.rollback()
        self.set_knobs(workload_task.knob_config)
        throughput = self._run_benchbase(workload_task.workload_path)
        return 0.0, -throughput

    def extract_query_plans(self, workload_path: Path) -> List[str]:
        """Extract query plans using auto_explain by tracking log file bytes before and after execution."""
        old_autocommit = self.connection.autocommit
        try:
            self.connection.commit()
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                self.logger.info("Enabling auto_explain for query plan extraction.")
                cursor.execute("ALTER SYSTEM SET shared_preload_libraries = 'auto_explain';")

            self._restart_db()

            try:
                 self.connection.commit()
            except Exception:
                 pass
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                self.logger.info("Configuring auto_explain settings.")
                cursor.execute("ALTER SYSTEM SET auto_explain.log_min_duration = 0;")
                cursor.execute("ALTER SYSTEM SET auto_explain.log_nested_statements = on;")
                # Reload configuration to apply the specific module settings
                cursor.execute("SELECT pg_reload_conf();")

            # Get postgres log file path
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute("SELECT pg_current_logfile();")
                    log_file = cursor.fetchone()[0]
                except Exception:
                    # Fallback if logging collector is not on
                    self.logger.warning(
                        "Could not determine current log file. Ensure logging_collector=on."
                    )
                    return []
                if not os.path.isabs(log_file):
                    cursor.execute("SHOW data_directory;")
                    data_dir = cursor.fetchone()[0]
                    log_file = os.path.join(data_dir, log_file)

            # Note the bytes length before running
            initial_size = os.path.getsize(log_file) if os.path.exists(log_file) else 0

            # Run workload with default knob settings
            self.logger.info(
                "Running workload to extract query plans via auto_explain."
            )
            self._run_benchbase(workload_path)

            # Read new log bytes and extract plans
            plans = []
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    f.seek(initial_size)
                    new_logs = f.read()

                in_plan = False
                current_plan = []
                
                for line in new_logs.splitlines():
                    # 1. Detect start of an auto_explain block
                    if "duration:" in line and "plan:" in line.lower():
                        in_plan = True
                        current_plan = []
                        continue
                    
                    if in_plan:
                        stripped = line.strip()
                        
                        # 2. Check if the block has ended
                        # (Plan lines must start with whitespace; if not, the plan is over)
                        if not (line.startswith("\t") or line.startswith("  ")):
                            if current_plan:
                                plans.append("(" + " ".join(current_plan) + ")")
                            in_plan = False
                            current_plan = []
                            continue

                        # 3. Filter out unnecessary metadata rows
                        if any(stripped.startswith(x) for x in ["Query Text:", "Query Parameters:", "Output:"]):
                            continue
                        
                        # 4. Remove cost/rows/width info to get clean operators
                        # This turns "Index Scan using idx (cost=0.28..8.29 rows=1...)" 
                        # into "Index Scan using idx"
                        clean_line = re.sub(r"\(cost=.*?width=.*?\)", "", stripped).strip()
                        
                        if clean_line:
                            current_plan.append(clean_line)

            # Disable auto_explain
            with self.connection.cursor() as cursor:
                cursor.execute("ALTER SYSTEM RESET shared_preload_libraries;")
                cursor.execute("ALTER SYSTEM RESET auto_explain.log_min_duration;")
                cursor.execute("ALTER SYSTEM RESET auto_explain.log_nested_statements;")
            self._restart_db()

            return plans

        except Exception as e:
            self.logger.error(f"Failed to extract query plans: {e}")
            return []
        finally:
            self.connection.autocommit = old_autocommit

    def load_database(self, workload_path: str):
        """Load benchmark data into the database via BenchBase (create + load, no execute)."""
        benchmark_name = self.benchmark_config.get("benchmark", "tpcc")
        benchbase_jar_dir = self.benchmark_config.get(
            "benchbase_jar_dir", "/home/benchbase/target/benchbase-postgres"
        )
        self._copy_config_to_benchbase(workload_path, benchmark_name)
        command = (
            f"cd {benchbase_jar_dir} && "
            f"java -jar benchbase.jar "
            f"-b {benchmark_name.lower()} "
            f"-c config/postgres/{os.path.basename(workload_path)} "
            f"--create=true --load=true --execute=false"
        )
        state = os.system(command)
        if state != 0:
            self.logger.error(f"Data loading failed with exit code: {state}")
            return 0.0
        self.logger.info("Data loaded successfully.")

    # --- Private helpers ---

    def _run_benchbase(self, workload_path) -> float:
        benchmark_name = self.benchmark_config.name
        workload_name = Path(workload_path).stem
        results_dir = os.path.abspath(
            os.path.join(
                "stress_test_results", f"{benchmark_name}_results", workload_name
            )
        )
        os.makedirs(results_dir, exist_ok=True)

        _, benchbase_jar_dir = self._copy_config_to_benchbase(
            workload_path, benchmark_name
        )
        timestamp = int(time.time())
        log_file = os.path.join(results_dir, f"{benchmark_name}_{timestamp}.log")

        command = (
            f"cd {benchbase_jar_dir} && "
            f"java -jar benchbase.jar "
            f"-b {benchmark_name.lower()} "
            f"-c config/postgres/{os.path.basename(workload_path)} "
            f"--execute=true "
            f"--directory={results_dir} "
            f"> {log_file} 2>&1"
        )
        self.logger.info(f"Running BenchBase benchmark: {benchmark_name}")
        
        try:
            # Run with a reasonable timeout (e.g., 90 seconds for a 30s benchmark)
            result = subprocess.run(
                command, 
                shell=True, 
                timeout=90, 
                capture_output=False # output is already redirected to log_file in the command string
            )
            
            if result.returncode != 0:
                self.logger.warning(f"BenchBase returned non-zero exit code: {result.returncode}")
                return 0.0
            else:
                self.logger.info("BenchBase execution completed successfully.")
                summary_path = self._find_and_archive_summary(results_dir)
                if summary_path:
                    return self._parse_throughput(summary_path)
                else:
                    self.logger.error("Summary file not found after BenchBase execution.")
                    return 0.0
                
        except subprocess.TimeoutExpired:
            self.logger.error("BenchBase execution timed out! Database likely became unresponsive.")
            # Penalize this configuration so HEBO avoids it
            return 0.0

    def _copy_config_to_benchbase(
        self, workload_path, benchmark_name: str
    ) -> tuple[str, str]:
        self._update_config_file(str(workload_path), benchmark_name)
        benchbase_jar = self.benchmark_config.benchbase_jar
        benchbase_root = os.path.dirname(os.path.dirname(benchbase_jar))
        benchbase_jar_dir = os.path.join(benchbase_root, "benchbase-postgres")
        config_dir = os.path.join(benchbase_jar_dir, "config", "postgres")
        os.makedirs(config_dir, exist_ok=True)
        dest = os.path.join(config_dir, os.path.basename(workload_path))
        shutil.copy2(str(workload_path), dest)
        return dest, benchbase_jar_dir

    def _update_config_file(self, config_file: str, benchmark_name: str):
        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()

        benchmark = benchmark_name.lower()
        url = (
            f"jdbc:postgresql://{self.db_config.host}:{self.db_config.port}/{self.db_config.name}"
            f"?sslmode=disable&amp;ApplicationName={benchmark_name}&amp;reWriteBatchedInserts=true"
        )
        content = re.sub(r"<url>.*?</url>", f"<url>{url}</url>", content)
        content = re.sub(
            r"<username>.*?</username>",
            f"<username>{self.db_config.user}</username>",
            content,
        )
        content = re.sub(
            r"<password>.*?</password>",
            f"<password>{self.db_config.password}</password>",
            content,
        )
        content = re.sub(r"<time>.*?</time>", "<time>30</time>", content)
        content = re.sub(
            r"<terminals>.*?</terminals>", "<terminals>4</terminals>", content
        )

        benchmark_overrides: Dict[str, Dict[str, str]] = {
            "ycsb": {"scalefactor": "3600", "rate": "70000"},
            "wikipedia": {"scalefactor": "1", "rate": "unlimited"},
            "twitter": {"scalefactor": "80", "rate": "unlimited"},
            "smallbank": {"scalefactor": "45", "rate": "unlimited"},
            "tpcc": {"scalefactor": "1", "rate": "unlimited"},
        }
        for tag, value in benchmark_overrides.get(benchmark, {}).items():
            content = re.sub(rf"<{tag}>.*?</{tag}>", f"<{tag}>{value}</{tag}>", content)

        with open(config_file, "w", encoding="utf-8") as f:
            f.write(content)

    def _find_and_archive_summary(self, results_dir: str) -> Optional[str]:
        summary_archive_dir = os.path.join(results_dir, "summary")
        os.makedirs(summary_archive_dir, exist_ok=True)
        time.sleep(2)

        summary_path = None
        for file in os.listdir(results_dir):
            if file == "summary":
                continue
            file_path = os.path.join(results_dir, file)
            if file.endswith(".summary.json"):
                shutil.copy2(file_path, os.path.join(summary_archive_dir, file))
                final_path = os.path.join(results_dir, "summary.json")
                shutil.copy2(file_path, final_path)
                summary_path = final_path

        for file in os.listdir(results_dir):
            if file in ("summary", "summary.json"):
                continue
            file_path = os.path.join(results_dir, file)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    self.logger.warning(f"Could not remove {file_path}: {e}")

        return summary_path

    def _parse_throughput(self, summary_path: str) -> float:
        try:
            with open(summary_path, "r") as f:
                data = json.load(f)
            throughput = float(data["Throughput (requests/second)"])
            self.logger.info(f"BenchBase throughput: {throughput}")
            return throughput
        except Exception as e:
            self.logger.error(f"Error parsing summary.json: {e}")
            return 0.0

    def _restart_db(self, stop_timeout: int = 30, start_timeout: int = 30) -> bool:
        try:
            base_cmd = [
                "sudo",
                "pg_ctlcluster",
                str(self.db_config.pg_version),
                self.db_config.cluster_name,
            ]
            auto_conf_path = f"{self.db_config.data_path}/postgresql.auto.conf"

            subprocess.run(base_cmd + ["stop"], check=True, timeout=stop_timeout)
            time.sleep(2)
            result = subprocess.run(
                base_cmd + ["start"],
                capture_output=True,
                text=True,
                timeout=start_timeout,
            )

            if result.returncode != 0:
                self.logger.warning(
                    "DB start failed, removing auto.conf and retrying..."
                )
                subprocess.run(
                    ["sudo", "rm", "-f", auto_conf_path], check=True, timeout=5
                )
                time.sleep(1)
                result = subprocess.run(
                    base_cmd + ["start"],
                    capture_output=True,
                    text=True,
                    timeout=start_timeout,
                )
                if result.returncode != 0:
                    self.logger.error(f"Retry start also failed: {result.stderr}")
                    return False
            self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Failed to restart PostgreSQL: {e}")
            return False
