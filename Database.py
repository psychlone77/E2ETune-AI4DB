import psycopg2
import json
import os
import subprocess
import time
from typing import Dict, List, Any, Optional

from knob_config.parse_knob_config import get_knobs
import utils


class Database:
    """
    PostgreSQL utility class without SSH.
    - All parameters are sourced from `config['database_config']`.
    - Supports configurable PostgreSQL version, cluster name, and data_path.
    - Applies knob changes via ALTER SYSTEM and restarts via pg_ctlcluster.
    """

    def __init__(self, config: Dict[str, Any], knob_config_path: str):
        """
        Initialize Database from config dict.

        Expected config['database_config'] keys:
          host: str
          port: int or str
          database: str
          user: str
          password: str
          data_path: str (e.g., /var/lib/postgresql/12/main)
          pg_version: str or int (e.g., "12", "14")
          cluster_name: str (default "main")
        """
        db_cfg = config.get('database_config', {})
        self.host: str = db_cfg.get('host', 'localhost')
        self.port: int = int(db_cfg.get('port', 5432))
        self.database: str = db_cfg.get('database', 'postgres')
        self.user: str = db_cfg.get('user', 'postgres')
        self.password: str = db_cfg.get('password', '')
        self.data_path: str = db_cfg.get('data_path', '')
        self.pg_version: str = str(db_cfg.get('pg_version', '12'))
        self.cluster_name: str = db_cfg.get('cluster_name', 'main')

        # Knob definitions (types, ranges, etc.) from your knob config
        self.knobs: Dict[str, Dict[str, Any]] = get_knobs(knob_config_path)
        self.logger = utils.get_logger(config['tuning_config']['log_path'])

    def get_conn(self, max_retries: int = 3) -> psycopg2.extensions.connection:
        """
        Establish a PostgreSQL connection with simple retry logic.
        If all retries fail, attempts to remove postgresql.auto.conf and tries once more.
        """
        self.logger.debug(f"Connecting to PostgreSQL {self.host}:{self.port}, db={self.database}")
        last_err: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    connect_timeout=10,
                )
                if attempt > 0:
                    self.logger.info(f"Connection successful on attempt {attempt + 1}")
                return conn
            except Exception as e:
                last_err = e
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in 2 seconds... ({attempt + 2}/{max_retries})")
                    time.sleep(2)

        self.logger.warning(f"All {max_retries} attempts failed. Removing auto.conf and retrying...")
        self.remove_auto_conf()

        time.sleep(2)
        try:
            self.logger.info("Final connection attempt after removing auto.conf")
            conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                connect_timeout=10,
            )
            self.logger.info("Connection successful after removing auto.conf")
            return conn
        except Exception as e:
            self.logger.error(f"Connection failed after {max_retries + 1} attempts and auto.conf removal")
            raise Exception(
                f"Could not establish database connection after {max_retries + 1} attempts "
                f"and auto.conf removal: {e}"
            ) from last_err

    def fetch_knob(self) -> Dict[str, float]:
        """
        Fetch current values for knobs listed in knob config using pg_settings.
        Returns a dict: { knob_name: value_as_float }
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        knobs: Dict[str, float] = {}
        try:
            for knob in self.knobs:
                sql = "SELECT name, setting FROM pg_settings WHERE name = %s"
                cursor.execute(sql, (knob,))
                for name, setting in cursor.fetchall():
                    try:
                        knobs[name] = float(setting)
                    except ValueError:
                        # Non-numeric settings represented as float 0.0 for downstream compatibility
                        knobs[name] = 0.0
        finally:
            cursor.close()
            conn.close()
        return knobs

    def extract_query_plans(self, workload_queries: List[str]) -> List[Dict[str, Any]]:
        """
        Extract JSON plans for a list of SQL queries using EXPLAIN (FORMAT JSON).
        Returns a list of objects suitable for downstream processing.
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        plans: List[Dict[str, Any]] = []

        try:
            for i, query in enumerate(workload_queries):
                try:
                    self.logger.info(f"Explaining query {i + 1}/{len(workload_queries)}")
                    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
                    row = cursor.fetchone()
                    # EXPLAIN JSON result is a single JSON array; row[0] is that array
                    plan_json = row[0][0]  # [{"Plan": {...}}] -> take first element
                    plans.append({
                        "Plan": plan_json,
                        "query": query.strip(),
                        "query_id": i,
                    })
                except Exception as e:
                    self.logger.error(f"Error explaining query {i + 1}: {e}")
                    self.logger.debug(f"Query (truncated): {query[:100]}...")
        finally:
            cursor.close()
            conn.close()

        print(f"Extracted {len(plans)} query plans")
        return plans

    def save_workload_plans(self, workload_queries: List[str], workload_name: str) -> List[Dict[str, Any]]:
        """
        Extract and save workload query plans as JSON in ./query_plans/{workload_name}.json
        """
        plans = self.extract_query_plans(workload_queries)
        if plans:
            os.makedirs("query_plans", exist_ok=True)
            output_file = os.path.join("query_plans", f"{workload_name}.json")
            with open(output_file, 'w') as f:
                json.dump(plans, f, indent=2)
            print(f"Saved {len(plans)} plans to {output_file}")
        else:
            print("No plans to save")
        return plans

    def reset_inner_metrics(self) -> None:
        """
        Reset PostgreSQL internal statistics (pg_stat_*).
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT pg_stat_reset();")
            cursor.execute("SELECT pg_stat_reset_shared('bgwriter');")
            conn.commit()
            print("Internal metrics reset successfully")
        except Exception as e:
            print(f"Error resetting internal metrics: {e}")
        finally:
            cursor.close()
            conn.close()

    def fetch_inner_metrics(self) -> Dict[str, float]:
        """
        Fetch internal metrics from PostgreSQL as a flat dict of floats.
        Includes database stats and IO estimates derived from block counters.
        """
        metrics: Dict[str, float] = {}
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
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
            """, (self.database,))
            d = cursor.fetchone()
            metrics.update({
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
            })

            cursor.execute("""
                SELECT COALESCE(SUM(
                    COALESCE(heap_blks_read, 0) +
                    COALESCE(idx_blks_read, 0) +
                    COALESCE(toast_blks_read, 0) +
                    COALESCE(tidx_blks_read, 0)
                ), 0)
                FROM pg_statio_all_tables;
            """)
            disk_read_count = float(cursor.fetchone()[0])

            cursor.execute("""
                SELECT buffers_checkpoint + buffers_clean + buffers_backend
                FROM pg_stat_bgwriter;
            """)
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
                "xact_commit": 0.0, "xact_rollback": 0.0, "blks_read": 0.0, "blks_hit": 0.0,
                "tup_returned": 0.0, "tup_fetched": 0.0, "tup_inserted": 0.0, "conflicts": 0.0,
                "tup_updated": 0.0, "tup_deleted": 0.0,
                "disk_read_count": 0.0, "disk_write_count": 0.0,
                "disk_read_bytes": 0.0, "disk_write_bytes": 0.0,
            }
        finally:
            cursor.close()
            conn.close()

        return metrics

    def change_knob(self, knobs: Dict[str, Any]) -> bool:
        """
        Apply knob changes using ALTER SYSTEM, then restart via pg_ctlcluster.

        knobs: dict { knob_name: value }
        Uses types from self.knobs to cast values (integer/real).
        """
        print("Applying knob changes...")
        success = True
        conn = self.get_conn()
        cursor = conn.cursor()
        conn.autocommit = True

        try:
            for knob, raw_val in knobs.items():
                val = raw_val
                kdef = self.knobs.get(knob, {})
                if kdef.get('type') == 'integer':
                    val = int(val)
                elif kdef.get('type') == 'real':
                    val = float(val)

                try:
                    cursor.execute(f"ALTER SYSTEM SET {knob} = %s;", (val,))
                    print(f"Set {knob} = {val}")
                except Exception as e:
                    print(f"Error setting {knob} = {val}: {e}")
                    success = False

            if success:
                print("Knobs applied. Restarting PostgreSQL cluster...")
                restart_ok = self.restart_db()
                if restart_ok:
                    print("Database restarted successfully.")
                else:
                    print("Database restart failed.")
                    success = False
            else:
                print("Some knobs failed to apply; skipping restart.")
        except Exception as e:
            print(f"Error applying knobs: {e}")
            success = False
        finally:
            cursor.close()
            conn.close()

        return success

    def restart_db(self, stop_timeout: int = 30, start_timeout: int = 30) -> bool:
        """
        Restart PostgreSQL using pg_ctlcluster with configured version and cluster name.
        Returns True on success.
        """
        try:
            print(f"Stopping PostgreSQL {self.pg_version}/{self.cluster_name}...")
            subprocess.run(
                ['sudo', 'pg_ctlcluster', str(self.pg_version), self.cluster_name, 'stop'],
                check=True, timeout=stop_timeout
            )
            time.sleep(2)

            print(f"Starting PostgreSQL {self.pg_version}/{self.cluster_name}...")
            result = subprocess.run(
                ['sudo', 'pg_ctlcluster', str(self.pg_version), self.cluster_name, 'start'],
                capture_output=True, text=True, timeout=start_timeout
            )

            if result.returncode != 0:
                print("Start failed. Removing auto.conf and retrying...")
                self.remove_auto_conf()
                time.sleep(1)
                subprocess.run(
                    ['sudo', 'pg_ctlcluster', str(self.pg_version), self.cluster_name, 'start'],
                    check=True, timeout=start_timeout
                )

            return True
        except Exception as e:
            print(f"Failed to restart PostgreSQL: {e}")
            return False

    def remove_auto_conf(self) -> None:
        """
        Remove postgresql.auto.conf from the configured data_path (if present).
        Useful when ALTER SYSTEM introduced a bad setting that prevents startup.
        """
        if not self.data_path:
            print("data_path not set; cannot remove postgresql.auto.conf")
            return

        auto_conf_path = os.path.join(self.data_path, "postgresql.auto.conf")
        try:
            subprocess.run(['sudo', 'rm', '-f', auto_conf_path], check=True)
            print(f"Removed {auto_conf_path} (if it existed).")
        except subprocess.CalledProcessError as e:
            print(f"Error removing {auto_conf_path}: {e}")

    def run_workload_with_defaults(self, workload_file: str) -> None:
        """
        Execute a workload file using psql.
        """
        try:
            print(f"Running workload from {workload_file}...")
            conn = self.get_conn()
            cursor = conn.cursor()
            with open(workload_file, 'r') as f:
                sql_commands = f.read()
            cursor.execute(sql_commands)
            conn.commit()
            cursor.close()
            conn.close()
            print("Workload execution completed.")
        except subprocess.CalledProcessError as e:
            print(f"Error running workload: {e}")

    def run_workload_with_config(self, workload_file: str, knobs: Dict[str, Any]) -> None:
        """
        Apply knobs, restart DB, and run workload file using psql.
        """
        if self.change_knob(knobs):
            self.run_workload_with_defaults(workload_file)
        else:
            print("Knob change failed; skipping workload execution.")


    def reset_db_knobs(self) -> None:
        """
        Reset the database knobs to their default values.

        This is a convenience module-level function moved here from resetDB.py
        so other modules can reuse it directly. It mirrors the previous
        standalone script behavior by accepting the same `args` dict.
        """
        conn = None

        try:
            conn = self.get_conn()
            # ALTER SYSTEM cannot run inside a transaction block; enable autocommit
            conn.autocommit = True
            cur = conn.cursor()

            # Simplest and safest: clear all overrides from postgresql.auto.conf
            # This resets parameters back to their default/boot values.
            cur.execute("ALTER SYSTEM RESET ALL;")

            # Reload the configuration to apply changes immediately
            cur.execute("SELECT pg_reload_conf();")
            cur.close()
            print("Database knobs have been reset to default values.")

        except Exception as e:
            print(f"An error occurred while resetting database knobs: {e}")
        finally:
            if conn is not None:
                conn.close()