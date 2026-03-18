import subprocess
import mysql.connector
from mysql.connector import errorcode
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
import sqlglot
from classes.base_classes.Database import Database
from classes.base_classes.Knob_Config import KnobConfig
from classes.base_classes.Workload_Runner import BenchmarkTask
from classes.base_classes.Script_Config import DatabaseConfig
from classes.base_classes.Internal_Metrics import InternalMetrics
import utils
import json

class MySQLDatabase(Database):
    """
    A class representing a MySQL database instance, responsible for managing the connection and executing workloads.
    """

    def __init__(self, db_config: DatabaseConfig, log_path: Optional[Path] = None):
        self.db_config = db_config
        self.logger = utils.get_logger(log_path)
        self.connection = None
        self.connect(3)

    def connect(self, max_retries: int = 3) -> None:
        # 1. Properly dispose of the old connection first
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Closed old MySQL connection.")
            except:
                pass 
            self.connection = None

        for attempt in range(1, max_retries + 1):
            try:
                # 2. Add 'use_pure=True' to avoid some C-extension reference issues
                connection = mysql.connector.connect(
                    database=self.db_config.name,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    host=self.db_config.host,
                    port=self.db_config.port,
                    autocommit=False,
                    buffered=True,
                    use_pure=True  # Pure Python is often more stable for tuning scripts
                )
                self.connection = connection
                return
            except mysql.connector.Error as err:
                self.logger.error(f"Connection attempt {attempt} failed: {err}")
                
                # Try to start MySQL service if connection failed
                if attempt < max_retries:
                    self.logger.info("Attempting to start MySQL service...")
                    try:
                        import subprocess
                        subprocess.run(["sudo", "service", "mysql", "start"], check=True)
                    except Exception as e:
                        self.logger.error(f"Failed to start MySQL service: {e}")

                if attempt == max_retries:
                    raise ConnectionError(f"Could not connect to MySQL: {err}")
                time.sleep(2)

    def set_knobs(self, knob_config: KnobConfig):
        """Set database configuration using SET PERSIST_ONLY (MySQL 8.0+)."""
        cursor = self.connection.cursor()
        try:
            for knob in knob_config.knobs:
                try:
                    cursor.execute(f"SET PERSIST_ONLY {knob.name} = %s;", (knob.value,))
                except mysql.connector.Error as inner_e:
                    self.logger.error(f"Failed to set {knob.name}={knob.value}: {inner_e}")
            
            self.connection.commit()
            self.logger.info("MySQL knobs persisted to mysqld-auto.cnf. Restarting to apply...")
            self.restart_db()
            
        except Exception as e:
            self.logger.error(f"Critical error during set_knobs: {e}")

    def fetch_internal_metrics(self) -> InternalMetrics:
        """Fetch internal metrics from performance_schema and Global Status."""
        metrics: InternalMetrics
        with self.connection.cursor(dictionary=True) as cursor:
            try:
                # Fetch Global Status Variables
                cursor.execute("SHOW GLOBAL STATUS WHERE Variable_name IN ("
                            "'Com_commit', 'Com_rollback', 'Innodb_buffer_pool_read_requests', "
                            "'Innodb_buffer_pool_reads', 'Rows_read', 'Rows_inserted', "
                            "'Rows_updated', 'Rows_deleted', 'Innodb_data_reads', 'Innodb_data_writes');")
                
                res = {row['Variable_name']: float(row['Value']) for row in cursor.fetchall()}

                # Mapping MySQL status to your InternalMetrics structure
                metrics = {
                    "xact_commit": res.get('Com_commit', 0.0),
                    "xact_rollback": res.get('Com_rollback', 0.0),
                    "blks_read": res.get('Innodb_buffer_pool_reads', 0.0),
                    "blks_hit": res.get('Innodb_buffer_pool_read_requests', 0.0) - res.get('Innodb_buffer_pool_reads', 0.0),
                    "tup_returned": res.get('Rows_read', 0.0),
                    "tup_fetched": res.get('Rows_read', 0.0), # MySQL doesn't distinguish exactly like PG here
                    "tup_inserted": res.get('Rows_inserted', 0.0),
                    "conflicts": 0.0, # Placeholder for Deadlocks/Conflicts
                    "tup_updated": res.get('Rows_updated', 0.0),
                    "tup_deleted": res.get('Rows_deleted', 0.0),
                    "disk_read_count": res.get('Innodb_data_reads', 0.0),
                    "disk_write_count": res.get('Innodb_data_writes', 0.0),
                    "disk_read_bytes": res.get('Innodb_data_reads', 0.0) * 16384.0, # MySQL default page is 16KB
                    "disk_write_bytes": res.get('Innodb_data_writes', 0.0) * 16384.0,
                }
            except Exception as e:
                self.logger.error(f"Error fetching metrics: {e}")
                # Return zeroed metrics on failure as per your template
                metrics = {k: 0.0 for k in [
                    "xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", 
                    "tup_fetched", "tup_inserted", "conflicts", "tup_updated", "tup_deleted",
                    "disk_read_count", "disk_write_count", "disk_read_bytes", "disk_write_bytes"
                ]}
            finally:
                cursor.close()
        return metrics
    
    def extract_query_plans(self, workload_path: Path) -> List[str]:
            # 1. Read the script
            with open(workload_path, "r") as f:
                sql_script = f.read()

            # 2. Transpile to MySQL syntax so EXPLAIN doesn't fail on Postgres syntax
            import sqlglot
            try:
                transpiled_statements = sqlglot.transpile(sql_script, read="postgres", write="mysql")
                workload_queries = [q.strip() for q in transpiled_statements if q.strip()]
            except Exception as e:
                self.logger.warning(f"Transpilation failed, falling back to raw split: {e}")
                workload_queries = [q.strip() for q in sql_script.split(";") if q.strip()]

            plans: List[str] = []

            with self.connection.cursor() as cursor:
                for i, query in enumerate(workload_queries):
                    try:
                        self.logger.info(f"Explaining query {i + 1}/{len(workload_queries)}")
                        # MySQL uses FORMAT=JSON without parentheses
                        cursor.execute(f"EXPLAIN FORMAT=JSON {query}")
                        row = cursor.fetchone()
                        self.connection.commit()
                        
                        # MySQL returns the JSON as a string in the first column
                        plan_json_str = row[0] 
                        formatted_plan = self._format_query_plan(plan_json_str)
                        plans.append(formatted_plan)
                        
                    except Exception as e:
                        self.logger.error(f"Error explaining query {i + 1}: {e}")
                        self.logger.debug(f"Query (truncated): {query[:100]}...")

            return plans
    
    def reset_internal_metrics(self):
        """MySQL doesn't have a single 'reset' like PG; usually requires a restart or FLUSH."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("FLUSH STATUS;")
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error resetting metrics: {e}")
        finally:
            cursor.close()

    def reset_knobs(self):
        """Clear all persisted variables."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("RESET PERSIST;")
            self.connection.commit()
            self.logger.info("MySQL persisted knobs have been cleared.")
        except Exception as e:
            self.logger.error(f"Error resetting knobs: {e}")
        finally:
            cursor.close()

    def restart_db(self, stop_timeout: int = 30, start_timeout: int = 30) -> bool:
        """Restarts MySQL via systemd and actively waits for it to be ready."""
        try:
            self.logger.info("Restarting MySQL service...")
            
            # 1. Trigger the restart
            subprocess.run(
                ["sudo", "systemctl", "restart", "mysql"], 
                check=True, 
                timeout=stop_timeout + start_timeout
            )
            
            # 2. Actively poll for a connection instead of sleeping for 5 seconds
            self.logger.info("Waiting for MySQL to become available...")
            start_wait = time.time()
            max_wait = 15.0  # Maximum seconds to wait for MySQL to come back
            
            while time.time() - start_wait < max_wait:
                try:
                    # Attempt a silent, temporary connection
                    conn = mysql.connector.connect(
                        database=self.db_config.name,
                        user=self.db_config.user,
                        password=self.db_config.password,
                        host=self.db_config.host,
                        port=self.db_config.port
                    )
                    conn.close()
                    
                    # If we got here, MySQL is ready!
                    ready_time = time.time() - start_wait
                    self.logger.info(f"MySQL is back up! (Took {ready_time:.2f}s)")
                    
                    # Establish the permanent connection for the class
                    self.connect() 
                    return True
                    
                except mysql.connector.Error:
                    # Not ready yet, wait a tiny bit and try again
                    time.sleep(0.5)

            self.logger.error("MySQL did not become ready within the timeout period.")
            return False

        except Exception as e:
            self.logger.error(f"Failed to restart MySQL: {e}")
            return False

    def run_workload(self, workload_task: BenchmarkTask, runs_per_iteration: Optional[int] = 1) -> tuple[float, float]:
            # 1. Read the raw PostgreSQL script
            with open(workload_task.workload_path, "r") as f:
                raw_sql_script = f.read()
                
            self.logger.info("Transpiling workload from PostgreSQL to MySQL syntax...")
            
            # 2. Transpile using sqlglot
            try:
                # transpile() returns a list of translated SQL statements
                transpiled_statements = sqlglot.transpile(
                    raw_sql_script, 
                    read="postgres", 
                    write="mysql",
                    pretty=True # Formats the output nicely for debugging if needed
                )
                
                # Filter out empty statements (like trailing semicolons)
                valid_queries = [q for q in transpiled_statements if q.strip()]
                
                # Join them back into a multi-statement script for the MySQL connector
                sql_script = ";\n".join(valid_queries) + ";"
                num_queries = len(valid_queries)
                
            except Exception as e:
                self.logger.error(f"SQL Transpilation failed for {workload_task.workload_path}: {e}")
                # If transpilation fails, we cannot run the workload
                return float("inf"), 0.0
            
            self.set_knobs(workload_task.knob_config)
            self.logger.info(f"Executing transpiled workload {workload_task.workload_path} ({num_queries} queries)...")
            
            # 3. Execute the MySQL script
            with self.connection.cursor() as cursor:
                try:
                    sum_latency = 0.0
                    sum_throughput = 0.0
                    
                    for _ in range(runs_per_iteration):
                        start = time.perf_counter()
                        
                        cursor.execute(sql_script)

                        while cursor.nextset():
                            pass
                                
                        self.connection.commit()
                        end = time.perf_counter()
                        
                        total_time = end - start
                        sum_latency += (
                            total_time / num_queries if num_queries > 0 else 0.0
                        )
                        sum_throughput += (
                            num_queries / total_time if total_time > 0 else 0.0
                        )
                        
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
                finally:
                    cursor.close()

    @staticmethod
    def _format_query_plan(plan_input: Any) -> str:
        """
        Parse and format a MySQL JSON query plan into a compact summary string.
        Returns a compact representation: NodeType(cost=X.X)(child1; child2; ...)
        """
        # Parse raw string if necessary
        if isinstance(plan_input, str):
            try:
                plan_json = json.loads(plan_input)
            except json.JSONDecodeError:
                return "Invalid JSON plan"
        else:
            plan_json = plan_input

        def extract_mysql_node(node: Any, node_name: str = "Query") -> str:
            # If it's a list (like inside a "nested_loop"), process all children
            if isinstance(node, list):
                children = [extract_mysql_node(child, node_name) for child in node]
                return "; ".join(filter(None, children))

            if not isinstance(node, dict):
                return ""

            # 1. Extract Cost
            cost_str = ""
            cost_info = node.get("cost_info", {})
            # MySQL costs can be under query_cost, read_cost, or eval_cost
            cost_val = cost_info.get("query_cost") or cost_info.get("read_cost") or cost_info.get("eval_cost")
            if cost_val is not None:
                cost_str = f"(cost={float(cost_val):.1f})"

            # 2. Handle Leaf Nodes (Table Scans)
            if "table" in node or node_name == "Table":
                t = node.get("table", node)
                access = t.get("access_type", "Scan").capitalize()
                tname = t.get("table_name", "Unknown")
                t_cost = t.get("cost_info", {}).get("read_cost", cost_val or 0)
                return f"Table{access}_{tname}(cost={float(t_cost):.1f})"

            # 3. Handle Structural Nodes (Joins, Groupings, etc.)
            # Keys we want to skip because they are metadata, not execution nodes
            ignore_keys = {
                "select_id", "cost_info", "table", "attached_condition", 
                "used_columns", "key", "key_length", "rows_examined_per_scan", 
                "rows_produced_per_join", "filtered", "message"
            }
            
            children_summaries = []
            
            for key, value in node.items():
                if key not in ignore_keys and (isinstance(value, dict) or isinstance(value, list)):
                    # Convert keys like 'nested_loop' to 'NestedLoop'
                    child_node_name = key.replace("_", " ").title().replace(" ", "")
                    child_summary = extract_mysql_node(value, child_node_name)
                    if child_summary:
                        children_summaries.append(child_summary)

            # Build current node string
            summary = f"{node_name}{cost_str}"
            if children_summaries:
                summary += "(" + "; ".join(children_summaries) + ")"
                
            return summary

        # MySQL query plans typically start wrapped in a 'query_block'
        root = plan_json.get("query_block", plan_json)
        return extract_mysql_node(root, "QueryBlock")
