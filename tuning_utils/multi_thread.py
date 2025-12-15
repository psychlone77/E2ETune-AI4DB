import psycopg2
import threading
import time
import re
from datetime import datetime

# If an iteration encounters any query errors, we invalidate the
# iteration by returning zero throughput and a very large average
# latency. Use a large finite value to remain JSON/DB friendly.
LARGE_INVALID_LATENCY = 1e9

def generate_random_string(length=None):
    """Generate timestamp-based identifier for workload runs."""
    now = datetime.now()
    timestamp_str = now.strftime("%m%d_%H_%M_%S")
    return timestamp_str


def connect_og(database_name, user_name, password, host, port):
    """Create a PostgreSQL connection and cursor."""
    connection = psycopg2.connect(
        database=database_name,
        user=user_name,
        password=password,
        host=host,
        port=port
    )
    cur = connection.cursor()
    return connection, cur


class ThreadStats:
    """Container for thread execution statistics."""
    def __init__(self, query_count, total_latency, error_count=0):
        self.value = query_count # Total queries executed successfully
        self.type = total_latency # Sum of all query execution times (seconds)
        self.error_count = error_count # Number of failed queries


class one_thread_given_queries(threading.Thread):
    """
    Thread-safe worker that executes a subset of SQL queries.
    
    Each thread creates its own database connection to avoid
    psycopg2 cursor race conditions.
    """
    
    def __init__(self, wg, log_path, db_config, thread_id, time_stamp):
        """
        Args:
            wg: List of SQL queries for this thread to execute
            log_path: Path to write thread execution logs
            db_config: Dict with keys: database, user, password, host, port
            thread_id: Unique identifier for this thread
            time_stamp: Shared dict to store ThreadStats (thread-safe for write-once pattern)
        """
        threading.Thread.__init__(self)
        self.wg = wg
        self.log_path = log_path
        self.db_config = db_config
        self.thread_id = thread_id
        self.time_stamp = time_stamp

    def run(self):
        """Execute assigned queries and track accurate performance metrics."""
        connection = None
        cur = None
        
        try:
            # Each thread creates its own connection (thread-safe)
            connection, cur = connect_og(
                database_name=self.db_config['database'],
                user_name=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )
            
            sql_list = self.wg
            query_count = 0
            error_count = 0
            total_query_latency = 0.0  # Sum of individual query execution times
            
            with open(self.log_path, 'a') as f:
                f.write(f"Thread {self.thread_id} started. Total queries: {len(sql_list)}\n")
                print(f"Thread {self.thread_id} started with {len(sql_list)} queries")
                
                for i, sql in enumerate(sql_list):
                    try:
                        # Track exact execution time for each query
                        query_start = time.time()
                        cur.execute(sql)
                        connection.commit()
                        query_end = time.time()
                        
                        query_latency = query_end - query_start
                        total_query_latency += query_latency
                        query_count += 1
                        
                        # Log progress periodically to avoid I/O overhead
                        if (i + 1) % 100 == 0 or (i + 1) == len(sql_list):
                            f.write(f"Thread {self.thread_id}: {i + 1}/{len(sql_list)} queries done. "
                                   f"Cumulative latency: {total_query_latency:.4f}s\n")
                            print(f"Thread {self.thread_id}: {i + 1}/{len(sql_list)} queries processed")
                    
                    except Exception as e:
                        # SAFETY GUARD: Track query-level errors
                        error_count += 1
                        error_msg = f"Thread {self.thread_id} Error on query {i}: {e}\n"
                        f.write(error_msg)
                        print(error_msg.strip())
                        continue
                
                # Store accurate statistics including error count
                self.time_stamp[self.thread_id] = ThreadStats(query_count, total_query_latency, error_count)
                
                f.write(f"Thread {self.thread_id} completed: {query_count} queries, "
                       f"total latency: {total_query_latency:.4f}s\n")
                print(f"Thread {self.thread_id} finished: {query_count} queries")
        
        except Exception as e:
            error_msg = f"Thread {self.thread_id} Fatal Error: {e}"
            print(error_msg)
            # Ensure we still record stats even on fatal error
            if self.thread_id not in self.time_stamp:
                self.time_stamp[self.thread_id] = ThreadStats(0, 0.0)
        
        finally:
            # Always close connection in finally block
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass


class multi_thread:
    """
    Multi-threaded workload executor with proper connection handling and accurate metrics.
    
    No longer shares connections across threads. Calculates accurate
    throughput and latency metrics based on actual query execution times.
    """
    
    def __init__(self, db, workload_path, thread_num, log_path):
        """
        Args:
            db: Database connection info object with attributes: database, user, password, host, port
            workload_path: Path to .wg workload file
            thread_num: Number of worker threads to spawn
            log_path: Path for summary log file
        """
        self.wg_file = None
        self.id = generate_random_string(10)
        self.workload_name = workload_path
        self.thread_num = thread_num
        self.db = db
        self.wg_path = workload_path
        self.log_path = log_path
        self.sql_list_idx = dict()

    def data_pre(self):
        """
        Load workload file and partition SQL queries across threads.
        Uses round-robin distribution for load balancing.
        """
        # Load workload file
        with open(self.wg_path, 'r') as f:
            self.wg_file = f.read()

        # Split into individual queries
        sql_list = re.split(r'[;\n]+', self.wg_file)
        for i, query in enumerate(sql_list):
            sql_list[i] = query.strip() + ";"

        # Remove empty trailing query
        if sql_list[-1] == ";":
            sql_list = sql_list[0:-1]

        # Limit workload size to prevent extremely long runs
        if len(sql_list) > 3000:
            print(f"Warning: Workload has {len(sql_list)} queries, limiting to 3000")
            sql_list = sql_list[:3000]

        # Partition queries across threads using round-robin
        self.sql_list_idx = {i: [] for i in range(self.thread_num)}
        
        for i, query in enumerate(sql_list):
            thread_id = i % self.thread_num
            self.sql_list_idx[thread_id].append(query)
        
        print(f"Workload partitioned: {len(sql_list)} queries across {self.thread_num} threads")
        for i in range(self.thread_num):
            print(f"  Thread {i}: {len(self.sql_list_idx[i])} queries")

    def run(self) -> dict:
        """
        Execute the workload across multiple threads.
        
        Calculates accurate metrics:
        - Throughput (QPS) = Total queries / Wall clock time
        - Avg Latency = Sum of all query latencies / Total queries
        
        Returns:
            dict with keys:
                - avg_time_per_query: Average latency per query (seconds)
                - throughput_qps: Queries per second (QPS)
        """
        # Prepare DB config dict instead of sharing connection/cursor
        db_config = {
            'database': self.db.database,
            'user': self.db.user,
            'password': self.db.password,
            'host': self.db.host,
            'port': self.db.port
        }
        
        threads = []
        time_stamp = dict()  # Shared dict to collect thread statistics
        
        # Create worker threads - each will make its own connection
        for i in range(self.thread_num):
            thread = one_thread_given_queries(
                wg=self.sql_list_idx[i],
                log_path=self.log_path + f".thread{i}",
                db_config=db_config,
                thread_id=i,
                time_stamp=time_stamp
            )
            threads.append(thread)
        
        # Execute workload and measure wall clock time
        print(f"Starting workload execution with {self.thread_num} threads...")
        start_time = time.time()
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        total_wall_time = end_time - start_time
        
        # Aggregate accurate statistics from all threads
        total_queries = 0
        total_latency = 0.0
        total_errors = 0
        missing_threads = 0
        
        for i in range(self.thread_num):
            if i in time_stamp:
                total_queries += time_stamp[i].value
                total_latency += time_stamp[i].type
                # Count any query-level errors
                try:
                    total_errors += time_stamp[i].error_count
                except Exception:
                    # Defensive: if ThreadStats shape changed, treat as error
                    total_errors += 1
            else:
                # A missing thread entry is suspicious (fatal)
                missing_threads += 1
        
        # Calculate accurate metrics
        # Throughput = Total queries executed / Wall clock time
        throughput_qps = total_queries / total_wall_time if total_wall_time > 0 else 0.0
        
        # Average latency = Sum of all query latencies / Total queries executed
        avg_latency = total_latency / total_queries if total_queries > 0 else 0.0

        if total_errors > 0 or missing_threads > 0:
            throughput_qps = 0.0
            avg_latency = LARGE_INVALID_LATENCY
            with open(self.log_path, 'a') as f:
                f.write("\n! INVALID ITERATION: detected errors or missing threads.\n")
                f.write(f"  total_errors={total_errors}, missing_threads={missing_threads}.\n")
                f.write(f"  Setting Throughput (QPS)=0.0 and Average latency={avg_latency}\n")
            print("INVALID ITERATION: errors/missing threads detected. Returning QPS=0 and large avg latency.")
        
        # Write comprehensive summary to log
        with open(self.log_path, 'w') as f:
            f.write("="*70 + "\n")
            f.write("WORKLOAD EXECUTION SUMMARY\n")
            f.write("="*70 + "\n")
            f.write(f"Workload: {self.workload_name}\n")
            f.write(f"Threads: {self.thread_num}\n")
            f.write(f"Total wall clock time: {total_wall_time:.4f} seconds\n")
            f.write(f"Total queries executed: {total_queries}\n")
            f.write(f"Sum of query latencies: {total_latency:.4f} seconds\n")
            f.write(f"\nPERFORMANCE METRICS:\n")
            f.write(f"  Throughput (QPS): {throughput_qps:.4f}\n")
            f.write(f"  Average latency per query: {avg_latency:.6f} seconds\n")
            f.write(f"\nPER-THREAD BREAKDOWN:\n")
            
            for i in range(self.thread_num):
                if i in time_stamp:
                    stats = time_stamp[i]
                    thread_avg = stats.type / stats.value if stats.value > 0 else 0.0
                    f.write(f"  Thread {i}: {stats.value} queries, "
                           f"{stats.type:.4f}s total latency, "
                           f"{thread_avg:.6f}s avg latency\n")
            
            f.write("="*70 + "\n")
        
        print(f"\nWorkload execution completed!")
        print(f"  Total queries: {total_queries}")
        print(f"  Wall clock time: {total_wall_time:.4f}s")
        print(f"  Throughput: {throughput_qps:.4f} QPS")
        print(f"  Average latency: {avg_latency:.6f}s")
        
        return {
            "avg_time_per_query": avg_latency,
            "throughput_qps": throughput_qps
        }

