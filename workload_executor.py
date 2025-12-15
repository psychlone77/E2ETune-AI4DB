import os
import time
import json
import subprocess
from typing import Dict, Any, List, Optional

from Database import Database


class workload_executor:
    """
    Execute workloads for tuning:
      - OLAP: .wg files containing SQL queries (one per line, comments prefixed with --)
      - OLTP: BenchBase profiles via command-line runner
      - Surrogate: optional fast prediction path for OLAP

    Returns a scalar performance metric (TPS/QPS). Higher is better.
    """

    def __init__(self, args: Dict[str, Any], logger, internal_metrics: Optional[Dict[str, float]]):
        self.args = args
        self.logger = logger
        self.internal_metrics = internal_metrics
        self.db = Database(config=args, knob_config_path=args['tuning_config']['knob_config'])

    def run_config(self, config: Optional[Dict[str, Any]], workload_file: str) -> float:
        """
        Apply knob config (if provided) and run the workload, returning performance.
        Detects OLAP vs OLTP from benchmark_config.benchmark.
        """
        if config:
            self.logger.info(f"Applying config")
            ok = self.db.change_knob(config)
            if not ok:
                self.logger.warning("Failed to apply some knobs; continuing with current settings")

        benchmark = self.args['benchmark_config'].get('benchmark', '').lower()

        if benchmark in ['tpch', 'job', 'olap', 'dwg']:
            return self._run_olap(workload_file)
        else:
            # Treat others as OLTP/BenchBase (tpcc, ycsb, smallbank, wikipedia, twitter)
            return self._run_oltp(workload_file)

    def run_config_surrogate(self, config: Dict[str, Any], workload_file: str) -> float:
        """
        Surrogate path for OLAP. Implement your model loading and prediction here.
        """
        # Example surrogate: sum of knob weights and query count as a proxy
        query_count = self._count_olap_queries(workload_file)
        weight = sum(float(v) for v in config.values() if isinstance(v, (int, float)))
        score = max(1.0, query_count) * (1.0 + 0.01 * weight)
        self.logger.info(f"Surrogate score (OLAP): {score:.3f}")
        return float(score)

    # ---------- OLAP ----------

    def _run_olap(self, workload_file: str) -> float:
        """
        Execute OLAP queries sequentially and measure queries/sec (QPS).
        """
        queries = self._load_wg_queries(workload_file)
        if not queries:
            self.logger.warning(f"No queries found in {workload_file}")
            return 0.0

        conn = self.db.get_conn()
        cur = conn.cursor()
        start = time.time()
        executed = 0

        for q in queries:
            try:
                cur.execute(q)
                executed += 1
            except Exception as e:
                self.logger.error(f"Query failed: {e}\nQuery: {q[:200]}")
                # continue to next query

        conn.commit()
        cur.close()
        conn.close()

        elapsed = max(1e-6, time.time() - start)
        qps = executed / elapsed
        self.logger.info(f"OLAP executed {executed} queries in {elapsed:.2f}s -> QPS={qps:.3f}")
        return float(qps)

    def _load_wg_queries(self, path: str) -> List[str]:
        """
        Load .wg workload file: one SQL per line; skip comments/blank lines.
        """
        queries: List[str] = []
        with open(path, 'r') as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith('--'):
                    continue
                # Some .wg lines may contain semicolons; leave as-is
                queries.append(s)
        return queries

    def _count_olap_queries(self, path: str) -> int:
        try:
            return len(self._load_wg_queries(path))
        except Exception:
            return 0

    # ---------- OLTP / BenchBase ----------

    def _run_oltp(self, workload_file: str) -> float:
        """
        Invoke BenchBase for the configured benchmark/profile.
        Expects args['benchmark_config'] to provide:
          - benchmark: e.g., tpcc, ycsb
          - config_path or profile path pointing to XML config
          - Optionally: BENCHBASE_HOME env or args['benchmark_config']['benchbase_home']

        Parses TPS from BenchBase results or logs.
        """
        bb_home = (
            os.environ.get("BENCHBASE_HOME")
            or self.args['benchmark_config'].get('benchbase_home')
        )
        if not bb_home:
            self.logger.error("BenchBase home not set. Set BENCHBASE_HOME or benchmark_config.benchbase_home.")
            return 0.0

        benchmark = self.args['benchmark_config'].get('benchmark', 'tpcc')
        # workload_file may be an XML; if not provided, use config_path from args
        profile = workload_file if workload_file.endswith('.xml') \
            else self.args['benchmark_config'].get('config_path')

        if not profile or not os.path.exists(profile):
            self.logger.error(f"BenchBase profile XML not found: {profile}")
            return 0.0

        results_dir = os.path.abspath(self.args['tuning_config'].get('log_path', 'log'))
        os.makedirs(results_dir, exist_ok=True)

        # Typical BenchBase invocation:
        # ./benchbase -b tpcc -c config.xml --execute=true
        cmd = [
            os.path.join(bb_home, "benchbase"),
            "-b", benchmark,
            "-c", profile,
            "--execute=true",
            "--results", results_dir
        ]

        self.logger.info(f"Running BenchBase: {' '.join(cmd)}")
        try:
            proc = subprocess.run(cmd, text=True, capture_output=True, timeout=3600)
        except Exception as e:
            self.logger.error(f"BenchBase failed to start: {e}")
            return 0.0

        if proc.returncode != 0:
            self.logger.error(f"BenchBase returned {proc.returncode}\nSTDERR:\n{proc.stderr}")
            # still try to parse any results that might have been written
        else:
            self.logger.info(f"BenchBase finished.\nSTDOUT:\n{proc.stdout[:400]}...")

        # Attempt to parse TPS from the latest result JSON file in results_dir
        tps = self._parse_benchbase_tps(results_dir)
        self.logger.info(f"BenchBase TPS: {tps:.3f}")
        return float(tps)

    def _parse_benchbase_tps(self, results_dir: str) -> float:
        """
        Try to parse TPS from BenchBase results:
         - Looks for summary.json or histograms in results_dir (latest file).
         - Fallback: regex parse from stdout/stderr is possible but less reliable.

        Returns 0.0 if not found.
        """
        try:
            files = sorted(
                (os.path.join(results_dir, f) for f in os.listdir(results_dir)),
                key=lambda p: os.path.getmtime(p),
                reverse=True
            )
        except Exception:
            return 0.0

        for fpath in files:
            if fpath.endswith(".json"):
                try:
                    with open(fpath, "r") as f:
                        data = json.load(f)
                    # Common BenchBase summary fields vary; try typical keys
                    # e.g., data["metrics"]["Throughput(req/sec)"] or data["Throughput (req/sec)"]
                    metrics = data.get("metrics") or data
                    for key in ["Throughput(req/sec)", "Throughput (req/sec)", "TPS", "tps"]:
                        val = metrics.get(key)
                        if isinstance(val, (int, float)):
                            return float(val)
                    # Sometimes per-phase metrics exist; take max
                    if isinstance(metrics, dict):
                        candidates = []
                        for v in metrics.values():
                            if isinstance(v, (int, float)):
                                candidates.append(float(v))
                        if candidates:
                            return max(candidates)
                except Exception:
                    continue
        return 0.0