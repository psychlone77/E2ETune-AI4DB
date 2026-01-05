"""
Stress testing tool for evaluating database configurations under workload.
Supports OLAP (dwg), OLTP (sysbench, tpcc), and surrogate model evaluation.
"""
import copy
import os
import time
import logging
from typing import Dict, Any, Optional
import json

import numpy as np

from tuning_utils.multi_thread import multi_thread
from tuning_utils.surrogate import Surrogate
from knob_config import parse_knob_config
from benchbase_runner import BenchBaseRunner


class stress_testing_tool:
    """
    Execute workloads under specific knob configurations and measure performance.
    
    Supports:
    - OLAP workloads via multi-threaded query execution (dwg)
    - OLTP benchmarks via sysbench and tpcc
    - Surrogate model-based fast evaluation
    """
    
    def __init__(self, config: Dict[str, Any], db, logger: logging.Logger, records_log: str):
        """Initialize stress testing tool."""
        self.args = config
        self.benchmark_config = config['benchmark_config']
        self.db = db
        self.sur_config = config.get('surrogate_config', {})
        self.logger = logger
        self.records_log = records_log
        self.iteration_count = 0
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(self.records_log), exist_ok=True)
        perf_record_dir = self.benchmark_config.get('performance_record_path', 'logs/performance_record')
        os.makedirs(perf_record_dir, exist_ok=True)

    def test_config(self, config: Dict[str, Any], iteration: Optional[int] = None) -> float:
        """Test a configuration and return performance metric."""
        self.iteration_count += 1
        if iteration is None:
            iteration = self.iteration_count
            
        workload_name = os.path.basename(self.benchmark_config.get('workload_path', 'unknown'))
        self.logger.info(f"[Iteration {iteration}] [Workload: {workload_name}] Testing configuration")
        
        temp_config = copy.deepcopy(config)
        cur_state = []
        tool = self.benchmark_config.get('tool', 'dwg')

        # Apply knobs and fetch metrics
        if tool != 'surrogate':
            self.logger.info(f"[Iteration {iteration}] Applying knobs to database")
            flag = self.db.change_knob(config)
            if not flag:
                self.logger.warning(f"[Iteration {iteration}] Some knobs failed to apply")
            
            self.logger.info(f"[Iteration {iteration}] Fetching internal metrics")
            cur_state = self.db.fetch_inner_metrics()

        if tool == 'dwg':
            self.logger.info(f"[Iteration {iteration}] Running DWG OLAP workload")
            y = self._test_by_dwg(
                self.benchmark_config['workload_path'],
                self.benchmark_config.get('log_path', 'logs/performance/workload_execution.log'),
                iteration
            )['throughput_qps']
            y = y[0] if isinstance(y, (list, tuple)) else y

        elif tool == 'benchbase':
            self.logger.info(f"[Iteration {iteration}] Running BenchBase benchmark")
            y = self.test_by_benchbase(
                self.benchmark_config['workload_path'],
                self.benchmark_config.get('log_path', 'logs/performance/benchbase_execution.log')
            )

        elif tool == 'surrogate':
            self.logger.info(f"[Iteration {iteration}] Running surrogate model")
            y = self._test_by_surrogate(cur_state, self.benchmark_config['workload_path'], 
                                        self.sur_config, config, iteration)
        else:
            self.logger.error(f"[Iteration {iteration}] Unknown tool: {tool}")
            return 0.0

        self.logger.info(f"[Iteration {iteration}] Performance: {y:.4f}")

        # Record to training log
        temp_config['tps'] = y
        temp_config['iteration'] = iteration
        temp_config['workload'] = workload_name
        with open(self.records_log, 'a') as f:
            f.write(json.dumps(temp_config) + '\n')
        
        # Record to offline sample
        if tool != 'surrogate':
            offline_path = 'logs/offline_sample/offline_sample.jsonl'
            os.makedirs(os.path.dirname(offline_path), exist_ok=True)
            with open(offline_path, 'a') as f:
                temp_config['y'] = [-y, 1/(-y)] if y != 0 else [0, 0]
                temp_config['inner_metrics'] = cur_state
                temp_config['workload'] = self.benchmark_config['workload_path']
                f.write(json.dumps(temp_config) + '\n')

        # Record to per-workload performance file
        perf_dir = self.benchmark_config.get('performance_record_path', 'logs/performance_record')
        perf_file = os.path.join(perf_dir, f"{workload_name}.txt")
        with open(perf_file, 'a') as w:
            w.write(f"[Iteration {iteration}] Performance: {y:.4f}\n")

        return y
    
    def test_by_benchbase(self, workload_path, log_file):
        # Test the database performance using benchbase
        benchbase_runner = BenchBaseRunner(self.args, self.logger)
        return benchbase_runner.run_benchmark(workload_path, log_file)

    def _test_by_dwg(self, workload_path: str, log_file: str, iteration: int) -> tuple:
        """Execute OLAP workload via multi-threaded DWG."""
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        thread_count = int(self.benchmark_config.get('thread', 1))
        mh = multi_thread(self.db, workload_path, thread_count, log_file)

        mh.data_pre()
        
        self.logger.info(f"[Iteration {iteration}] Running with {thread_count} thread(s)")
        result = mh.run()
        
        self.logger.info(f"[Iteration {iteration}] DWG complete, result: {result}")
        return result
    
    def _test_by_surrogate(self, inner_metrics: list, workload_path: str, 
                           sur_config: Dict[str, Any], knobs: Dict[str, Any], 
                           iteration: int) -> float:
        """Predict performance using surrogate model."""
        self.logger.info(f"[Iteration {iteration}] Loading surrogate model")
        sg = Surrogate(sur_config, workload_path)
        
        knob_detail = parse_knob_config.get_knobs('knob_config/knob_config.json')
        x = []
        
        # Normalize knob values to [0, 1]
        for key in knob_detail.keys():
            detail = knob_detail[key]
            if detail['max'] - detail['min'] != 0:
                normalized = (knobs[key] - detail['min']) / (detail['max'] - detail['min'])
                x.append(normalized)

        self.logger.info(f"[Iteration {iteration}] Running surrogate prediction")
        prediction = sg.run(inner_metrics, x)
        self.logger.info(f"[Iteration {iteration}] Surrogate prediction: {prediction:.4f}")
        return prediction