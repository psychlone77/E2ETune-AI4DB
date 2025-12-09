import csv
import time
import json
from typing import Dict, List, Optional, Tuple, Any

# NumPy removed top-level NaN in newer versions; use `nan` and alias it.
from numpy import nan as NaN

import Database
import utils
from config import parse_config
import stress_testing_tool
from tuner import tuner
from knob_config.parse_knob_config import get_knobs

import argparse

# def test_fluctuation(config):
#     knobs_detail = parse_knob_config.get_knobs(config['tuning_config']['knob_config'])
#     knob_default = {}
#     for index, knob in enumerate(knobs_detail):
#         knob_default[knob] = knobs_detail[knob]['default']
#     all_result = []
#     repeat = 5
#     for i in range(repeat):

default = [16.0, 3.0, 200.0, 2048.0, 4096.0, 0.1, 50.0, 600.0, 2.0, -1.0, 0.2, 50.0, 0.0, 2000.0, 64.0, 100.0, 2.0, 0.5, 32.0, 900.0, 0.0, 5.0, 0.1, 1000.0, 100.0, 16384.0, 1.0, 8.0, 5.0, 0.0, 0.0, 0.0, 12.0, 8.0, 16384.0, 128.0, -1.0, 0.0, 200.0, 20.0, 1.0, 10.0, 200.0, 65536.0]


def test_surrogate_result(key: str, args: Dict[str, Any], config: Dict[str, float]) -> None:
    """
    Execute a short repeated test for a provided configuration and record results.

    - Determines if the workload is OLAP or OLTP based on `workload_path`.
    - Tests both the default configuration and the provided `config` several times.
    - Writes JSON Lines records to `record/olap_surrogate_record.jsonl` or
      `record/oltp_surrogate_record.jsonl` accordingly.

    Parameters
    ----------
    key : str
        Workload identifier used for the record.
    args : dict
        Parsed configuration dictionary used across DB and benchmarking.
    config : dict
        The knob-value mapping to test.
    """
    olap = False
    if args['benchmark_config']['workload_path'].startswith('SuperWG'):
        olap = True
        args['benchmark_config']['tool'] = 'dwg'
    else:
        olap = False
        benchmark = args['benchmark_config']['workload_path'].split('/')[0]
        args['benchmark_config']['tool'] = benchmark
        args['benchmark_config']['config_path'] = args['benchmark_config']['workload_path']
        args['database_config']['database'] = 'benchbase'
    database = Database(args, 'knob_config/knob_config.json')
    logger = utils.get_logger(args['tuning_config']['log_path'])
    sample = args['tuning_config']['finetune_sample']
    stt = stress_testing_tool(args, database, logger, sample)
    knobs_detail = get_knobs('knob_config/knob_config.json')
    print(f'test workload {key}')

    cur_point = config
    point: Dict[str, float] = {}
    default_point: Dict[str, float] = {}
    for index, knob in enumerate(knobs_detail):
        # point[knob] = float(cur_point[index])
        default_point[knob] = float(default[index])
    point = config
    repeat = 3
    best_test: List[float] = []
    default_test: List[float] = []
    for j in range(repeat):
        y = stt.test_config(default_point)
        default_test.append(y)
    inner = database.fetch_inner_metric()
    for j in range(repeat):
        y = stt.test_config(point)
        best_test.append(y)
    
        # with open(f'all_workload_test{cmd.workload}.txt', 'a') as w:
        #     w.write("step {}: performance: {}\n".format(j, y))
    
    # if max(best_test) > max(default_test):
    if olap:
        with open('record/olap_surrogate_record.jsonl', 'a') as w:
            # strs = json.dumps({'workload': key, 'inner': inner, 'default_tps': [float(i) for i in default_test], \
            #             'best_tps': [float(i) for i in best_test], 'best_config': point, \
            #             'delta': (max(best_test) - max(default_test))})
            strs = json.dumps({'workload': key, 'inner': inner, 'best_config': point, 'best': best_test, 'default': default_test})
            w.write(strs + '\n')
    else:
        with open('record/oltp_surrogate_record.jsonl', 'a') as w:
            # strs = json.dumps({'workload': key, 'inner': inner, 'default_tps': [float(i) for i in default_test], \
            #             'best_tps': [float(i) for i in best_test], 'best_config': point, \
            #             'delta': (max(best_test) - max(default_test))})
            strs = json.dumps({'workload': key, 'inner': inner, 'best_config': point, 'best': best_test, 'default': default_test})
            w.write(strs + '\n')

def tune(workload: str, host: str, args: Dict[str, Any]) -> None:
    """
    Run the tuning process, summarize offline sample performance, and record results.

    - Invokes the `tuner` to produce a (potential) best configuration.
    - Parses the offline sample file to compute default TPS, best TPS, and undulation.
    - Writes results to `record/offine_record.jsonl` (OLTP/OLAP paths differ when surrogate tool used).

    Parameters
    ----------
    workload : str
        Workload identifier for record keeping.
    host : str
        Host string used to locate inner metrics file.
    args : dict
        Parsed configuration dictionary for tuning and evaluation.
    """
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # try: 
    t = tuner(args).tune()
    # except Exception as e:
    #     print(f'an error occurred during tuning: {e}')
    
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def get_tps(line: str) -> Optional[Tuple[float, str]]:
        """
        Extract TPS and configuration substring from a line.

        The function expects lines containing a "tps:" segment followed by a numeric value
        and a closing brace. Returns None if parsing fails or the TPS value is NaN.
        """
        if "tps" not in line:
            return None
        try:
            tps_segment = line[line.find("tps"):]
            tps_value_str = tps_segment.split(":", 1)[1].split("}")[0].strip()
            config_str = line[1:line.find('tps') - 3]
            if tps_value_str.lower() == "nan":
                return None
            return float(tps_value_str), config_str
        except Exception:
            return None
    
    
    # Read offline samples and compute default and best TPS statistics.
    with open(args['tuning_config']['offline_sample'], 'r') as f:
        lines = f.readlines()
        # Safely parse the first line for default TPS
        first_tps = get_tps(lines[0]) if lines else None
        default_tps_value = first_tps[0] if first_tps else 0.0
        all_default: List[float] = []
        best_tps: float = default_tps_value
        best_config: str = ''
        # Consider first 5 lines as default repetitions if available
        for i in range(min(5, len(lines))):
            parsed = get_tps(lines[i])
            if parsed is not None:
                tps_val, _ = parsed
                all_default.append(tps_val)
        # Scan entire file for best TPS
        for line in lines:
            parsed = get_tps(line)
            if parsed is None:
                continue
            tps_val, config_str = parsed
            if best_tps < tps_val:
                best_tps = tps_val
                best_config = config_str
    
    if args['benchmark_config']['tool'] != 'surrogate':
        if all_default:
            delta = best_tps - max(all_default)
            print(all_default, delta)
            # Load inner metrics safely
            with open(f'record/inner_metrics{host}.json', 'r') as rf:
                inner = json.load(rf)['inner']
            with open('record/offine_record.jsonl', 'a') as w:
                strs = json.dumps({
                    'workload': workload,
                    'inner': inner,
                    'default_tps': [float(i) for i in all_default],
                    'best_tps': best_tps,
                    'best_config': best_config,
                    'undulation': max(all_default) - min(all_default),
                    'delta': delta
                })
                w.write(strs + '\n')
        else:
            # Fallback when defaults couldn't be parsed
            with open('record/offine_record.jsonl', 'a') as w:
                strs = json.dumps({
                    'workload': workload,
                    'default_tps': default_tps_value,
                    'best_tps': best_tps,
                    'best_config': best_config
                })
                w.write(strs + '\n')
    else:
        try: 
            best_config = '{' + best_config + '}'
            print(best_config)
            best_config = json.loads(best_config.strip())
            test_surrogate_result(workload, args=args, config=best_config)
        except:
            with open('record/offine_record.jsonl', 'a') as w:
                strs = json.dumps({
                    'workload': workload,
                    'default_tps': default_tps_value,
                    'best_tps': best_tps,
                    'best_config': best_config
                })
                w.write(strs + '\n')
        