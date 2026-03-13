from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Dict

from classes.base_classes.Workload_Feature_Extractor import WorkloadFeatureExtractor
from classes.oltp_metadata import BENCHMARK_METADATA


class WorkloadFeatureExtractorOLTP(WorkloadFeatureExtractor):
    """
    Extracts macro-level workload features from a BenchBase OLTP XML config.

    The benchmark name must be supplied explicitly at construction time so
    that the correct transaction metadata is used without any guesswork.

    Parameters
    ----------
    benchmark : str
        Benchmark key that matches a top-level entry in BENCHMARK_METADATA
        (e.g. "tpcc", "smallbank").

    Usage
    -----
    extractor = WorkloadFeatureExtractorOLTP("tpcc")
    features  = extractor.extract("/path/to/config.xml")
    """

    def __init__(self, benchmark: str) -> None:
        self.benchmark = benchmark.lower()

    def extract(self, config_path: str | Path) -> Dict:
        """
        Parse a BenchBase XML config and return a workload feature dictionary.

        Parameters
        ----------
        config_path : str or Path
            Path to the BenchBase XML configuration file.

        Returns
        -------
        dict with keys:
            total_statements, table_access_frequency, read_count,
            write_count, read_write_ratio, avg_predicates_per_query,
            operator_proportions
        """
        cfg = self._parse_xml_config(config_path)
        weights = cfg["weights"]
        tx_names = cfg["tx_names"]

        meta = BENCHMARK_METADATA.get(self.benchmark, {})

        table_freq: Dict[str, float] = defaultdict(float)
        read_count: float = 0.0
        write_count: float = 0.0
        total_preds: float = 0.0
        total_stmts: float = 0.0
        op_counts: Dict[str, float] = defaultdict(float)

        for tx_name, weight in zip(tx_names, weights):
            tx_info = meta.get(
                tx_name, {"tables": [], "read": None, "predicates": 0, "operators": {}}
            )

            n_ops = sum(tx_info["operators"].values()) if tx_info["operators"] else 1
            total_stmts += weight * n_ops

            for table in tx_info["tables"]:
                table_freq[table] += weight

            if tx_info["read"] is True:
                read_count += weight
            elif tx_info["read"] is False:
                write_count += weight

            total_preds += weight * tx_info["predicates"]

            for op, count in tx_info["operators"].items():
                op_counts[op] += weight * count

        total = sum(weights)
        avg_preds = (total_preds / total) if total else 0.0
        rw_ratio = (read_count / write_count) if write_count > 0 else None

        total_op_weight = sum(op_counts.values())
        op_props = {
            op: (cnt / total_op_weight) if total_op_weight else 0.0
            for op, cnt in sorted(op_counts.items())
        }

        return {
            "total_statements": total_stmts,
            "table_access_frequency": dict(table_freq),
            "read_count": read_count,
            "write_count": write_count,
            "read_write_ratio": rw_ratio,
            "avg_predicates_per_query": avg_preds,
            "operator_proportions": op_props,
        }

    @staticmethod
    def _parse_xml_config(config_path: str | Path) -> dict:
        """Parse a BenchBase XML config and return transaction weights and names."""
        tree = ET.parse(config_path)
        root = tree.getroot()

        tx_names: list[str] = [
            name_el.text.strip()
            for tt in root.findall(".//transactiontype")
            if (name_el := tt.find("name")) is not None and name_el.text
        ]

        weights_text = ""
        for work in root.findall(".//work"):
            w_el = work.find("weights")
            if w_el is not None and w_el.text:
                weights_text = w_el.text.strip()
                break

        raw_weights = [
            float(w.strip()) for w in re.split(r"[,\s]+", weights_text) if w.strip()
        ]
        total_w = sum(raw_weights)
        weights = [w / total_w * 100 for w in raw_weights] if total_w else raw_weights

        return {"weights": weights, "tx_names": tx_names}
