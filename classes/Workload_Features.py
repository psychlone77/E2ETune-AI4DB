from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict

import sqlglot
import sqlglot.expressions as exp

class WorkloadFeatureExtractor:
    """
    Extracts macro-level static features from a SQL workload file.

    Uses sqlglot to parse SQL into an AST, giving exact results for:
      - table references (immune to EXTRACT(x FROM y) false-positives)
      - predicate counts (walks And/Or tree nodes rather than text-splitting)
      - operator detection (typed AST nodes instead of regex on raw text)

    Usage
    -----
    extractor = WorkloadFeatureExtractor()
    features  = extractor.extract("/path/to/workload.sql")
    """

    _READ_TYPES  = (exp.Select,)
    _WRITE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)

    _AGG_MAP = {
        "count_agg": exp.Count,
        "sum_agg":   exp.Sum,
        "avg_agg":   exp.Avg,
        "min_agg":   exp.Min,
        "max_agg":   exp.Max,
    }


    def extract(self, sql_file_path: str | Path) -> Dict:
        """
        Parse *sql_file_path* and return a feature dictionary.

        Parameters
        ----------
        sql_file_path : str or Path
            Path to a plain SQL file.  Statements must be semicolon-separated.

        Returns
        -------
        dict with keys:
            total_statements, table_access_frequency, read_count,
            write_count, read_write_ratio, avg_predicates_per_query,
            operator_proportions
        """
        sql_text = Path(sql_file_path).read_text(encoding="utf-8")

        statements = [
            s for s in sqlglot.parse(sql_text, error_level=sqlglot.ErrorLevel.WARN)
            if s is not None
        ]

        total = len(statements)
        if total == 0:
            return self._empty_result()

        table_freq: Dict[str, int] = defaultdict(int)
        read_count      = 0
        write_count     = 0
        predicate_total = 0
        op_hits = {k: 0 for k in ("order_by", "group_by", *self._AGG_MAP)}

        for stmt in statements:
            # ── DML type ──────────────────────────────────────────────
            if isinstance(stmt, self._READ_TYPES):
                read_count += 1
            elif isinstance(stmt, self._WRITE_TYPES):
                write_count += 1

            # ── Table access frequency ─────────────────────────────────
            for table in stmt.find_all(exp.Table):
                name = table.name.lower()
                if name:
                    table_freq[name] += 1

            # ── Predicate count ────────────────────────────────────────
            predicate_total += self._count_predicates(stmt)

            # ── Key operators ──────────────────────────────────────────
            if stmt.find(exp.Order):
                op_hits["order_by"] += 1
            if stmt.find(exp.Group):
                op_hits["group_by"] += 1
            for key, agg_type in self._AGG_MAP.items():
                if stmt.find(agg_type):
                    op_hits[key] += 1

        rw_ratio  = (read_count / write_count) if write_count > 0 else None
        avg_preds = round(predicate_total / total, 4)
        op_props  = {k: v / total for k, v in op_hits.items()}

        return {
            "total_statements":         total,
            "table_access_frequency":   dict(table_freq),
            "read_count":               read_count,
            "write_count":              write_count,
            "read_write_ratio":         rw_ratio,
            "avg_predicates_per_query": avg_preds,
            "operator_proportions":     op_props,
        }


    def _count_predicates(self, stmt: exp.Expression) -> int:
        """
        Count atomic predicate conditions across all WHERE and HAVING clauses
        in *stmt* (including those inside sub-queries).

        Walks the And/Or connector tree so that nested boolean expressions are
        counted accurately without any text manipulation.
        """
        total = 0
        for clause in stmt.find_all(exp.Where, exp.Having):
            total += self._count_conditions(clause.this)
        return total

    def _count_conditions(self, node: exp.Expression) -> int:
        """Recursively count leaf predicates under *node*."""
        if isinstance(node, (exp.And, exp.Or)):
            return (
                self._count_conditions(node.left)
                + self._count_conditions(node.right)
            )
        return 1

    @staticmethod
    def _empty_result() -> Dict:
        return {
            "total_statements":         0,
            "table_access_frequency":   {},
            "read_count":               0,
            "write_count":              0,
            "read_write_ratio":         None,
            "avg_predicates_per_query": 0.0,
            "operator_proportions": {
                "order_by":  0.0,
                "group_by":  0.0,
                "count_agg": 0.0,
                "sum_agg":   0.0,
                "avg_agg":   0.0,
                "min_agg":   0.0,
                "max_agg":   0.0,
            },
        }
