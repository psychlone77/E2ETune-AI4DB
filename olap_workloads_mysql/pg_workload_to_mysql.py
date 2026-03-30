"""
Convert PostgreSQL workload query files to MySQL-compatible SQL.

Default source:
  E2ETune-AI4DB/olap_workloads

Default target:
  E2ETune-AI4DB/olap_workloads_mysql

Usage:
  python olap_workloads_mysql/pg_workload_to_mysql.py
  python olap_workloads_mysql/pg_workload_to_mysql.py --overwrite
  python olap_workloads_mysql/pg_workload_to_mysql.py --source ./olap_workloads --target ./olap_workloads_mysql
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


PG_CAST_TO_MYSQL = {
	"int": "SIGNED",
	"int2": "SIGNED",
	"int4": "SIGNED",
	"int8": "SIGNED",
	"integer": "SIGNED",
	"smallint": "SIGNED",
	"bigint": "SIGNED",
	"serial": "SIGNED",
	"bigserial": "SIGNED",
	"float": "DECIMAL(20,6)",
	"float4": "DECIMAL(20,6)",
	"float8": "DECIMAL(20,6)",
	"real": "DECIMAL(20,6)",
	"double": "DECIMAL(20,6)",
	"numeric": "DECIMAL(20,6)",
	"decimal": "DECIMAL(20,6)",
	"bool": "UNSIGNED",
	"boolean": "UNSIGNED",
	"text": "CHAR",
	"varchar": "CHAR",
	"char": "CHAR",
	"date": "DATE",
	"timestamp": "DATETIME",
	"timestamptz": "DATETIME",
}


def _normalize_cast_type(pg_type: str) -> str:
	t = pg_type.strip().lower()
	t = re.sub(r"\s+", " ", t)
	if t == "double precision":
		return "double"
	return t


def _replace_pg_casts(sql: str) -> str:
	"""
	Replace common PostgreSQL inline casts (expr::type) with MySQL CAST(expr AS type).

	This intentionally targets the workload patterns used in this repository,
	where casts are mostly in the form `alias.column::numeric`.
	"""

	# Handles identifiers, dotted identifiers, quoted identifiers, and simple parenthesized expressions.
	cast_pattern = re.compile(
		r"(?P<expr>(?:\([^()]+\)|[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*))\s*::\s*(?P<type>[A-Za-z][A-Za-z0-9_]*(?:\s+[A-Za-z][A-Za-z0-9_]*)?)",
		flags=re.IGNORECASE,
	)

	def repl(match: re.Match) -> str:
		expr = match.group("expr")
		pg_type = _normalize_cast_type(match.group("type"))
		mysql_type = PG_CAST_TO_MYSQL.get(pg_type, "CHAR")
		return f"CAST({expr} AS {mysql_type})"

	return cast_pattern.sub(repl, sql)


def _replace_extract_year(sql: str) -> str:
	# EXTRACT(YEAR FROM CURRENT_DATE) -> YEAR(CURDATE())
	sql = re.sub(
		r"EXTRACT\s*\(\s*YEAR\s+FROM\s+CURRENT_DATE\s*\)",
		"YEAR(CURDATE())",
		sql,
		flags=re.IGNORECASE,
	)
	# EXTRACT(YEAR FROM expr) -> YEAR(expr)
	sql = re.sub(
		r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([^\)]+)\)",
		r"YEAR(\1)",
		sql,
		flags=re.IGNORECASE,
	)
	return sql


def _replace_ilike(sql: str) -> str:
	# MySQL has no ILIKE; use case-insensitive comparison via LOWER() wrappers.
	# This is a conservative replacement that works for common `col ILIKE pattern` forms.
	pattern = re.compile(
		r"(?P<lhs>[A-Za-z_][A-Za-z0-9_\.]*?)\s+ILIKE\s+(?P<rhs>('(?:[^']|'')*'|[A-Za-z_][A-Za-z0-9_\.]*))",
		flags=re.IGNORECASE,
	)
	return pattern.sub(r"LOWER(\g<lhs>) LIKE LOWER(\g<rhs>)", sql)


def convert_pg_sql_to_mysql(sql: str) -> str:
	converted = sql
	converted = _replace_pg_casts(converted)
	converted = _replace_extract_year(converted)
	converted = _replace_ilike(converted)
	return converted


def convert_workloads(source_dir: Path, target_dir: Path, overwrite: bool = False) -> tuple[int, int, int]:
	source_dir = source_dir.resolve()
	target_dir = target_dir.resolve()

	if not source_dir.exists():
		raise FileNotFoundError(f"Source directory not found: {source_dir}")

	target_dir.mkdir(parents=True, exist_ok=True)

	# Treat .wg as workload files; include .sql for flexibility.
	workload_files = sorted(
		[*source_dir.rglob("*.wg"), *source_dir.rglob("*.sql")]
	)

	converted_count = 0
	skipped_count = 0
	failed_count = 0

	for src in workload_files:
		rel = src.relative_to(source_dir)
		dst = target_dir / rel
		dst.parent.mkdir(parents=True, exist_ok=True)

		if dst.exists() and not overwrite:
			print(f"[SKIP] Exists: {dst}")
			skipped_count += 1
			continue

		try:
			content = src.read_text(encoding="utf-8", errors="ignore")
			converted = convert_pg_sql_to_mysql(content)
			dst.write_text(converted, encoding="utf-8")
			print(f"[OK]   {src} -> {dst}")
			converted_count += 1
		except Exception as exc:
			print(f"[FAIL] {src}: {exc}")
			failed_count += 1

	return converted_count, skipped_count, failed_count


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Convert PostgreSQL workload files to MySQL syntax.")
	parser.add_argument(
		"--source",
		type=Path,
		default=Path("olap_workloads"),
		help="Source workload directory (default: olap_workloads)",
	)
	parser.add_argument(
		"--target",
		type=Path,
		default=Path("olap_workloads_mysql"),
		help="Target directory for converted workloads (default: olap_workloads_mysql)",
	)
	parser.add_argument(
		"--overwrite",
		action="store_true",
		help="Overwrite existing files in target directory.",
	)
	return parser


def main() -> int:
	args = build_arg_parser().parse_args()

	converted, skipped, failed = convert_workloads(
		source_dir=args.source,
		target_dir=args.target,
		overwrite=args.overwrite,
	)

	print("\n=== Conversion Summary ===")
	print(f"Converted: {converted}")
	print(f"Skipped  : {skipped}")
	print(f"Failed   : {failed}")

	return 1 if failed > 0 else 0


if __name__ == "__main__":
	raise SystemExit(main())

