#!/usr/bin/env python3
"""Append sentence-transformer embeddings for query plans into a CSV file.

This script reads a CSV containing a `collected.query_plans` column, converts each
row's query plan list into a compact text representation, embeds it with a tiny
SentenceTransformer model, and writes embeddings either as:
1) multiple numeric columns (`qp_emb_000`, `qp_emb_001`, ...), or
2) one JSON vector column (for compact storage in one cell).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
from typing import List

import numpy as np
import pandas as pd

try:
    from sentence_transformers import SentenceTransformer
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: sentence-transformers. "
        "Install with: pip install sentence-transformers"
    ) from exc


def parse_plan_cell(value: object) -> List[str]:
    """Parse a query plan cell into a list of strings."""
    if value is None:
        return []
    if isinstance(value, float) and np.isnan(value):
        return []
    if isinstance(value, list):
        return [str(x) for x in value]

    text = str(value).strip()
    if not text:
        return []

    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except Exception:
            continue

    # Fallback: treat raw string as a single plan
    return [text]


def plans_to_embedding_text(plans: List[str], max_chars: int) -> str:
    """Create a bounded text representation for embedding."""
    if not plans:
        return ""
    joined = " [SEP] ".join(p.strip() for p in plans if p and str(p).strip())
    if len(joined) > max_chars:
        return joined[:max_chars]
    return joined


def resolve_device(device_arg: str) -> str:
    if device_arg != "auto":
        return device_arg

    try:
        import torch

        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return "mps"
        if torch.cuda.is_available():
            return "cuda"
    except Exception:
        pass
    return "cpu"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add query plan embedding columns to a CSV file."
    )
    parser.add_argument(
        "--input-csv",
        default="surrogate/cost_model_collected.csv",
        help="Path to input CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Path to output CSV. Defaults to in-place update of --input-csv.",
    )
    parser.add_argument(
        "--query-plan-col",
        default="collected.query_plans",
        help="Name of the query plan column.",
    )
    parser.add_argument(
        "--model-name",
        default="sentence-transformers/paraphrase-MiniLM-L3-v2",
        help="SentenceTransformer model name.",
    )
    parser.add_argument(
        "--device",
        default="auto",
        choices=["auto", "cpu", "cuda", "mps"],
        help="Inference device.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help="Batch size for embedding generation.",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=4000,
        help="Max characters per row passed to the encoder.",
    )
    parser.add_argument(
        "--prefix",
        default="qp_emb",
        help="Prefix for embedding columns.",
    )
    parser.add_argument(
        "--format",
        default="multi",
        choices=["multi", "single"],
        help="Embedding output format: 'multi' columns or one 'single' JSON vector column.",
    )
    parser.add_argument(
        "--single-col",
        default="qp_emb_vector",
        help="Column name used when --format single.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="If in-place update, write a .bak copy first.",
    )
    args = parser.parse_args()

    input_csv = args.input_csv
    output_csv = args.output_csv or input_csv

    if not os.path.exists(input_csv):
        raise SystemExit(f"Input CSV not found: {input_csv}")

    df = pd.read_csv(input_csv)
    if args.query_plan_col not in df.columns:
        raise SystemExit(
            f"Column '{args.query_plan_col}' not found in CSV. "
            f"Available columns: {len(df.columns)}"
        )

    print(f"Loaded {len(df)} rows from {input_csv}")
    print(f"Using model: {args.model_name}")

    plan_lists = [parse_plan_cell(v) for v in df[args.query_plan_col].tolist()]
    embed_texts = [plans_to_embedding_text(plans, args.max_chars) for plans in plan_lists]

    device = resolve_device(args.device)
    print(f"Encoding on device: {device}")

    model = SentenceTransformer(args.model_name, device=device)
    vectors = model.encode(
        embed_texts,
        batch_size=args.batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    ).astype(np.float32)

    dim = vectors.shape[1]
    print(f"Generated embeddings: shape={vectors.shape}")

    # Replace previously generated embedding columns (same prefix) to avoid duplicates.
    existing_prefix_cols = [c for c in df.columns if c.startswith(f"{args.prefix}_")]
    if existing_prefix_cols:
        df = df.drop(columns=existing_prefix_cols)

    if args.single_col in df.columns:
        df = df.drop(columns=[args.single_col])

    if args.format == "multi":
        col_names = [f"{args.prefix}_{i:03d}" for i in range(dim)]
        emb_df = pd.DataFrame(vectors, columns=col_names, index=df.index)
        out_df = pd.concat([df, emb_df], axis=1)
    else:
        vector_strings = [
            json.dumps(vec.astype(float).tolist(), separators=(",", ":"))
            for vec in vectors
        ]
        out_df = df.copy()
        out_df[args.single_col] = vector_strings

    if args.backup and output_csv == input_csv:
        backup_path = f"{input_csv}.bak"
        pd.read_csv(input_csv).to_csv(backup_path, index=False)
        print(f"Backup written: {backup_path}")

    out_df.to_csv(output_csv, index=False)
    print(f"Wrote CSV with embeddings to: {output_csv}")
    if args.format == "multi":
        print(f"Added {dim} embedding columns with prefix '{args.prefix}_'")
    else:
        print(f"Added single embedding column '{args.single_col}' with vector length {dim}")


if __name__ == "__main__":
    main()
