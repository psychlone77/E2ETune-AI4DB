# #!/usr/bin/env python3
# """Append sentence-transformer embeddings for query plans into a CSV file.

# This script reads a CSV containing a `collected.query_plans` column, converts each
# row's query plan list into a compact text representation, embeds it with a tiny
# SentenceTransformer model, and writes embeddings either as:
# 1) multiple numeric columns (`qp_emb_000`, `qp_emb_001`, ...), or
# 2) one JSON vector column (for compact storage in one cell).
# """

# from __future__ import annotations

# import argparse
# import ast
# import json
# import os
# from typing import List, Sequence

# import numpy as np
# import pandas as pd

# try:
#     from sentence_transformers import SentenceTransformer
# except ImportError as exc:  # pragma: no cover
#     raise SystemExit(
#         "Missing dependency: sentence-transformers. "
#         "Install with: pip install sentence-transformers"
#     ) from exc


# def parse_plan_cell(value: object) -> List[str]:
#     """Parse a query plan cell into a list of strings."""
#     if value is None:
#         return []
#     if isinstance(value, float) and np.isnan(value):
#         return []
#     if isinstance(value, list):
#         return [str(x) for x in value]

#     text = str(value).strip()
#     if not text:
#         return []

#     for parser in (json.loads, ast.literal_eval):
#         try:
#             parsed = parser(text)
#             if isinstance(parsed, list):
#                 return [str(x) for x in parsed]
#         except Exception:
#             continue

#     # Fallback: treat raw string as a single plan
#     return [text]


# def parse_vector_cell(value: object) -> List[float] | None:
#     """Parse one embedding-vector cell into a list of floats or None."""
#     if value is None:
#         return None
#     if isinstance(value, float) and np.isnan(value):
#         return None

#     if isinstance(value, (list, tuple, np.ndarray)):
#         try:
#             parsed = [float(x) for x in value]
#             if not parsed:
#                 return None
#             if not np.isfinite(np.asarray(parsed, dtype=np.float32)).all():
#                 return None
#             return parsed
#         except Exception:
#             return None

#     text = str(value).strip()
#     if not text:
#         return None

#     for parser in (json.loads, ast.literal_eval):
#         try:
#             parsed = parser(text)
#             if isinstance(parsed, (list, tuple)):
#                 as_float = [float(x) for x in parsed]
#                 if not as_float:
#                     return None
#                 if not np.isfinite(np.asarray(as_float, dtype=np.float32)).all():
#                     return None
#                 return as_float
#         except Exception:
#             continue
#     return None


# def normalize_vector_text(vec: Sequence[float]) -> str:
#     """Serialize vector to compact JSON string."""
#     return json.dumps([float(x) for x in vec], separators=(",", ":"))


# def is_missing_single_embedding(value: object) -> bool:
#     """Return True if a single-column embedding cell is empty/invalid."""
#     return parse_vector_cell(value) is None


# def is_missing_multi_embedding(row: pd.Series, cols: List[str]) -> bool:
#     """Return True if row has any missing/invalid multi-column embedding values."""
#     if not cols:
#         return True
#     try:
#         vals = row[cols].astype(float).to_numpy(dtype=np.float32)
#     except Exception:
#         return True
#     return not np.isfinite(vals).all()


# def plans_to_embedding_text(plans: List[str], max_chars: int) -> str:
#     """Create a bounded text representation for embedding."""
#     if not plans:
#         return ""
#     joined = " [SEP] ".join(p.strip() for p in plans if p and str(p).strip())
#     if len(joined) > max_chars:
#         return joined[:max_chars]
#     return joined


# def resolve_device(device_arg: str) -> str:
#     if device_arg != "auto":
#         return device_arg

#     try:
#         import torch

#         if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
#             return "mps"
#         if torch.cuda.is_available():
#             return "cuda"
#     except Exception:
#         pass
#     return "cpu"


# def main() -> None:
#     parser = argparse.ArgumentParser(
#         description="Add query plan embedding columns to a CSV file."
#     )
#     parser.add_argument(
#         "--input-csv",
#         default="surrogate/cost_model_collected.csv",
#         help="Path to input CSV.",
#     )
#     parser.add_argument(
#         "--output-csv",
#         default=None,
#         help="Path to output CSV. Defaults to in-place update of --input-csv.",
#     )
#     parser.add_argument(
#         "--query-plan-col",
#         default="collected.query_plans",
#         help="Name of the query plan column.",
#     )
#     parser.add_argument(
#         "--model-name",
#         default="sentence-transformers/paraphrase-MiniLM-L3-v2",
#         help="SentenceTransformer model name.",
#     )
#     parser.add_argument(
#         "--device",
#         default="auto",
#         choices=["auto", "cpu", "cuda", "mps"],
#         help="Inference device.",
#     )
#     parser.add_argument(
#         "--batch-size",
#         type=int,
#         default=128,
#         help="Batch size for embedding generation.",
#     )
#     parser.add_argument(
#         "--max-chars",
#         type=int,
#         default=4000,
#         help="Max characters per row passed to the encoder.",
#     )
#     parser.add_argument(
#         "--prefix",
#         default="qp_emb",
#         help="Prefix for embedding columns.",
#     )
#     parser.add_argument(
#         "--format",
#         default="multi",
#         choices=["multi", "single"],
#         help="Embedding output format: 'multi' columns or one 'single' JSON vector column.",
#     )
#     parser.add_argument(
#         "--single-col",
#         default="qp_emb_vector",
#         help="Column name used when --format single.",
#     )
#     parser.add_argument(
#         "--backup",
#         action="store_true",
#         help="If in-place update, write a .bak copy first.",
#     )
#     parser.add_argument(
#         "--rewrite-all",
#         action="store_true",
#         help="Recompute all embeddings. Default behavior backfills missing/invalid rows only.",
#     )
#     args = parser.parse_args()

#     input_csv = args.input_csv
#     output_csv = args.output_csv or input_csv

#     if not os.path.exists(input_csv):
#         raise SystemExit(f"Input CSV not found: {input_csv}")

#     df = pd.read_csv(input_csv)
#     if args.query_plan_col not in df.columns:
#         raise SystemExit(
#             f"Column '{args.query_plan_col}' not found in CSV. "
#             f"Available columns: {len(df.columns)}"
#         )

#     print(f"Loaded {len(df)} rows from {input_csv}")
#     print(f"Using model: {args.model_name}")

#     if args.format == "single":
#         if args.single_col not in df.columns:
#             df[args.single_col] = np.nan
#         if args.rewrite_all:
#             target_mask = np.ones(len(df), dtype=bool)
#         else:
#             target_mask = df[args.single_col].apply(is_missing_single_embedding).to_numpy()
#     else:
#         existing_multi_cols = sorted(c for c in df.columns if c.startswith(f"{args.prefix}_"))
#         if args.rewrite_all:
#             target_mask = np.ones(len(df), dtype=bool)
#         else:
#             if existing_multi_cols:
#                 target_mask = (
#                     df[existing_multi_cols]
#                     .apply(lambda r: is_missing_multi_embedding(r, existing_multi_cols), axis=1)
#                     .to_numpy()
#                 )
#             else:
#                 target_mask = np.ones(len(df), dtype=bool)

#     target_idx = np.flatnonzero(target_mask)
#     print(f"Rows needing embeddings: {len(target_idx)} / {len(df)}")

#     if len(target_idx) == 0:
#         out_df = df
#         print("No missing embeddings found; nothing to encode.")
#     else:
#         plan_lists = [parse_plan_cell(v) for v in df.loc[target_idx, args.query_plan_col].tolist()]
#         embed_texts = [plans_to_embedding_text(plans, args.max_chars) for plans in plan_lists]

#         device = resolve_device(args.device)
#         print(f"Encoding on device: {device}")

#         model = SentenceTransformer(args.model_name, device=device)
#         vectors = model.encode(
#             embed_texts,
#             batch_size=args.batch_size,
#             show_progress_bar=True,
#             convert_to_numpy=True,
#             normalize_embeddings=True,
#         ).astype(np.float32)

#         dim = vectors.shape[1]
#         print(f"Generated embeddings for missing rows: shape={vectors.shape}")

#         out_df = df.copy()
#         if args.format == "single":
#             vector_strings = [normalize_vector_text(vec.tolist()) for vec in vectors]
#             out_df.loc[target_idx, args.single_col] = vector_strings
#         else:
#             existing_multi_cols = sorted(c for c in out_df.columns if c.startswith(f"{args.prefix}_"))
#             if existing_multi_cols and len(existing_multi_cols) != dim:
#                 raise SystemExit(
#                     "Existing multi-column embedding width does not match model output "
#                     f"({len(existing_multi_cols)} != {dim}). "
#                     "Use --rewrite-all after removing old columns, or switch prefix/model."
#                 )
#             if not existing_multi_cols:
#                 existing_multi_cols = [f"{args.prefix}_{i:03d}" for i in range(dim)]
#                 for col in existing_multi_cols:
#                     out_df[col] = np.nan
#             out_df.loc[target_idx, existing_multi_cols] = vectors

#     if args.backup and output_csv == input_csv:
#         backup_path = f"{input_csv}.bak"
#         pd.read_csv(input_csv).to_csv(backup_path, index=False)
#         print(f"Backup written: {backup_path}")

#     out_df.to_csv(output_csv, index=False)
#     print(f"Wrote CSV with embeddings to: {output_csv}")
#     if args.format == "multi":
#         final_cols = [c for c in out_df.columns if c.startswith(f"{args.prefix}_")]
#         print(f"Embedding columns with prefix '{args.prefix}_': {len(final_cols)}")
#     else:
#         filled = (~out_df[args.single_col].apply(is_missing_single_embedding)).sum()
#         print(
#             f"Single embedding column '{args.single_col}' has {filled} valid rows out of {len(out_df)}"
#         )


# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
"""
Add SentenceTransformer embeddings for query plans into default_data.csv.

- Reads a CSV containing a query plan column (default: query_plans).
- Parses each row's list-of-plans cell into text, embeds it with a SentenceTransformer model,
  and writes embeddings either as:
    1) multiple numeric columns (qp_emb_000, qp_emb_001, ...)
    2) one JSON vector column (default: qp_emb_vector)

Default input is surrogate/default_data.csv.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
from typing import List, Sequence

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

    return [text]


def parse_vector_cell(value: object) -> List[float] | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None

    if isinstance(value, (list, tuple, np.ndarray)):
        try:
            parsed = [float(x) for x in value]
            if not parsed:
                return None
            if not np.isfinite(np.asarray(parsed, dtype=np.float32)).all():
                return None
            return parsed
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None

    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
            if isinstance(parsed, (list, tuple)):
                as_float = [float(x) for x in parsed]
                if not as_float:
                    return None
                if not np.isfinite(np.asarray(as_float, dtype=np.float32)).all():
                    return None
                return as_float
        except Exception:
            continue
    return None


def normalize_vector_text(vec: Sequence[float]) -> str:
    return json.dumps([float(x) for x in vec], separators=(",", ":"))


def is_missing_single_embedding(value: object) -> bool:
    return parse_vector_cell(value) is None


def is_missing_multi_embedding(row: pd.Series, cols: List[str]) -> bool:
    if not cols:
        return True
    try:
        vals = row[cols].astype(float).to_numpy(dtype=np.float32)
    except Exception:
        return True
    return not np.isfinite(vals).all()


def plans_to_embedding_text(plans: List[str], max_chars: int) -> str:
    if not plans:
        return ""
    joined = " [SEP] ".join(p.strip() for p in plans if p and str(p).strip())
    return joined[:max_chars] if len(joined) > max_chars else joined


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
    parser = argparse.ArgumentParser(description="Add query plan embeddings to default_data.csv.")
    parser.add_argument(
        "--input-csv",
        default="/home/E2ETune-AI4DB/surrogate/default_data_new.csv",
        help="Path to input CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Path to output CSV. Defaults to in-place update of --input-csv.",
    )
    parser.add_argument(
        "--query-plan-col",
        default="query_plans",
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
        help="Prefix for embedding columns when using --format multi.",
    )
    parser.add_argument(
        "--format",
        default="single",
        choices=["multi", "single"],
        help="Embedding output format: multi columns or one single JSON vector column.",
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
    parser.add_argument(
        "--rewrite-all",
        action="store_true",
        help="Recompute all embeddings. Default backfills missing/invalid rows only.",
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
            f"Available columns: {list(df.columns)}"
        )

    print(f"Loaded {len(df)} rows from {input_csv}")
    print(f"Using model: {args.model_name}")

    if args.format == "single":
        if args.single_col not in df.columns:
            df[args.single_col] = np.nan
        if args.rewrite_all:
            target_mask = np.ones(len(df), dtype=bool)
        else:
            target_mask = df[args.single_col].apply(is_missing_single_embedding).to_numpy()
    else:
        existing_multi_cols = sorted(c for c in df.columns if c.startswith(f"{args.prefix}_"))
        if args.rewrite_all:
            target_mask = np.ones(len(df), dtype=bool)
        else:
            if existing_multi_cols:
                target_mask = (
                    df[existing_multi_cols]
                    .apply(lambda r: is_missing_multi_embedding(r, existing_multi_cols), axis=1)
                    .to_numpy()
                )
            else:
                target_mask = np.ones(len(df), dtype=bool)

    target_idx = np.flatnonzero(target_mask)
    print(f"Rows needing embeddings: {len(target_idx)} / {len(df)}")

    if len(target_idx) == 0:
        out_df = df
        print("No missing embeddings found; nothing to encode.")
    else:
        plan_lists = [parse_plan_cell(v) for v in df.loc[target_idx, args.query_plan_col].tolist()]
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

        out_df = df.copy()
        if args.format == "single":
            vector_strings = [normalize_vector_text(vec.tolist()) for vec in vectors]
            out_df.loc[target_idx, args.single_col] = vector_strings
        else:
            existing_multi_cols = sorted(c for c in out_df.columns if c.startswith(f"{args.prefix}_"))
            if existing_multi_cols and len(existing_multi_cols) != dim:
                raise SystemExit(
                    "Existing multi-column embedding width does not match model output "
                    f"({len(existing_multi_cols)} != {dim}). "
                    "Use --rewrite-all after removing old columns, or switch prefix/model."
                )
            if not existing_multi_cols:
                existing_multi_cols = [f"{args.prefix}_{i:03d}" for i in range(dim)]
                for col in existing_multi_cols:
                    out_df[col] = np.nan
            out_df.loc[target_idx, existing_multi_cols] = vectors

    if args.backup and output_csv == input_csv:
        backup_path = f"{input_csv}.bak"
        pd.read_csv(input_csv).to_csv(backup_path, index=False)
        print(f"Backup written: {backup_path}")

    out_df.to_csv(output_csv, index=False)
    print(f"Wrote CSV with embeddings to: {output_csv}")


if __name__ == "__main__":
    main()