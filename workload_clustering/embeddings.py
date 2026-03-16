import os
import glob
import time
import re
from pathlib import Path
import numpy as np
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import ResourceExhausted, GoogleAPIError
import sqlglot

def load_env_api_key():
    """Load API key from a .env file and return it.

    Looks for a .env file in the project root (two levels up from this file)
    and in the current directory. Expects a line like:

        GEMINI_API_KEY=your_key_here
    or
        GOOGLE_API_KEY=your_key_here
    """
    # Candidate .env locations
    this_file = Path(__file__).resolve()
    candidates = [
        this_file.parent.parent / ".env",  # repo root: E2ETune-AI4DB/.env
        this_file.parent / ".env",          # same directory as this script
    ]

    for env_path in candidates:
        if env_path.exists():
            with env_path.open() as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    # Only set if not already in the environment
                    os.environ.setdefault(key, value)

            break

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "No API key found. Please add GEMINI_API_KEY or GOOGLE_API_KEY to your .env file."
        )
    return api_key


# Initialize client using API key from .env
genai.configure(api_key=load_env_api_key())

def normalize_sql(sql_text):
    """Normalize SQL so embeddings depend on structure, not schema.

    - Removes string and numeric literals.
    - Anonymizes table and column names (and other identifiers) using
      per-query stable placeholders like T1, T2, C1, C2, etc.
    - Falls back to a simple regex-based cleanup if parsing fails.
    """

    # First, strip literals so they don't affect identifiers parsing much.
    text = re.sub(r"'.*?'", '?', sql_text)
    text = re.sub(r'\b\d+\b', '?', text)

    # try:
    #     # Parse possibly multi-statement SQL.
    #     expressions = sqlglot.parse(text)

    #     table_map = {}
    #     column_map = {}
    #     other_ident_map = {}

    #     table_counter = 0
    #     column_counter = 0
    #     ident_counter = 0

    #     def _normalize_identifier(name, kind):
    #         nonlocal table_counter, column_counter, ident_counter
    #         if kind == "table":
    #             if name not in table_map:
    #                 table_counter += 1
    #                 table_map[name] = f"T{table_counter}"
    #             return table_map[name]
    #         if kind == "column":
    #             if name not in column_map:
    #                 column_counter += 1
    #                 column_map[name] = f"C{column_counter}"
    #             return column_map[name]
    #         # other identifiers (aliases, etc.)
    #         if name not in other_ident_map:
    #             ident_counter += 1
    #             other_ident_map[name] = f"I{ident_counter}"
    #         return other_ident_map[name]

    #     def _transform(expr):
    #         from sqlglot import exp

    #         # Tables — anonymize both the table name and any alias.
    #         # Modify in-place (no copy) so sqlglot continues traversing children
    #         # on the same node, ensuring nested Columns are also anonymized.
    #         if isinstance(expr, exp.Table):
    #             name = getattr(expr, "name", None)
    #             if not name and expr.this is not None:
    #                 inner = expr.this
    #                 name = getattr(inner, "this", None) if hasattr(inner, "this") else str(inner)
    #             if name:
    #                 anon = _normalize_identifier(str(name), "table")
    #                 expr.set("this", sqlglot.exp.to_identifier(anon))
    #                 if expr.alias:
    #                     anon_alias = _normalize_identifier(str(expr.alias), "other")
    #                     expr.set("alias", anon_alias)
    #             return expr

    #         # Columns — anonymize qualifier and column name
    #         if isinstance(expr, exp.Column):
    #             if expr.table:
    #                 anon_table = _normalize_identifier(str(expr.table), "table")
    #                 expr.set("table", sqlglot.exp.to_identifier(anon_table))
    #             if expr.name:
    #                 anon_col = _normalize_identifier(str(expr.name), "column")
    #                 expr.set("this", sqlglot.exp.to_identifier(anon_col))
    #             return expr

    #         # Aliases in SELECT (e.g. COUNT(*) AS num_cast_members)
    #         if isinstance(expr, exp.Alias):
    #             if expr.alias:
    #                 anon_alias = _normalize_identifier(str(expr.alias), "other")
    #                 expr.set("alias", anon_alias)
    #             return expr

    #         return expr

        # transformed = [e.transform(_transform) for e in expressions]
        # Use default SQL generation (no dialect= argument to avoid version incompatibilities)
        # normalized = "; ".join(e.sql() for e in transformed)
        # return normalized

    # except Exception:
        # If anything goes wrong, fall back to literal stripping only.
    return text

def process_workloads_with_resume(directory_path, save_file="workload_embeddings.npy", tracker_file="processed_files.txt"):
    # Get and sort all workload files to ensure consistent ordering
    workload_files = sorted([f for f in glob.glob(os.path.join(directory_path, "*.wg"))])

    
    # Load previous progress to avoid re-embedding files you already paid for/waited for
    processed_files = set()
    if os.path.exists(tracker_file):
        with open(tracker_file, 'r') as f:
            processed_files = set(f.read().splitlines())
            
    # Load existing vectors so we append to them instead of overwriting
    embeddings_list = []
    if os.path.exists(save_file):
        embeddings_list = np.load(save_file).tolist()

    print(f"Found {len(workload_files)} total workloads.")
    print(f"Resuming... {len(processed_files)} already processed securely on disk.")

    for file_path in workload_files:
        filename = os.path.basename(file_path)
        
        # Skip if we already embedded this file (e.g., ssb_1 to ssb_37)
        if filename in processed_files:
            continue
            
        with open(file_path, 'r') as file:
            raw_sql = file.read()
            
        clean_sql = normalize_sql(raw_sql)
        print(f"Processing {filename} with normalized SQL:\n{clean_sql}\n")
    
        prompt_text = f"Analyze the structural complexity of this database workload: {clean_sql}"

        # Retry loop for handling 429 Quota Exceeded errors gracefully
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Call the new unified model
                response = genai.embed_content(
                    model='models/gemini-embedding-001',
                    content=prompt_text,
                    task_type="RETRIEVAL_DOCUMENT",
                    # Force 768 to match the dimension shape of your first 37 files
                    output_dimensionality=768
                )
                
                # Save successful embedding vector
                embeddings_list.append(response['embedding'])
                
                # Log progress to disk IMMEDIATELY
                with open(tracker_file, 'a') as f:
                    f.write(filename + '\n')
                np.save(save_file, np.array(embeddings_list))
                
                # Dynamic pacing: Wait slightly to avoid hitting the 1000 RPM limit
                time.sleep(1) 
                
                break # Success! Break out of the retry loop and move to the next file

            # except GoogleAPIError as e:
            #     # Catch 429 Quota Exceeded error
            #     if e.code == 429:
            #         wait_time = 30 * (attempt + 1) # Wait 30s, then 60s, then 90s
            #         print(f"\nRate limited on {filename}. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
            #         time.sleep(wait_time)
            #         continue # Try this file again
            #     else:
            #         # For other API errors (like a broken key), exit completely so you can fix it
            #         print(f"\nCRITICAL API ERROR on {filename}: {e.message}")
            #         break 
            
            except ResourceExhausted as e:
                # Catch 429 Quota Exceeded error correctly
                wait_time = 30 * (attempt + 1) # Wait 30s, 60s, 90s
                print(f"\nRate limited on {filename}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue # Try this file again
                
            except Exception as e:
                print(f"\nWarning: Unexpected error on {filename} - {e}. Skipping.")
                break

        # Update in-memory tracker to keep log outputs accurate
        processed_files.add(filename)

        if len(processed_files) % 50 == 0:
            print(f"Processed {len(processed_files)}/{len(workload_files)} workloads...")

    print(f"\nRun complete. {len(embeddings_list)} embeddings securely saved.")
    return np.array(embeddings_list)

# Run the pipeline
embeddings, filenames = process_workloads_with_resume("/Users/nisith/Desktop/FYP/E2ETune-AI4DB/olap_workloads/")