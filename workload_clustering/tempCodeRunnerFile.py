nv_api_key())

def normalize_sql(sql_text):
    """Normalize SQL so embeddings depend on structure, not schema.

    - Removes string and numeric literals.
    - Anonymizes table and column names (and other identifiers) using
      per-query stable placeholders li