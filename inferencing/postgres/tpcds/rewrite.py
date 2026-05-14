import re
from pathlib import Path

file_path = Path("/home/E2ETune-AI4DB/inferencing/postgres/tpcds/tpcds.sql")
with open(file_path, "r") as f:
    sql = f.read()

def replace_top(match):
    return f"select {match.group(2)}"

queries = sql.split(';')

new_queries = []
for q in queries:
    top_match = re.search(r'(?i)select\s+top\s+(\d+)\s+', q)
    if top_match:
        limit_val = top_match.group(1)
        q = re.sub(r'(?i)(select)\s+top\s+(\d+)\s+', r'\1 ', q)
        if q.strip():
            q = f"{q.rstrip()}\nlimit {limit_val}"
    
    # Replace + number days
    q = re.sub(r'\+\s*(\d+)\s+days', r"+ interval '\1 days'", q)
    
    # Replace - number days
    q = re.sub(r'-\s*(\d+)\s+days', r"- interval '\1 days'", q)
    
    # Fix lochierarchy by substituting it in the ORDER BY clause
    # It fails because in Postgres you can't reuse a selection alias in the ORDER BY that is inside an expression.
    # Grouping returns an integer. So `grouping(i_category)+grouping(i_class)` is used instead.
    q = q.replace("case when lochierarchy = 0 then i_category end,", 
                  "case when grouping(i_category)+grouping(i_class) = 0 then i_category end,")
    
    new_queries.append(q)

new_sql = ';'.join(new_queries)

with open(file_path, "w") as f:
    f.write(new_sql)
