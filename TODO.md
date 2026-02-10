Create SSB Flat Dataset
```sql
CREATE TABLE lineorder_flat AS
SELECT 
    l.*,
    c.c_name, c.c_address, c.c_city, c.c_nation, c.c_region, c.c_phone, c.c_mktsegment,
    s.s_name, s.s_address, s.s_city, s.s_nation, s.s_region, s.s_phone,
    p.p_name, p.p_mfgr, p.p_category, p.p_brand, p.p_color, p.p_type, p.p_size, p.p_container,
    d.d_date, d.d_dayofweek, d.d_month, d.d_year, d.d_yearmonthnum, d.d_yearmonth,
    d.d_daynuminweek, d.d_daynuminmonth, d.d_daynuminyear, d.d_monthnuminyear, d.d_weeknuminyear,
    d.d_sellingseason, d.d_lastdayinweekfl, d.d_lastdayinmonthfl, d.d_holidayfl, d.d_weekdayfl
FROM lineorder l
JOIN customer c ON l.lo_custkey = c.c_custkey
JOIN supplier s ON l.lo_suppkey = s.s_suppkey
JOIN part p ON l.lo_partkey = p.p_partkey
JOIN dates d ON l.lo_orderdate = d.d_datekey;
```

- When running inference on CPU, consider using `torch.float16` to reduce memory usage and potentially improve performance. Also add swap space if memory is limited on the machine to avoid out-of-memory errors.

- Fine Tuning LM requires around 34GB of GPU memory according to our tests. We fine tuned the Lm on A NVIDIA RTX A6000 with 48GB of GPU memory, and 38.4 TFLOPS of FP16 performance.

- Fix surrogate Model Prediction Bug in stress testing tool.

- Change Workload statistics generation to use: 
    - Table access frequency: How often each table is accessed.
    - Total number of SQL statements.
    - Read-write ratio.
    - Average number of predicates per SQL query.
    - Proportion of key operators, specifically ORDER BY, GROUP BY, and aggregation functions.