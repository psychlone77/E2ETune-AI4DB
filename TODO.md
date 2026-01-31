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