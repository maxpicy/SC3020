-- 1. Two-table join (project spec example)
SELECT *
FROM customer C, orders O
WHERE C.c_custkey = O.o_custkey;

-- 2. Three-table join with date filter
SELECT c_name, o_orderdate, l_extendedprice
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey
  AND o_orderkey = l_orderkey
  AND o_orderdate >= DATE '1995-01-01'
  AND o_orderdate < DATE '1996-01-01';

-- 3. GROUP BY with HAVING
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       AVG(l_extendedprice) AS avg_price
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag, l_linestatus
HAVING SUM(l_quantity) > 1000
ORDER BY l_returnflag, l_linestatus;

-- 4. IN subquery
SELECT s_name, s_address
FROM supplier, nation
WHERE s_nationkey = n_nationkey
  AND n_name = 'FRANCE'
  AND s_suppkey IN (
      SELECT ps_suppkey
      FROM partsupp
      WHERE ps_availqty > 5000
  );

-- 5. Four-table join with ORDER BY
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND o_orderkey = l_orderkey
  AND c_nationkey = n_nationkey
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC;

-- 6. Small table scan (expects seq scan)
SELECT * FROM region;

-- 7. Selective filter (may trigger index scan)
SELECT o_orderkey, o_totalprice, o_orderdate
FROM orders
WHERE o_orderkey = 100;

-- 8. Six-table join with aggregation (TPC-H Q5 simplified)
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC;

-- 9. EXISTS subquery (correlated)
SELECT c_name, c_acctbal
FROM customer
WHERE c_acctbal > 9000
  AND EXISTS (
      SELECT 1 FROM orders
      WHERE o_custkey = c_custkey
        AND o_totalprice > 300000
  );

-- 10. DISTINCT with join
SELECT DISTINCT n_name, s_name
FROM supplier, nation
WHERE s_nationkey = n_nationkey
  AND s_acctbal > 9000
ORDER BY n_name, s_name;

-- 11. COUNT with GROUP BY (expects hash aggregate)
SELECT c_mktsegment, COUNT(*) AS cnt
FROM customer
GROUP BY c_mktsegment
ORDER BY cnt DESC;

-- 12. Scalar aggregation
SELECT MIN(o_totalprice), MAX(o_totalprice), AVG(o_totalprice)
FROM orders;

-- 13. BETWEEN range filter
SELECT l_orderkey, l_shipdate, l_quantity
FROM lineitem
WHERE l_shipdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31'
  AND l_quantity > 40;

-- 14. LIKE filter with three-table join
SELECT p_name, p_type, s_name
FROM part, partsupp, supplier
WHERE p_partkey = ps_partkey
  AND ps_suppkey = s_suppkey
  AND p_type LIKE '%BRASS%'
  AND ps_supplycost < 100;

-- 15. Correlated subquery (expects nested loop)
SELECT c_name, c_acctbal,
       (SELECT COUNT(*) FROM orders WHERE o_custkey = c_custkey) AS order_count
FROM customer
WHERE c_nationkey = 24;
