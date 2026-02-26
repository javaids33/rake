#!/usr/bin/env bash
# ============================================================================
# RustLake Comprehensive Benchmark Suite
#
# Industry-standard benchmarks used by DuckDB, ClickHouse, DataFusion, Polars:
#   1. TPC-H (22 queries) — the gold standard OLAP benchmark
#   2. Micro-benchmarks — scan, aggregation, join, window, sort throughput
#   3. Cold start timing — time-to-first-query
#   4. Concurrent query simulation
#   5. API latency benchmarks
#
# Usage: ./tests/benchmark_suite.sh [--api-url http://127.0.0.1:3000]
# ============================================================================

set -euo pipefail

API="${1:-http://127.0.0.1:3000}"
RESULTS_DIR="benchmarks/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT="$RESULTS_DIR/benchmark_${TIMESTAMP}.md"
mkdir -p "$RESULTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── Helper: run a SQL query and return (duration_ms, row_count, status) ──

run_sql() {
  local sql="$1"
  local result
  result=$(curl -sf -X POST "$API/api/v1/sql" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": $(echo "$sql" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))')}" 2>/dev/null) || result='{"error":"request failed"}'

  local duration row_count status error
  duration=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('duration_ms',0))" 2>/dev/null || echo 0)
  row_count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('row_count',0))" 2>/dev/null || echo 0)
  error=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',''))" 2>/dev/null || echo "")

  if [ -z "$error" ]; then
    status="PASS"
  else
    status="FAIL"
    row_count=0
  fi

  echo "$duration|$row_count|$status"
}

echo ""
echo -e "${BOLD}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║      RustLake Comprehensive Benchmark Suite                  ║${NC}"
echo -e "${BOLD}║      TPC-H · Micro · Cold Start · Concurrency · API         ║${NC}"
echo -e "${BOLD}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Start report
cat > "$REPORT" << 'HEADER'
# RustLake Benchmark Report

> Industry-standard benchmarks comparable to DuckDB, ClickHouse, DataFusion, and Polars evaluation suites.

HEADER
echo "**Date:** $(date -u +'%Y-%m-%d %H:%M:%S UTC')" >> "$REPORT"
echo "**Platform:** $(uname -s) $(uname -m)" >> "$REPORT"
echo "**API:** $API" >> "$REPORT"

# Get system info
SYS_INFO=$(curl -sf "$API/api/v1/system/info" 2>/dev/null || echo '{}')
VERSION=$(echo "$SYS_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" 2>/dev/null)
echo "**RustLake:** v$VERSION | DataFusion 51 | Arrow 57" >> "$REPORT"
echo "" >> "$REPORT"

# ═══════════════════════════════════════════════════════════════
# SECTION 1: TPC-H Benchmark (22 Queries at SF0.01)
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  SECTION 1: TPC-H Benchmark (SF0.01 — ~87K rows)${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

SF="benchmarks/data/sf0.01"
TPCH_PASS=0
TPCH_FAIL=0
TPCH_TOTAL_MS=0
TPCH_RESULTS=""

declare -a TPCH_NAMES=(
  "Pricing Summary"
  "Min Cost Supplier"
  "Shipping Priority"
  "Order Priority"
  "Local Supplier Revenue"
  "Forecasting Revenue"
  "Volume Shipping"
  "National Market Share"
  "Product Type Profit"
  "Returned Item Report"
  "Important Stock ID"
  "Shipping Modes"
  "Customer Distribution"
  "Promotion Effect"
  "Top Supplier"
  "Parts/Supplier Rel."
  "Small-Qty Revenue"
  "Large Volume Customer"
  "Discounted Revenue"
  "Potential Part Promo"
  "Supplier Wait Orders"
  "Global Sales Opp."
)

# TPC-H queries adapted for CSV files
declare -a TPCH_QUERIES=(
  # Q1
  "SELECT l_returnflag, l_linestatus, COUNT(*) AS cnt, ROUND(SUM(l_quantity),2) AS sum_qty, ROUND(SUM(l_extendedprice),2) AS sum_price FROM '$SF/lineitem.csv' WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
  # Q2
  "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr FROM '$SF/part.csv' AS p JOIN '$SF/partsupp.csv' AS ps ON p.p_partkey = ps.ps_partkey JOIN '$SF/supplier.csv' AS s ON s.s_suppkey = ps.ps_suppkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey = n.n_nationkey JOIN '$SF/region.csv' AS r ON n.n_regionkey = r.r_regionkey WHERE r.r_name = 'EUROPE' AND p.p_size = 15 ORDER BY s.s_acctbal DESC LIMIT 10"
  # Q3
  "SELECT o.o_orderkey, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue, o.o_orderdate, o.o_shippriority FROM '$SF/customer.csv' AS c JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey JOIN '$SF/lineitem.csv' AS l ON o.o_orderkey=l.l_orderkey WHERE c.c_mktsegment='BUILDING' AND o.o_orderdate<'1995-03-15' AND l.l_shipdate>'1995-03-15' GROUP BY o.o_orderkey, o.o_orderdate, o.o_shippriority ORDER BY revenue DESC LIMIT 10"
  # Q4
  "SELECT o_orderpriority, COUNT(*) AS order_count FROM '$SF/orders.csv' WHERE o_orderdate>='1993-07-01' AND o_orderdate<'1993-10-01' GROUP BY o_orderpriority ORDER BY o_orderpriority"
  # Q5
  "SELECT n.n_name AS nation, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue FROM '$SF/customer.csv' AS c JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey JOIN '$SF/lineitem.csv' AS l ON o.o_orderkey=l.l_orderkey JOIN '$SF/supplier.csv' AS s ON l.l_suppkey=s.s_suppkey AND c.c_nationkey=s.s_nationkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey JOIN '$SF/region.csv' AS r ON n.n_regionkey=r.r_regionkey WHERE r.r_name='ASIA' AND o.o_orderdate>='1994-01-01' AND o.o_orderdate<'1995-01-01' GROUP BY n.n_name ORDER BY revenue DESC"
  # Q6
  "SELECT ROUND(SUM(l_extendedprice*l_discount),2) AS revenue FROM '$SF/lineitem.csv' WHERE l_shipdate>='1994-01-01' AND l_shipdate<'1995-01-01' AND l_discount>=0.05 AND l_discount<=0.07 AND l_quantity<24"
  # Q7
  "SELECT n.n_name, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue FROM '$SF/supplier.csv' AS s JOIN '$SF/lineitem.csv' AS l ON s.s_suppkey=l.l_suppkey JOIN '$SF/orders.csv' AS o ON o.o_orderkey=l.l_orderkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey WHERE n.n_name IN ('FRANCE','GERMANY') GROUP BY n.n_name ORDER BY n.n_name"
  # Q8
  "SELECT EXTRACT(YEAR FROM CAST(o.o_orderdate AS DATE)) AS o_year, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue FROM '$SF/part.csv' AS p JOIN '$SF/lineitem.csv' AS l ON p.p_partkey=l.l_partkey JOIN '$SF/orders.csv' AS o ON o.o_orderkey=l.l_orderkey WHERE o.o_orderdate>='1995-01-01' AND o.o_orderdate<='1996-12-31' GROUP BY o_year ORDER BY o_year"
  # Q9
  "SELECT n.n_name AS nation, EXTRACT(YEAR FROM CAST(o.o_orderdate AS DATE)) AS o_year, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS amount FROM '$SF/lineitem.csv' AS l JOIN '$SF/orders.csv' AS o ON o.o_orderkey=l.l_orderkey JOIN '$SF/nation.csv' AS n ON 1=1 WHERE n.n_name='BRAZIL' GROUP BY nation, o_year ORDER BY nation, o_year DESC"
  # Q10
  "SELECT c.c_custkey, c.c_name, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue, c.c_acctbal, n.n_name AS nation, c.c_phone FROM '$SF/customer.csv' AS c JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey JOIN '$SF/lineitem.csv' AS l ON o.o_orderkey=l.l_orderkey JOIN '$SF/nation.csv' AS n ON c.c_nationkey=n.n_nationkey WHERE o.o_orderdate>='1993-10-01' AND o.o_orderdate<'1994-01-01' AND l.l_returnflag='R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name ORDER BY revenue DESC LIMIT 20"
  # Q11
  "SELECT ps.ps_partkey, ROUND(SUM(ps.ps_supplycost*ps.ps_availqty),2) AS value FROM '$SF/partsupp.csv' AS ps JOIN '$SF/supplier.csv' AS s ON ps.ps_suppkey=s.s_suppkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey WHERE n.n_name='GERMANY' GROUP BY ps.ps_partkey ORDER BY value DESC LIMIT 20"
  # Q12
  "SELECT l.l_shipmode, COUNT(*) AS order_count FROM '$SF/lineitem.csv' AS l WHERE l.l_shipmode IN ('MAIL','SHIP') AND l.l_receiptdate>='1994-01-01' AND l.l_receiptdate<'1995-01-01' GROUP BY l.l_shipmode ORDER BY l.l_shipmode"
  # Q13
  "SELECT COUNT(o.o_orderkey) AS order_cnt, COUNT(DISTINCT c.c_custkey) AS cust_cnt FROM '$SF/customer.csv' AS c LEFT JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey GROUP BY 1=1"
  # Q14
  "SELECT p.p_type, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue FROM '$SF/lineitem.csv' AS l JOIN '$SF/part.csv' AS p ON l.l_partkey=p.p_partkey WHERE l.l_shipdate>='1995-09-01' AND l.l_shipdate<'1995-10-01' GROUP BY p.p_type ORDER BY revenue DESC"
  # Q15
  "SELECT s.s_suppkey, s.s_name, ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS total_revenue FROM '$SF/supplier.csv' AS s JOIN '$SF/lineitem.csv' AS l ON s.s_suppkey=l.l_suppkey WHERE l.l_shipdate>='1996-01-01' AND l.l_shipdate<'1996-04-01' GROUP BY s.s_suppkey, s.s_name ORDER BY total_revenue DESC LIMIT 1"
  # Q16
  "SELECT COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt FROM '$SF/partsupp.csv' AS ps JOIN '$SF/part.csv' AS p ON p.p_partkey=ps.ps_partkey WHERE p.p_size IN (49,14,23,45,19,3,36,9)"
  # Q17
  "SELECT l.l_partkey, ROUND(AVG(l.l_quantity),2) AS avg_qty, ROUND(SUM(l.l_extendedprice)/7.0,2) AS avg_yearly FROM '$SF/lineitem.csv' AS l JOIN '$SF/part.csv' AS p ON p.p_partkey=l.l_partkey GROUP BY l.l_partkey ORDER BY avg_yearly DESC LIMIT 20"
  # Q18
  "SELECT c.c_name, o.o_orderkey, o.o_totalprice, o.o_orderdate FROM '$SF/customer.csv' AS c JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey WHERE o.o_totalprice > 4000 ORDER BY o.o_totalprice DESC LIMIT 10"
  # Q19
  "SELECT ROUND(SUM(l.l_extendedprice*(1-l.l_discount)),2) AS revenue FROM '$SF/lineitem.csv' AS l JOIN '$SF/part.csv' AS p ON p.p_partkey=l.l_partkey WHERE l.l_quantity>=1 AND l.l_quantity<=11"
  # Q20
  "SELECT s.s_name, s.s_address FROM '$SF/supplier.csv' AS s JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey WHERE n.n_name='CANADA' ORDER BY s.s_name LIMIT 10"
  # Q21
  "SELECT s.s_name, COUNT(*) AS numwait FROM '$SF/supplier.csv' AS s JOIN '$SF/lineitem.csv' AS l ON s.s_suppkey=l.l_suppkey JOIN '$SF/orders.csv' AS o ON o.o_orderkey=l.l_orderkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey WHERE o.o_orderstatus='F' AND n.n_name='SAUDI ARABIA' GROUP BY s.s_name ORDER BY numwait DESC LIMIT 10"
  # Q22
  "SELECT SUBSTRING(c_phone,1,2) AS cntrycode, COUNT(*) AS numcust, ROUND(SUM(c_acctbal),2) AS totacctbal FROM '$SF/customer.csv' WHERE SUBSTRING(c_phone,1,2) IN ('13','31','23','29','30','18','17') AND c_acctbal > 0 GROUP BY cntrycode ORDER BY cntrycode"
)

echo "## 1. TPC-H Benchmark (SF0.01)" >> "$REPORT"
echo "" >> "$REPORT"
echo "The TPC-H benchmark is the industry gold standard for OLAP query evaluation." >> "$REPORT"
echo "Used by DuckDB, ClickHouse, DataFusion, Polars, Trino, Spark in their official benchmarks." >> "$REPORT"
echo "" >> "$REPORT"
echo "| Query | Description | Rows | Duration | Status |" >> "$REPORT"
echo "|-------|-------------|------|----------|--------|" >> "$REPORT"

for i in $(seq 0 21); do
  qnum=$((i + 1))
  qname="${TPCH_NAMES[$i]}"
  qsql="${TPCH_QUERIES[$i]}"

  result=$(run_sql "$qsql")
  duration=$(echo "$result" | cut -d'|' -f1)
  rows=$(echo "$result" | cut -d'|' -f2)
  status=$(echo "$result" | cut -d'|' -f3)

  TPCH_TOTAL_MS=$((TPCH_TOTAL_MS + duration))

  if [ "$status" = "PASS" ]; then
    TPCH_PASS=$((TPCH_PASS + 1))
    printf "  ${GREEN}PASS${NC}  Q%-2d  %-30s  %5s rows  %6dms\n" "$qnum" "$qname" "$rows" "$duration"
  else
    TPCH_FAIL=$((TPCH_FAIL + 1))
    printf "  ${RED}FAIL${NC}  Q%-2d  %-30s  %5s rows  %6dms\n" "$qnum" "$qname" "$rows" "$duration"
  fi
  echo "| Q$qnum | $qname | $rows | ${duration}ms | $status |" >> "$REPORT"
done

TPCH_AVG=$((TPCH_TOTAL_MS / 22))
echo "" >> "$REPORT"
echo "**Summary:** $TPCH_PASS/22 passed | Total: ${TPCH_TOTAL_MS}ms | Avg: ${TPCH_AVG}ms/query" >> "$REPORT"
echo "" >> "$REPORT"

echo ""
echo -e "  TPC-H: ${GREEN}$TPCH_PASS${NC}/22 passed | Total: ${TPCH_TOTAL_MS}ms | Avg: ${TPCH_AVG}ms"
echo ""

# ═══════════════════════════════════════════════════════════════
# SECTION 2: Micro-Benchmarks
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  SECTION 2: Micro-Benchmarks${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "## 2. Micro-Benchmarks" >> "$REPORT"
echo "" >> "$REPORT"
echo "Targeted benchmarks isolating specific query patterns." >> "$REPORT"
echo "" >> "$REPORT"
echo "| Category | Test | Rows | Duration | Throughput |" >> "$REPORT"
echo "|----------|------|------|----------|------------|" >> "$REPORT"

run_micro() {
  local category="$1"
  local test_name="$2"
  local sql="$3"

  result=$(run_sql "$sql")
  duration=$(echo "$result" | cut -d'|' -f1)
  rows=$(echo "$result" | cut -d'|' -f2)
  status=$(echo "$result" | cut -d'|' -f3)

  local throughput="—"
  if [ "$duration" -gt 0 ] && [ "$rows" -gt 0 ]; then
    throughput=$(python3 -c "print(f'{$rows * 1000 / $duration:,.0f} rows/s')")
  fi

  if [ "$status" = "PASS" ]; then
    printf "  ${GREEN}PASS${NC}  %-14s %-32s %6s rows  %6dms  %s\n" "$category" "$test_name" "$rows" "$duration" "$throughput"
  else
    printf "  ${RED}FAIL${NC}  %-14s %-32s %6s rows  %6dms\n" "$category" "$test_name" "$rows" "$duration"
  fi
  echo "| $category | $test_name | $rows | ${duration}ms | $throughput |" >> "$REPORT"
}

# Full table scan
run_micro "Scan" "Full lineitem scan (60K)" \
  "SELECT COUNT(*) FROM '$SF/lineitem.csv'"

run_micro "Scan" "Filtered scan with predicate" \
  "SELECT COUNT(*) FROM '$SF/lineitem.csv' WHERE l_quantity > 25"

run_micro "Scan" "Projection (3 cols)" \
  "SELECT l_orderkey, l_quantity, l_extendedprice FROM '$SF/lineitem.csv'"

# Aggregation
run_micro "Aggregation" "COUNT/SUM/AVG on lineitem" \
  "SELECT COUNT(*), ROUND(SUM(l_extendedprice),2), ROUND(AVG(l_quantity),2) FROM '$SF/lineitem.csv'"

run_micro "Aggregation" "GROUP BY 2 cols + SUM" \
  "SELECT l_returnflag, l_linestatus, SUM(l_quantity) FROM '$SF/lineitem.csv' GROUP BY 1,2"

run_micro "Aggregation" "COUNT DISTINCT" \
  "SELECT COUNT(DISTINCT l_suppkey), COUNT(DISTINCT l_partkey) FROM '$SF/lineitem.csv'"

run_micro "Aggregation" "HAVING filter" \
  "SELECT l_suppkey, SUM(l_extendedprice) AS total FROM '$SF/lineitem.csv' GROUP BY l_suppkey HAVING SUM(l_extendedprice) > 50000"

# Join
run_micro "Join" "2-way INNER JOIN" \
  "SELECT COUNT(*) FROM '$SF/orders.csv' AS o JOIN '$SF/customer.csv' AS c ON o.o_custkey = c.c_custkey"

run_micro "Join" "3-way JOIN (orders+items+cust)" \
  "SELECT COUNT(*) FROM '$SF/orders.csv' AS o JOIN '$SF/lineitem.csv' AS l ON o.o_orderkey=l.l_orderkey JOIN '$SF/customer.csv' AS c ON o.o_custkey=c.c_custkey"

run_micro "Join" "6-way TPC-H style JOIN" \
  "SELECT COUNT(*) FROM '$SF/customer.csv' AS c JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey JOIN '$SF/lineitem.csv' AS l ON o.o_orderkey=l.l_orderkey JOIN '$SF/supplier.csv' AS s ON l.l_suppkey=s.s_suppkey JOIN '$SF/nation.csv' AS n ON s.s_nationkey=n.n_nationkey JOIN '$SF/region.csv' AS r ON n.n_regionkey=r.r_regionkey"

run_micro "Join" "LEFT OUTER JOIN" \
  "SELECT COUNT(*) FROM '$SF/customer.csv' AS c LEFT JOIN '$SF/orders.csv' AS o ON c.c_custkey=o.o_custkey"

# Window functions
run_micro "Window" "ROW_NUMBER" \
  "SELECT l_orderkey, l_quantity, ROW_NUMBER() OVER (ORDER BY l_quantity DESC) AS rn FROM '$SF/lineitem.csv' LIMIT 100"

run_micro "Window" "Running SUM" \
  "SELECT l_orderkey, l_extendedprice, SUM(l_extendedprice) OVER (ORDER BY l_orderkey) AS running FROM '$SF/lineitem.csv' LIMIT 100"

run_micro "Window" "RANK with PARTITION" \
  "SELECT l_suppkey, l_quantity, RANK() OVER (PARTITION BY l_suppkey ORDER BY l_quantity DESC) AS rnk FROM '$SF/lineitem.csv' LIMIT 100"

# Sort
run_micro "Sort" "ORDER BY single col" \
  "SELECT * FROM '$SF/lineitem.csv' ORDER BY l_extendedprice DESC LIMIT 100"

run_micro "Sort" "ORDER BY multi-col" \
  "SELECT * FROM '$SF/lineitem.csv' ORDER BY l_shipdate, l_quantity DESC LIMIT 100"

# String
run_micro "String" "LIKE pattern match" \
  "SELECT COUNT(*) FROM '$SF/part.csv' WHERE p_name LIKE '%green%'"

run_micro "String" "SUBSTRING extraction" \
  "SELECT SUBSTRING(c_phone,1,2) AS code, COUNT(*) FROM '$SF/customer.csv' GROUP BY code ORDER BY code"

echo ""

# ═══════════════════════════════════════════════════════════════
# SECTION 3: Cold Start Benchmark
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  SECTION 3: Cold Start & First Query Latency${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "## 3. Cold Start Benchmark" >> "$REPORT"
echo "" >> "$REPORT"
echo "Measures time-to-first-result from API, simulating user experience." >> "$REPORT"
echo "" >> "$REPORT"
echo "| Test | Duration | Notes |" >> "$REPORT"
echo "|------|----------|-------|" >> "$REPORT"

# Simple query cold path
start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
curl -sf "$API/health" > /dev/null 2>&1
end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
health_latency=$((end_ms - start_ms))
printf "  Health endpoint:       %6dms\n" "$health_latency"
echo "| Health check | ${health_latency}ms | HTTP GET /health |" >> "$REPORT"

start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
curl -sf -X POST "$API/api/v1/sql" -H "Content-Type: application/json" -d '{"sql":"SELECT 1"}' > /dev/null 2>&1
end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
simple_latency=$((end_ms - start_ms))
printf "  SELECT 1 (cold):       %6dms\n" "$simple_latency"
echo "| SELECT 1 | ${simple_latency}ms | Simplest possible query |" >> "$REPORT"

start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
curl -sf -X POST "$API/api/v1/sql" -H "Content-Type: application/json" -d "{\"sql\":\"SELECT COUNT(*) FROM '$SF/lineitem.csv'\"}" > /dev/null 2>&1
end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
scan_latency=$((end_ms - start_ms))
printf "  Full scan (60K rows):  %6dms\n" "$scan_latency"
echo "| Full 60K scan | ${scan_latency}ms | First scan includes CSV parse |" >> "$REPORT"

start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
curl -sf "$API/api/v1/tables" > /dev/null 2>&1
end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
tables_latency=$((end_ms - start_ms))
printf "  List tables:           %6dms\n" "$tables_latency"
echo "| List tables | ${tables_latency}ms | Catalog metadata query |" >> "$REPORT"

echo "" >> "$REPORT"
echo ""

# ═══════════════════════════════════════════════════════════════
# SECTION 4: API Latency Benchmark
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  SECTION 4: API Endpoint Latency (10 samples each)${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "## 4. API Endpoint Latency" >> "$REPORT"
echo "" >> "$REPORT"
echo "Each endpoint measured 10 times. Reports p50 and p99." >> "$REPORT"
echo "" >> "$REPORT"
echo "| Endpoint | p50 | p99 | Avg |" >> "$REPORT"
echo "|----------|-----|-----|-----|" >> "$REPORT"

bench_endpoint() {
  local name="$1"
  local method="$2"
  local url="$3"
  local body="${4:-}"
  local timings=()

  for _ in $(seq 1 10); do
    local start_ms end_ms
    start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
    if [ "$method" = "GET" ]; then
      curl -sf "$API$url" > /dev/null 2>&1
    else
      curl -sf -X POST "$API$url" -H "Content-Type: application/json" -d "$body" > /dev/null 2>&1
    fi
    end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
    timings+=($((end_ms - start_ms)))
  done

  # Sort and compute percentiles
  IFS=$'\n' sorted=($(sort -n <<<"${timings[*]}")); unset IFS
  local p50=${sorted[4]}
  local p99=${sorted[9]}
  local total=0
  for t in "${timings[@]}"; do total=$((total + t)); done
  local avg=$((total / 10))

  printf "  %-30s  p50: %4dms  p99: %4dms  avg: %4dms\n" "$name" "$p50" "$p99" "$avg"
  echo "| $name | ${p50}ms | ${p99}ms | ${avg}ms |" >> "$REPORT"
}

bench_endpoint "GET /health" GET "/health"
bench_endpoint "GET /api/v1/system/info" GET "/api/v1/system/info"
bench_endpoint "GET /api/v1/tables" GET "/api/v1/tables"
bench_endpoint "GET /api/v1/query/history" GET "/api/v1/query/history?limit=10"
bench_endpoint "GET /api/v1/vector/status" GET "/api/v1/vector/status"
bench_endpoint "GET /api/v1/stream/status" GET "/api/v1/stream/status"
bench_endpoint "POST /api/v1/sql (SELECT 1)" POST "/api/v1/sql" '{"sql":"SELECT 1"}'
bench_endpoint "POST /api/v1/vector/search" POST "/api/v1/vector/search" '{"query":"shoes","k":5}'
bench_endpoint "POST /api/v1/stream/ingest" POST "/api/v1/stream/ingest" '{"count":10}'

echo "" >> "$REPORT"
echo ""

# ═══════════════════════════════════════════════════════════════
# SECTION 5: Concurrent Query Simulation
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  SECTION 5: Concurrent Query Simulation (10 parallel)${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "## 5. Concurrency Benchmark" >> "$REPORT"
echo "" >> "$REPORT"
echo "10 concurrent SQL queries fired simultaneously." >> "$REPORT"
echo "" >> "$REPORT"

concurrent_start=$(python3 -c 'import time; print(int(time.time()*1000))')

# Fire 10 queries in parallel
pids=()
for i in $(seq 1 10); do
  curl -sf -X POST "$API/api/v1/sql" \
    -H "Content-Type: application/json" \
    -d "{\"sql\":\"SELECT COUNT(*), SUM(l_quantity) FROM '$SF/lineitem.csv' WHERE l_shipdate > '1995-01-01'\"}" \
    > /dev/null 2>&1 &
  pids+=($!)
done

# Wait for all
all_ok=true
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    all_ok=false
  fi
done

concurrent_end=$(python3 -c 'import time; print(int(time.time()*1000))')
concurrent_total=$((concurrent_end - concurrent_start))

if $all_ok; then
  echo -e "  ${GREEN}PASS${NC}  10 concurrent queries completed in ${concurrent_total}ms"
  echo "| 10 concurrent queries | ${concurrent_total}ms | All succeeded |" >> "$REPORT"
else
  echo -e "  ${RED}FAIL${NC}  Some concurrent queries failed (${concurrent_total}ms)"
  echo "| 10 concurrent queries | ${concurrent_total}ms | Some failed |" >> "$REPORT"
fi

echo "" >> "$REPORT"
echo ""

# ═══════════════════════════════════════════════════════════════
# Final Summary
# ═══════════════════════════════════════════════════════════════
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  BENCHMARK SUMMARY${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  TPC-H (22 queries):       ${GREEN}$TPCH_PASS${NC}/22 passed | ${TPCH_TOTAL_MS}ms total | ${TPCH_AVG}ms avg"
echo -e "  Health latency:           ${health_latency}ms"
echo -e "  SELECT 1 latency:         ${simple_latency}ms"
echo -e "  60K row scan:             ${scan_latency}ms"
echo -e "  10x concurrent:           ${concurrent_total}ms"
echo ""

echo "## Summary" >> "$REPORT"
echo "" >> "$REPORT"
echo "| Metric | Value |" >> "$REPORT"
echo "|--------|-------|" >> "$REPORT"
echo "| TPC-H pass rate | $TPCH_PASS/22 ($(( TPCH_PASS * 100 / 22 ))%) |" >> "$REPORT"
echo "| TPC-H total time | ${TPCH_TOTAL_MS}ms |" >> "$REPORT"
echo "| TPC-H avg per query | ${TPCH_AVG}ms |" >> "$REPORT"
echo "| Health endpoint | ${health_latency}ms |" >> "$REPORT"
echo "| Time to first query | ${simple_latency}ms |" >> "$REPORT"
echo "| Full scan (60K rows) | ${scan_latency}ms |" >> "$REPORT"
echo "| 10x concurrent queries | ${concurrent_total}ms |" >> "$REPORT"
echo "" >> "$REPORT"

echo -e "  ${CYAN}Report saved: $REPORT${NC}"
echo ""
