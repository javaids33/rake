#!/usr/bin/env bash
# ============================================================================
# RustLake Persona Test Suite
#
# Simulates 5 real engineer personas interacting with the RustLake platform
# via the HTTP API. Each persona exercises different workflows and validates
# response correctness.
#
# Usage: ./tests/persona_tests.sh [--api-url http://127.0.0.1:3000]
# ============================================================================

set -euo pipefail

API="${1:-http://127.0.0.1:3000}"
PASS=0
FAIL=0
TOTAL=0
RESULTS=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

run_test() {
  local persona="$1"
  local test_name="$2"
  local method="$3"
  local endpoint="$4"
  local body="${5:-}"
  local expected_field="${6:-}"
  local expected_check="${7:-}"

  TOTAL=$((TOTAL + 1))

  local start_ms
  start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')

  local response
  if [ "$method" = "GET" ]; then
    response=$(curl -s "$API$endpoint" 2>/dev/null) || response=""
  else
    response=$(curl -s -X POST "$API$endpoint" \
      -H "Content-Type: application/json" \
      -d "$body" 2>/dev/null) || response=""
  fi

  local end_ms
  end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
  local duration=$((end_ms - start_ms))

  local status="FAIL"
  if [ -n "$response" ]; then
    if [ -n "$expected_field" ] && [ -n "$expected_check" ]; then
      local actual
      actual=$(echo "$response" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    keys = '$expected_field'.split('.')
    v = d
    for k in keys:
        if isinstance(v, list):
            v = v[int(k)] if k.isdigit() else None
            break
        v = v.get(k)
    print(v if v is not None else '')
except: print('')
" 2>/dev/null)
      if [ "$expected_check" = "notempty" ]; then
        [ -n "$actual" ] && status="PASS"
      elif [ "$expected_check" = "gt0" ]; then
        [ -n "$actual" ] && [ "$actual" != "0" ] && [ "$actual" != "None" ] && status="PASS"
      elif [ "$actual" = "$expected_check" ]; then
        status="PASS"
      fi
    else
      # Just check we got a valid JSON response
      echo "$response" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null && status="PASS"
    fi
  fi

  if [ "$status" = "PASS" ]; then
    PASS=$((PASS + 1))
    printf "  ${GREEN}PASS${NC}  %-20s %-42s %6dms\n" "[$persona]" "$test_name" "$duration"
  else
    FAIL=$((FAIL + 1))
    printf "  ${RED}FAIL${NC}  %-20s %-42s %6dms\n" "[$persona]" "$test_name" "$duration"
  fi
  RESULTS+="| $persona | $test_name | $status | ${duration}ms |\n"
}

echo ""
echo -e "${BOLD}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║         RustLake Platform — Persona Test Suite               ║${NC}"
echo -e "${BOLD}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}👤 PERSONA 1: Data Engineer (Sarah)${NC}"
echo -e "   Builds pipelines, registers tables, monitors streaming"
echo ""

run_test "DataEng" "Health check" \
  GET "/health" "" "status" "ok"

TNAME="test_orders_$(date +%s)"
run_test "DataEng" "Register CSV table" \
  POST "/api/v1/tables/register" \
  "{\"name\":\"$TNAME\",\"path\":\"sample-data/orders.csv\",\"format\":\"csv\"}" \
  "status" "ok"

run_test "DataEng" "List all tables" \
  GET "/api/v1/tables" "" "tables" "notempty"

run_test "DataEng" "Get table schema" \
  GET "/api/v1/tables/$TNAME/schema" "" "table" "$TNAME"

run_test "DataEng" "Get table stats" \
  GET "/api/v1/tables/$TNAME/stats" "" "row_count" "gt0"

run_test "DataEng" "Stream ingest 100 events" \
  POST "/api/v1/stream/ingest" \
  '{"count":100}' \
  "events_generated" "100"

run_test "DataEng" "Check stream status" \
  GET "/api/v1/stream/status" "" "metrics.events_ingested" "gt0"

run_test "DataEng" "Get stream events" \
  GET "/api/v1/stream/events?limit=10" "" "count" "gt0"

run_test "DataEng" "Stream ingest 500 events" \
  POST "/api/v1/stream/ingest" \
  '{"count":500}' \
  "events_generated" "500"

run_test "DataEng" "System info check" \
  GET "/api/v1/system/info" "" "engine" "DataFusion"

echo ""

# ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}👤 PERSONA 2: Data Scientist (Marcus)${NC}"
echo -e "   Runs exploratory SQL, uses vector search, analyzes data"
echo ""

run_test "DataSci" "Simple SELECT query" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT 1 + 1 AS result"}' \
  "row_count" "1"

run_test "DataSci" "Explore orders table" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT * FROM '\''sample-data/orders.csv'\'' LIMIT 5"}' \
  "row_count" "5"

run_test "DataSci" "Aggregation query" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT status, COUNT(*) AS cnt, ROUND(AVG(total_amount),2) AS avg_amt FROM '\''sample-data/orders.csv'\'' GROUP BY status ORDER BY cnt DESC"}' \
  "row_count" "gt0"

run_test "DataSci" "Multi-table JOIN" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT c.name, c.tier, ROUND(SUM(o.total_amount),2) AS spend FROM '\''sample-data/customers.csv'\'' AS c JOIN '\''sample-data/orders.csv'\'' AS o ON c.customer_id = o.customer_id GROUP BY c.name, c.tier ORDER BY spend DESC LIMIT 5"}' \
  "row_count" "5"

run_test "DataSci" "Vector search: headphones" \
  POST "/api/v1/vector/search" \
  '{"query":"wireless headphones","k":5}' \
  "result_count" "gt0"

run_test "DataSci" "Vector search: coffee" \
  POST "/api/v1/vector/search" \
  '{"query":"organic coffee beans","k":5}' \
  "result_count" "gt0"

run_test "DataSci" "Vector index status" \
  GET "/api/v1/vector/status" "" "document_count" "gt0"

run_test "DataSci" "Window function query" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT customer_id, total_amount, ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS rn FROM '\''sample-data/orders.csv'\'' LIMIT 10"}' \
  "row_count" "10"

echo ""

# ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}👤 PERSONA 3: Analytics Engineer (Priya)${NC}"
echo -e "   Runs dbt-style transforms, checks lineage, builds models"
echo ""

run_test "AnalyEng" "List transforms" \
  GET "/api/v1/transforms" "" "transforms" "notempty"

run_test "AnalyEng" "View lineage DAG" \
  GET "/api/v1/lineage" "" "nodes" "notempty"

run_test "AnalyEng" "Run stg_orders transform" \
  POST "/api/v1/transforms/stg_orders/run" '{}' \
  "row_count" "gt0"

run_test "AnalyEng" "Run stg_customers transform" \
  POST "/api/v1/transforms/stg_customers/run" '{}' \
  "row_count" "gt0"

run_test "AnalyEng" "Run fct_revenue transform" \
  POST "/api/v1/transforms/fct_revenue/run" '{}' \
  "row_count" "gt0"

run_test "AnalyEng" "Run rpt_customer_ltv transform" \
  POST "/api/v1/transforms/rpt_customer_ltv/run" '{}' \
  "row_count" "gt0"

run_test "AnalyEng" "Run dim_product_category" \
  POST "/api/v1/transforms/dim_product_category/run" '{}' \
  "row_count" "gt0"

run_test "AnalyEng" "CTE aggregation query" \
  POST "/api/v1/sql" \
  '{"sql":"WITH daily AS (SELECT CAST(order_date AS DATE) AS day, COUNT(*) AS orders, ROUND(SUM(total_amount),2) AS rev FROM '\''sample-data/orders.csv'\'' WHERE status = '\''completed'\'' GROUP BY day) SELECT day, orders, rev FROM daily ORDER BY day LIMIT 10"}' \
  "row_count" "gt0"

echo ""

# ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}👤 PERSONA 4: Platform Engineer (Alex)${NC}"
echo -e "   Monitors infrastructure, checks Flight, validates API health"
echo ""

run_test "PlatEng" "API health endpoint" \
  GET "/health" "" "status" "ok"

run_test "PlatEng" "System info uptime" \
  GET "/api/v1/system/info" "" "uptime_seconds" "gt0"

run_test "PlatEng" "System query counter" \
  GET "/api/v1/system/info" "" "total_queries" "gt0"

run_test "PlatEng" "Flight server info" \
  GET "/api/v1/flight/info" "" "protocol" "Arrow Flight SQL"

run_test "PlatEng" "Flight capabilities" \
  GET "/api/v1/flight/info" "" "capabilities" "notempty"

run_test "PlatEng" "Query history available" \
  GET "/api/v1/query/history?limit=5" "" "count" "gt0"

run_test "PlatEng" "Table preview works" \
  GET "/api/v1/tables/$TNAME/preview" "" "row_count" "gt0"

run_test "PlatEng" "Error handling: bad SQL" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT FROM INVALID SYNTAX"}' \
  "error" "notempty"  # Should return error message

echo ""

# ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}👤 PERSONA 5: Business Analyst (Jordan)${NC}"
echo -e "   Runs dashboards, revenue reports, customer segmentation"
echo ""

run_test "BizAnalyst" "Revenue by category" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT p.category, ROUND(SUM(o.total_amount),2) AS revenue FROM '\''sample-data/orders.csv'\'' AS o JOIN '\''sample-data/products.csv'\'' AS p ON o.product_id = p.product_id WHERE o.status = '\''completed'\'' GROUP BY p.category ORDER BY revenue DESC"}' \
  "row_count" "gt0"

run_test "BizAnalyst" "Customer tier breakdown" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT c.tier, COUNT(DISTINCT c.customer_id) AS customers, COUNT(o.order_id) AS orders, ROUND(SUM(o.total_amount),2) AS revenue FROM '\''sample-data/customers.csv'\'' AS c JOIN '\''sample-data/orders.csv'\'' AS o ON c.customer_id = o.customer_id GROUP BY c.tier ORDER BY revenue DESC"}' \
  "row_count" "gt0"

run_test "BizAnalyst" "Top products by revenue" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT p.name, p.category, COUNT(*) AS orders, ROUND(SUM(o.total_amount),2) AS revenue FROM '\''sample-data/orders.csv'\'' AS o JOIN '\''sample-data/products.csv'\'' AS p ON o.product_id = p.product_id GROUP BY p.name, p.category ORDER BY revenue DESC LIMIT 10"}' \
  "row_count" "10"

run_test "BizAnalyst" "Monthly trend" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT DATE_TRUNC('\''month'\'', CAST(order_date AS DATE)) AS month, COUNT(*) AS orders, ROUND(SUM(total_amount),2) AS revenue FROM '\''sample-data/orders.csv'\'' WHERE status IN ('\''completed'\'','\''shipped'\'') GROUP BY month ORDER BY month"}' \
  "row_count" "gt0"

run_test "BizAnalyst" "Conversion funnel" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT event_type, COUNT(*) AS events, COUNT(DISTINCT customer_id) AS users FROM '\''sample-data/events.csv'\'' GROUP BY event_type ORDER BY events DESC"}' \
  "row_count" "gt0"

run_test "BizAnalyst" "Order status distribution" \
  POST "/api/v1/sql" \
  '{"sql":"SELECT status, COUNT(*) AS cnt, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct FROM '\''sample-data/orders.csv'\'' GROUP BY status ORDER BY cnt DESC"}' \
  "row_count" "gt0"

echo ""

# ─────────────────────────────────────────────────────────────────
# Summary
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  RESULTS SUMMARY${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Total tests:  ${BOLD}$TOTAL${NC}"
echo -e "  Passed:       ${GREEN}${BOLD}$PASS${NC}"
echo -e "  Failed:       ${RED}${BOLD}$FAIL${NC}"
echo -e "  Pass rate:    ${BOLD}$(( PASS * 100 / TOTAL ))%${NC}"
echo ""

if [ $FAIL -eq 0 ]; then
  echo -e "  ${GREEN}${BOLD}✓ ALL PERSONA TESTS PASSED${NC}"
else
  echo -e "  ${RED}${BOLD}✗ $FAIL TEST(S) FAILED${NC}"
fi
echo ""

# Save markdown report
REPORT_FILE="benchmarks/results/persona_tests_$(date +%Y%m%d_%H%M%S).md"
mkdir -p benchmarks/results
cat > "$REPORT_FILE" << EOF
# RustLake Persona Test Results

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**API:** $API
**Total:** $TOTAL | **Passed:** $PASS | **Failed:** $FAIL | **Rate:** $(( PASS * 100 / TOTAL ))%

## Personas

| Persona | Role | Tests | Focus |
|---------|------|-------|-------|
| Sarah | Data Engineer | 10 | Pipelines, table registration, streaming |
| Marcus | Data Scientist | 8 | Exploratory SQL, vector search, analytics |
| Priya | Analytics Engineer | 8 | dbt transforms, lineage, CTE queries |
| Alex | Platform Engineer | 8 | Health checks, Flight, monitoring |
| Jordan | Business Analyst | 6 | Revenue reports, customer segmentation |

## Test Results

| Persona | Test | Status | Duration |
|---------|------|--------|----------|
$(echo -e "$RESULTS")

EOF

echo -e "  Report saved: ${CYAN}$REPORT_FILE${NC}"
echo ""
