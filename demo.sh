#!/usr/bin/env bash
set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}   RustLake — The All-Rust Data Platform Demo${RESET}"
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo ""

BIN="cargo run --bin rustlake --"

echo -e "${CYAN}1. Basic SQL — Compute expressions${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"SELECT 1 + 1 AS math, 'hello' AS greeting\"${RESET}"
$BIN query "SELECT 1 + 1 AS math, 'hello' AS greeting" 2>/dev/null
echo ""

echo -e "${CYAN}2. Built-in functions — Date/time, math, strings${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"SELECT NOW() AS ts, PI() AS pi, UPPER('rustlake') AS name\"${RESET}"
$BIN query "SELECT NOW() AS ts, PI() AS pi, UPPER('rustlake') AS name" 2>/dev/null
echo ""

echo -e "${CYAN}3. Generate series — Create data on the fly${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"SELECT * FROM generate_series(1, 5) AS t(n)\"${RESET}"
$BIN query "SELECT * FROM generate_series(1, 5) AS t(n)" 2>/dev/null
echo ""

echo -e "${CYAN}4. Query CSV file — Register & analyze sales data${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"SELECT region, product, SUM(quantity) ... FROM 'sample-data/sales.csv' GROUP BY ...\"${RESET}"
$BIN query "
    SELECT
        region,
        product,
        SUM(quantity) AS total_qty,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM 'sample-data/sales.csv'
    GROUP BY region, product
    ORDER BY revenue DESC
" 2>/dev/null
echo ""

echo -e "${CYAN}5. Window functions — Ranking${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"SELECT ... RANK() OVER (PARTITION BY region ORDER BY ...) ...\"${RESET}"
$BIN query "
    SELECT
        region,
        product,
        SUM(quantity * price) AS revenue,
        RANK() OVER (PARTITION BY region ORDER BY SUM(quantity * price) DESC) AS rank
    FROM 'sample-data/sales.csv'
    GROUP BY region, product
    ORDER BY region, rank
" 2>/dev/null
echo ""

echo -e "${CYAN}6. JSON output format${RESET}"
echo -e "   ${GREEN}\$ rustlake -f json query \"SELECT region, COUNT(*) AS cnt FROM 'sample-data/sales.csv' GROUP BY region\"${RESET}"
$BIN -f json query "SELECT region, COUNT(*) AS cnt FROM 'sample-data/sales.csv' GROUP BY region ORDER BY cnt DESC" 2>/dev/null
echo ""

echo -e "${CYAN}7. EXPLAIN — Query execution plan${RESET}"
echo -e "   ${GREEN}\$ rustlake query \"EXPLAIN SELECT ... FROM 'sample-data/sales.csv' ...\"${RESET}"
$BIN query "EXPLAIN SELECT region, SUM(quantity) FROM 'sample-data/sales.csv' GROUP BY region" 2>/dev/null
echo ""

echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}   Demo complete. All queries powered by:${RESET}"
echo -e "${BOLD}   Apache Arrow + DataFusion + Rust${RESET}"
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
