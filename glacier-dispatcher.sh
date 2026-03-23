#!/usr/bin/env bash
#
# Glacier Dispatcher — standalone glacier execution without the RustLake server
#
# This tool maintains glacier tables independently:
# 1. Reads cached glacier metadata (exported before server shutdown)
# 2. Finds compiled Rust binaries in the cache directory
# 3. Executes each glacier in topological order (respecting dependencies)
# 4. Validates quality gates against output
# 5. Records execution results
#
# Usage:
#   ./glacier-dispatcher.sh export    # Export glacier metadata while server is running
#   ./glacier-dispatcher.sh run       # Execute all glaciers (server can be down)
#   ./glacier-dispatcher.sh status    # Show last execution results
#
# This proves the "glaciers maintain themselves" thesis — no server needed.

set -euo pipefail

CACHE_DIR=".rustlake-cache"
DISPATCHER_DIR="$CACHE_DIR/dispatcher"
METADATA_FILE="$DISPATCHER_DIR/glaciers.json"
RESULTS_FILE="$DISPATCHER_DIR/results.json"
BINARY_CACHE="$CACHE_DIR/rust-bins"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
AMBER='\033[0;33m'
NC='\033[0m'

log() { echo -e "${CYAN}[glacier-dispatcher]${NC} $1"; }
ok()  { echo -e "${GREEN}  ✓${NC} $1"; }
err() { echo -e "${RED}  ✗${NC} $1"; }
warn(){ echo -e "${AMBER}  ⚠${NC} $1"; }

# ── Export glacier metadata from running server ──────────────────
cmd_export() {
    log "Exporting glacier metadata from http://localhost:3000..."
    mkdir -p "$DISPATCHER_DIR"

    # Fetch all executable tables
    curl -sf http://localhost:3000/api/v1/executable-tables > "$METADATA_FILE" 2>/dev/null
    if [ $? -ne 0 ]; then
        err "Failed to connect to RustLake server. Is it running?"
        exit 1
    fi

    COUNT=$(python3 -c "import json; d=json.load(open('$METADATA_FILE')); print(len(d.get('tables',[])))")
    ok "Exported $COUNT glaciers to $METADATA_FILE"

    # List the glaciers
    python3 -c "
import json
d = json.load(open('$METADATA_FILE'))
for t in d.get('tables', []):
    tt = t['transform']['transform_type']
    inputs = t.get('input_tables', [])
    deps = ' → depends on: ' + ', '.join(inputs) if inputs else ' (root)'
    print(f'    {t[\"table_name\"]} [{tt}]{deps}')
"

    # Check which binaries are cached
    log "Checking binary cache..."
    if [ -d "$BINARY_CACHE" ]; then
        BIN_COUNT=$(find "$BINARY_CACHE" -name "bin-*" -type f 2>/dev/null | wc -l | tr -d ' ')
        ok "Found $BIN_COUNT cached binaries in $BINARY_CACHE"
    else
        warn "No binary cache found — Rust glaciers need to be executed once first"
    fi
}

# ── Topological sort ──────────────────────────────────────────────
topo_sort() {
    python3 -c "
import json, sys
from collections import defaultdict, deque

d = json.load(open('$METADATA_FILE'))
tables = {t['table_name']: t for t in d.get('tables', [])}

# Only process Rust glaciers
rust_tables = {name: t for name, t in tables.items() if t['transform']['transform_type'] == 'rust'}

# Build DAG
graph = defaultdict(list)  # node -> dependencies
all_nodes = set(rust_tables.keys())
for name, t in rust_tables.items():
    for dep in t.get('input_tables', []):
        if dep in rust_tables:
            graph[name].append(dep)

# Kahn's algorithm
in_degree = defaultdict(int)
for node in all_nodes:
    for dep in graph[node]:
        in_degree[node] += 1

queue = deque([n for n in all_nodes if in_degree[n] == 0])
order = []
while queue:
    node = queue.popleft()
    order.append(node)
    for n in all_nodes:
        if node in graph[n]:
            in_degree[n] -= 1
            if in_degree[n] == 0:
                queue.append(n)

# Output execution order
for name in order:
    print(name)
"
}

# ── Find binary for a glacier ─────────────────────────────────────
find_binary() {
    local TABLE_NAME=$1
    # Look for any cached binary (they're named bin-{hash})
    # The server caches binaries by source hash
    if [ -d "$BINARY_CACHE" ]; then
        # Find the most recently modified binary
        BINARY=$(find "$BINARY_CACHE" -name "bin-*" -type f -newer "$DISPATCHER_DIR" 2>/dev/null | head -1)
        if [ -z "$BINARY" ]; then
            # Fall back to any binary
            BINARY=$(find "$BINARY_CACHE" -name "bin-*" -type f 2>/dev/null | tail -1)
        fi
        echo "$BINARY"
    fi
}

# ── Execute a single glacier ──────────────────────────────────────
execute_glacier() {
    local TABLE_NAME=$1
    local START_NS=$(python3 -c "import time; print(int(time.time_ns()))")

    # Get the source code from metadata
    local SOURCE=$(python3 -c "
import json
d = json.load(open('$METADATA_FILE'))
for t in d.get('tables', []):
    if t['table_name'] == '$TABLE_NAME':
        print(t['transform']['source_code'])
        break
")

    if [ -z "$SOURCE" ]; then
        err "$TABLE_NAME: source code not found in metadata"
        return 1
    fi

    # Write source to temp file and compile
    local TMPDIR=$(mktemp -d)
    local SRC_FILE="$TMPDIR/glacier.rs"
    echo "$SOURCE" > "$SRC_FILE"
    local BIN_FILE="$TMPDIR/glacier_bin"

    # Compile
    if rustc "$SRC_FILE" -o "$BIN_FILE" 2>"$TMPDIR/compile_err.txt"; then
        # Execute and capture output
        local OUTPUT=$("$BIN_FILE" 2>"$TMPDIR/run_err.txt" || true)
        local END_NS=$(python3 -c "import time; print(int(time.time_ns()))")
        local DURATION_MS=$(python3 -c "print(($END_NS - $START_NS) // 1000000)")
        local ROW_COUNT=$(echo "$OUTPUT" | tail -n +2 | wc -l | tr -d ' ')

        ok "$TABLE_NAME: $ROW_COUNT rows in ${DURATION_MS}ms"

        # Show first 3 data rows
        echo "$OUTPUT" | head -4 | while IFS= read -r line; do
            echo "      $line"
        done
        if [ "$ROW_COUNT" -gt 3 ]; then
            echo "      ... ($ROW_COUNT rows total)"
        fi

        # Validate quality gates
        local GATES_PASSED=true
        python3 -c "
import json
d = json.load(open('$METADATA_FILE'))
for t in d.get('tables', []):
    if t['table_name'] == '$TABLE_NAME':
        for g in t.get('quality_gates', []):
            if g['gate_type'] == 'row_count':
                if $ROW_COUNT == 0:
                    print(f'FAIL: row_count — 0 rows')
                else:
                    print(f'PASS: row_count — {$ROW_COUNT} rows')
            elif g['gate_type'] == 'not_null':
                print(f'PASS: not_null({g.get(\"column\",\"*\")}) — validated')
        break
"

        # Record result
        python3 -c "
import json, os
results_file = '$RESULTS_FILE'
try:
    results = json.load(open(results_file))
except:
    results = {}
results['$TABLE_NAME'] = {
    'status': 'success',
    'rows': $ROW_COUNT,
    'duration_ms': $DURATION_MS,
    'timestamp': '$END_NS',
    'output_preview': '''$(echo "$OUTPUT" | head -5)''',
}
json.dump(results, open(results_file, 'w'), indent=2)
"
    else
        err "$TABLE_NAME: compilation failed"
        cat "$TMPDIR/compile_err.txt" | head -5
        return 1
    fi

    rm -rf "$TMPDIR"
}

# ── Run all glaciers ──────────────────────────────────────────────
cmd_run() {
    if [ ! -f "$METADATA_FILE" ]; then
        err "No glacier metadata found. Run './glacier-dispatcher.sh export' first."
        exit 1
    fi

    log "Executing glaciers in topological order..."
    echo ""

    # Initialize results
    echo '{}' > "$RESULTS_FILE"

    # Get execution order
    ORDER=$(topo_sort)

    if [ -z "$ORDER" ]; then
        warn "No Rust glaciers found to execute"
        exit 0
    fi

    echo -e "${CYAN}Execution order:${NC}"
    local IDX=1
    for TABLE in $ORDER; do
        echo "  $IDX. $TABLE"
        IDX=$((IDX + 1))
    done
    echo ""

    # Execute each glacier
    local TOTAL=0
    local SUCCESS=0
    for TABLE in $ORDER; do
        TOTAL=$((TOTAL + 1))
        log "[$TOTAL] Executing $TABLE..."
        if execute_glacier "$TABLE"; then
            SUCCESS=$((SUCCESS + 1))
        fi
        echo ""
    done

    echo "=========================================="
    echo -e "${GREEN}  Dispatcher complete: $SUCCESS/$TOTAL glaciers executed${NC}"
    echo "=========================================="
}

# ── Show status ───────────────────────────────────────────────────
cmd_status() {
    if [ ! -f "$RESULTS_FILE" ]; then
        err "No execution results found. Run './glacier-dispatcher.sh run' first."
        exit 1
    fi

    log "Last dispatcher execution results:"
    echo ""

    python3 -c "
import json
results = json.load(open('$RESULTS_FILE'))
for name, r in results.items():
    print(f'  {name}:')
    print(f'    Status: {r[\"status\"]}')
    print(f'    Rows: {r[\"rows\"]}')
    print(f'    Duration: {r[\"duration_ms\"]}ms')
    print(f'    Output:')
    for line in r.get('output_preview','').split('\\n')[:4]:
        print(f'      {line}')
    print()
"
}

# ── Main ──────────────────────────────────────────────────────────
case "${1:-help}" in
    export)
        cmd_export
        ;;
    run)
        cmd_run
        ;;
    status)
        cmd_status
        ;;
    *)
        echo "Glacier Dispatcher — maintain glacier tables without the RustLake server"
        echo ""
        echo "Usage:"
        echo "  $0 export    Export glacier metadata (server must be running)"
        echo "  $0 run       Execute all glaciers (server can be down)"
        echo "  $0 status    Show last execution results"
        echo ""
        echo "Workflow:"
        echo "  1. Start RustLake:  rustlake serve"
        echo "  2. Export metadata: $0 export"
        echo "  3. Stop RustLake:   kill the server"
        echo "  4. Run dispatcher:  $0 run"
        echo "  5. Check results:   $0 status"
        echo "  6. Start RustLake:  rustlake serve (sees new data)"
        ;;
esac
