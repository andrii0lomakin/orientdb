#!/usr/bin/env bash
#
# run-ycsb.sh — YCSB benchmark runner for YouTrackDB
#
# Automates the full benchmark lifecycle: build, load data, snapshot,
# run each selected workload in two passes (max throughput + fixed
# throughput for latency distribution), and collect results.
#
# Quick validation (small dataset, two workloads):
#   ./ycsb/run-ycsb.sh --record-count 1000 --operation-count 500 --workloads B,C
#
# Full benchmark run (defaults: 3.5M records, 1M ops, all workloads):
#   ./ycsb/run-ycsb.sh
#
set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# JVM --add-opens flags required by YouTrackDB.
# Keep in sync with ycsb/pom.xml <argLine>.
JVM_ADD_OPENS=(
  "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED"
  "--add-opens=java.base/sun.security.x509=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  "--add-opens=java.base/java.util=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
  "--add-opens=java.base/java.net=ALL-UNNAMED"
)

ALL_WORKLOADS="B,C,E,W,I"
DEFAULT_HEAP="4g"

# ============================================================================
# Defaults
# ============================================================================

WORKLOADS="$ALL_WORKLOADS"
RECORD_COUNT=""
OPERATION_COUNT=""
THREADS=""
THROUGHPUT_RATIO="0.8"
DB_PATH=""
RESULTS_DIR=""
HEAP_SIZE="$DEFAULT_HEAP"
SKIP_BUILD=false
SKIP_LOAD=false

# ============================================================================
# Functions
# ============================================================================

usage() {
  cat <<USAGE
Usage: $(basename "$0") [OPTIONS]

Options:
  --workloads W1,W2,...   Workloads to run (default: $ALL_WORKLOADS)
  --record-count N        Number of records to load (overrides property file)
  --operation-count N     Operations per workload run (overrides property file)
  --threads N             Thread count (default: number of CPUs)
  --heap SIZE             JVM heap size (default: $DEFAULT_HEAP)
  --throughput-ratio R    Pass 2 target = max_throughput * R (default: 0.8)
  --db-path PATH          Database directory (default: \$RESULTS_DIR/ytdb-data)
  --results-dir PATH      Results output directory (default: ./ycsb-results)
  --skip-build            Skip Maven build
  --skip-load             Skip data loading (assumes snapshot exists)
  --help                  Show this help message

Workload names: B (read-mostly), C (read-only), E (scan),
                W (write-heavy), I (insert-burst)

Examples:
  # Quick validation
  $(basename "$0") --record-count 1000 --operation-count 500 --workloads B,C

  # Full run with defaults
  $(basename "$0")

  # Custom thread count and results directory
  $(basename "$0") --threads 8 --results-dir /data/ycsb-results
USAGE
}

# Detect CPU count (works on Linux and macOS)
detect_cpu_count() {
  if command -v nproc &>/dev/null; then
    nproc
  elif [ -f /proc/cpuinfo ]; then
    grep -c '^processor' /proc/cpuinfo
  elif command -v sysctl &>/dev/null; then
    sysctl -n hw.ncpu
  else
    echo 4
  fi
}

log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

log_error() {
  echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2
}

# Require that an option has a non-flag value following it.
require_arg() {
  if [ $# -lt 2 ] || [[ "$2" == --* ]]; then
    log_error "Option $1 requires a value"
    usage
    exit 1
  fi
}

# Require that a value is a positive integer.
require_positive_int() {
  local name="$1" value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    log_error "$name must be a positive integer, got: $value"
    exit 1
  fi
}

# Resolve the uber-jar path from the Maven build output.
resolve_uber_jar() {
  local jar
  jar=$(find "$SCRIPT_DIR/target" -maxdepth 1 -name 'youtrackdb-ycsb-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' ! -name 'original-*' \
    -print 2>/dev/null | head -n 1)
  if [ -z "$jar" ]; then
    log_error "Uber-jar not found in $SCRIPT_DIR/target/"
    log_error "Run without --skip-build or build manually first."
    exit 1
  fi
  echo "$jar"
}

# Run a YCSB command via the uber-jar.
#
# Usage: run_ycsb <mode> [extra_args...]
#   mode: "load" or "run"
#   extra_args: additional -P, -p, -target, etc. arguments
#
# Output is written to stdout and captured in $YCSB_OUTPUT_FILE if set.
run_ycsb() {
  local mode="$1"
  shift

  # Translate mode to YCSB CLI flag: -load or -t (transaction/run)
  local flag
  case "$mode" in
    load) flag="-load" ;;
    run)  flag="-t" ;;
    *)    log_error "Unknown YCSB mode: $mode"; return 1 ;;
  esac

  local jar
  jar=$(resolve_uber_jar)

  local cmd=(
    java
    "-Xms${HEAP_SIZE}" "-Xmx${HEAP_SIZE}"
    "${JVM_ADD_OPENS[@]}"
    -jar "$jar"
    "$flag"
    -threads "$THREADS"
  )

  # Add common properties
  cmd+=(-P "$SCRIPT_DIR/workloads/workload-common.properties")

  # Append any extra arguments
  cmd+=("$@")

  # Override record/operation counts if specified on command line
  if [ -n "$RECORD_COUNT" ]; then
    cmd+=(-p "recordcount=$RECORD_COUNT")
  fi
  if [ -n "$OPERATION_COUNT" ]; then
    cmd+=(-p "operationcount=$OPERATION_COUNT")
  fi

  log "Running: ${cmd[*]}"

  if [ -n "${YCSB_OUTPUT_FILE:-}" ]; then
    "${cmd[@]}" 2>&1 | tee "$YCSB_OUTPUT_FILE"
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
      log_error "YCSB $mode failed (exit code: $rc)"
      return 1
    fi
  else
    if ! "${cmd[@]}" 2>&1; then
      log_error "YCSB $mode failed"
      return 1
    fi
  fi
}

# Signal handler — print recovery instructions on interrupt.
cleanup_on_signal() {
  echo ""
  log_error "Interrupted. To resume:"
  log_error "  - If loading was interrupted: re-run without --skip-load"
  log_error "  - If a workload was interrupted: re-run with --skip-load --skip-build"
  log_error "  - Snapshot location: ${DB_PATH:-<not yet configured>}-snapshot"
  exit 130
}

trap cleanup_on_signal INT TERM

# ============================================================================
# Argument parsing
# ============================================================================

while [ $# -gt 0 ]; do
  case "$1" in
    --workloads)
      require_arg "$@"
      WORKLOADS="$2"
      shift 2
      ;;
    --record-count)
      require_arg "$@"
      RECORD_COUNT="$2"
      shift 2
      ;;
    --operation-count)
      require_arg "$@"
      OPERATION_COUNT="$2"
      shift 2
      ;;
    --threads)
      require_arg "$@"
      THREADS="$2"
      shift 2
      ;;
    --heap)
      require_arg "$@"
      HEAP_SIZE="$2"
      shift 2
      ;;
    --throughput-ratio)
      require_arg "$@"
      THROUGHPUT_RATIO="$2"
      shift 2
      ;;
    --db-path)
      require_arg "$@"
      DB_PATH="$2"
      shift 2
      ;;
    --results-dir)
      require_arg "$@"
      RESULTS_DIR="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --skip-load)
      SKIP_LOAD=true
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

# Apply defaults after argument parsing
THREADS="${THREADS:-$(detect_cpu_count)}"
RESULTS_DIR="${RESULTS_DIR:-./ycsb-results}"
DB_PATH="${DB_PATH:-$RESULTS_DIR/ytdb-data}"
DB_PATH="${DB_PATH%/}"  # Strip trailing slash to prevent snapshot path corruption

# ============================================================================
# Validation
# ============================================================================

require_positive_int "--threads" "$THREADS"
[ -n "$RECORD_COUNT" ] && require_positive_int "--record-count" "$RECORD_COUNT"
[ -n "$OPERATION_COUNT" ] && require_positive_int "--operation-count" "$OPERATION_COUNT"

if [ -z "$RESULTS_DIR" ]; then
  log_error "--results-dir must not be empty"
  exit 1
fi

if [ -z "$DB_PATH" ]; then
  log_error "--db-path must not be empty"
  exit 1
fi

# Validate throughput ratio is a positive number in (0, 1.0]
if ! LC_NUMERIC=C awk -v r="$THROUGHPUT_RATIO" \
  'BEGIN { exit !(r > 0 && r <= 1.0) }' 2>/dev/null; then
  log_error "--throughput-ratio must be a number in (0, 1.0], got: $THROUGHPUT_RATIO"
  exit 1
fi

# Validate workload names against available property files
IFS=',' read -ra WORKLOAD_LIST <<< "$WORKLOADS"
for w in "${WORKLOAD_LIST[@]}"; do
  if [ ! -f "$SCRIPT_DIR/workloads/workload-$w.properties" ]; then
    log_error "Unknown workload: $w (valid: $ALL_WORKLOADS)"
    exit 1
  fi
done

# ============================================================================
# Build
# ============================================================================

if [ "$SKIP_BUILD" = false ]; then
  log "Building ycsb module..."
  # Skip Spotless: the benchmark runner packages a jar, it does not gate
  # code style. Spotless also depends on the Equo P2 offline cache, which
  # can fail with transient NoSuchFileException on the bundle-pool jars.
  (cd "$REPO_ROOT" && ./mvnw -pl ycsb -am package \
    -DskipTests -Dspotless.check.skip=true -q)
  log "Build complete."
else
  log "Skipping build (--skip-build)."
fi

# Verify uber-jar exists
resolve_uber_jar >/dev/null

log "Configuration:"
log "  Workloads:        $WORKLOADS"
log "  Record count:     ${RECORD_COUNT:-<from property file>}"
log "  Operation count:  ${OPERATION_COUNT:-<from property file>}"
log "  Threads:          $THREADS"
log "  Heap size:        $HEAP_SIZE"
log "  Throughput ratio: $THROUGHPUT_RATIO"
log "  DB path:          $DB_PATH"
log "  Snapshot path:    ${DB_PATH}-snapshot"
log "  Results dir:      $RESULTS_DIR"

# Create results directory
mkdir -p "$RESULTS_DIR"

# ============================================================================
# Load phase
# ============================================================================

SNAPSHOT_PATH="${DB_PATH}-snapshot"

if [ "$SKIP_LOAD" = false ]; then
  # Remove any existing database, snapshot, and stale temp snapshots
  if [ -d "$DB_PATH" ]; then
    log "Removing existing database at $DB_PATH"
    rm -rf "$DB_PATH"
  fi
  if [ -d "$SNAPSHOT_PATH" ]; then
    log "Removing existing snapshot at $SNAPSHOT_PATH"
    rm -rf "$SNAPSHOT_PATH"
  fi
  rm -rf "${SNAPSHOT_PATH}.tmp."* 2>/dev/null || true

  log "Loading data (ytdb.newdb=true)..."
  LOAD_START=$(date +%s)

  run_ycsb load \
    -p "ytdb.url=$DB_PATH" \
    -p "ytdb.newdb=true"

  LOAD_END=$(date +%s)
  LOAD_DURATION=$((LOAD_END - LOAD_START))
  log "Load complete in ${LOAD_DURATION}s."

  # Verify database was created at the expected path
  if [ ! -d "$DB_PATH" ]; then
    log_error "Database directory not found after load: $DB_PATH"
    log_error "Check that ytdb.url property matches --db-path."
    exit 1
  fi

  # Snapshot the database directory for reproducible workload runs.
  # The YCSB load phase closes the database (cleanup), which flushes WAL,
  # so the directory is in a consistent state.
  # Use atomic copy-then-rename to prevent partial snapshots from surviving
  # interrupted runs.
  SNAPSHOT_TMP="${SNAPSHOT_PATH}.tmp.$$"
  log "Creating snapshot at $SNAPSHOT_PATH"
  rm -rf "$SNAPSHOT_TMP"
  cp -a "$DB_PATH" "$SNAPSHOT_TMP"
  mv "$SNAPSHOT_TMP" "$SNAPSHOT_PATH"
  log "Snapshot created."
else
  log "Skipping load phase (--skip-load)."
  if [ ! -d "$SNAPSHOT_PATH" ]; then
    log_error "Snapshot not found at $SNAPSHOT_PATH"
    log_error "Run without --skip-load to create a snapshot first."
    exit 1
  fi
fi

# ============================================================================
# Workload execution (two-pass per workload)
# ============================================================================

# Restore the database from snapshot to ensure a clean starting state.
restore_snapshot() {
  log "Restoring database from snapshot..."
  rm -rf "$DB_PATH"
  cp -a "$SNAPSHOT_PATH" "$DB_PATH"
}

# Parse the overall throughput (ops/sec) from YCSB output.
# Returns the throughput as a string, or empty if not found.
parse_throughput() {
  local output_file="$1"
  awk -F', ' '/\[OVERALL\], Throughput\(ops\/sec\)/ { print $3 }' "$output_file"
}

for w in "${WORKLOAD_LIST[@]}"; do
  log "========================================"
  log "Workload $w"
  log "========================================"

  WORKLOAD_PROPS="$SCRIPT_DIR/workloads/workload-$w.properties"
  PASS1_OUTPUT="$RESULTS_DIR/workload-$w-pass1.txt"
  PASS2_OUTPUT="$RESULTS_DIR/workload-$w-pass2.txt"

  # --- Pass 1: Max throughput ---
  restore_snapshot

  log "Pass 1 (max throughput) — workload $w"
  if ! YCSB_OUTPUT_FILE="$PASS1_OUTPUT" \
    run_ycsb run \
      -P "$WORKLOAD_PROPS" \
      -p "ytdb.url=$DB_PATH" \
      -p "ytdb.newdb=false" \
      -target 0; then
    log_error "Pass 1 failed for workload $w. Skipping."
    continue
  fi

  THROUGHPUT=$(parse_throughput "$PASS1_OUTPUT")
  if [ -z "$THROUGHPUT" ] || \
     ! LC_NUMERIC=C awk -v tp="$THROUGHPUT" 'BEGIN { exit !(tp > 0) }' 2>/dev/null; then
    log_error "Could not parse a positive throughput from pass 1 output."
    log_error "Skipping pass 2 for workload $w."
    continue
  fi
  log "Pass 1 throughput: $THROUGHPUT ops/sec"

  # --- Pass 2: Fixed throughput for latency distribution ---
  # measurement.interval=intended enables coordinated-omission correction,
  # accounting for queuing delay when operations cannot keep up with the
  # target rate.
  TARGET=$(LC_NUMERIC=C awk -v tp="$THROUGHPUT" -v ratio="$THROUGHPUT_RATIO" \
    'BEGIN { printf "%d", tp * ratio }')
  if [ -z "$TARGET" ] || [ "$TARGET" -le 0 ] 2>/dev/null; then
    log_error "Computed target throughput is 0 or invalid. Skipping pass 2 for workload $w."
    continue
  fi
  log "Pass 2 target: $TARGET ops/sec (${THROUGHPUT_RATIO} × max)"

  restore_snapshot

  log "Pass 2 (fixed throughput) — workload $w"
  if ! YCSB_OUTPUT_FILE="$PASS2_OUTPUT" \
    run_ycsb run \
      -P "$WORKLOAD_PROPS" \
      -p "ytdb.url=$DB_PATH" \
      -p "ytdb.newdb=false" \
      -p "measurement.interval=intended" \
      -target "$TARGET"; then
    log_error "Pass 2 failed for workload $w."
    continue
  fi

  log "Workload $w complete."
done

# ============================================================================
# Results summary
# ============================================================================

# Parse a latency percentile from YCSB output.
# Usage: parse_latency <output_file> <operation> <percentile_label>
#   e.g., parse_latency file.txt READ "50thPercentileLatency(us)"
# Returns the value, or empty string if not found.
parse_latency() {
  local output_file="$1" op="$2" label="$3"
  awk -F', ' -v op="$op" -v label="$label" \
    '$1 == "[" op "]" && $2 == label { print $3 }' "$output_file" 2>/dev/null
}

# Determine the primary operation for latency reporting per workload.
# For mixed workloads (B, C, W), report READ latency since it is always
# present and represents the user-facing operation path.
primary_operation() {
  local workload="$1"
  case "$workload" in
    E) echo "SCAN" ;;
    I) echo "INSERT" ;;
    *) echo "READ" ;;
  esac
}

log ""
log "========================================"
log "Summary"
log "========================================"

# Print header
printf "%-10s %15s %10s %10s %10s %8s\n" \
  "Workload" "MaxThroughput" "P50(us)" "P95(us)" "P99(us)" "Op"

# Collect summary data for JSON output
JSON_ENTRIES=()

for w in "${WORKLOAD_LIST[@]}"; do
  PASS1_FILE="$RESULTS_DIR/workload-$w-pass1.txt"
  PASS2_FILE="$RESULTS_DIR/workload-$w-pass2.txt"

  # Max throughput from pass 1 (empty string = not available)
  if [ -f "$PASS1_FILE" ]; then
    MAX_THROUGHPUT=$(parse_throughput "$PASS1_FILE")
  else
    MAX_THROUGHPUT=""
  fi

  # Latency from pass 2 (primary operation depends on workload type)
  OP=$(primary_operation "$w")
  if [ -f "$PASS2_FILE" ]; then
    P50=$(parse_latency "$PASS2_FILE" "$OP" "50thPercentileLatency(us)")
    P95=$(parse_latency "$PASS2_FILE" "$OP" "95thPercentileLatency(us)")
    P99=$(parse_latency "$PASS2_FILE" "$OP" "99thPercentileLatency(us)")
  else
    P50=""; P95=""; P99=""
  fi

  printf "%-10s %15s %10s %10s %10s %8s\n" \
    "$w" "${MAX_THROUGHPUT:--}" "${P50:--}" "${P95:--}" "${P99:--}" "$OP"

  # Build JSON entry
  JSON_ENTRIES+=("$(cat <<JSON
  {
    "workload": "$w",
    "primaryOperation": "$OP",
    "maxThroughputOpsPerSec": ${MAX_THROUGHPUT:--1},
    "p50LatencyUs": ${P50:--1},
    "p95LatencyUs": ${P95:--1},
    "p99LatencyUs": ${P99:--1}
  }
JSON
  )")
done

# Write JSON summary
SUMMARY_JSON="$RESULTS_DIR/summary.json"
{
  echo "["
  for i in "${!JSON_ENTRIES[@]}"; do
    if [ "$i" -lt $((${#JSON_ENTRIES[@]} - 1)) ]; then
      echo "${JSON_ENTRIES[$i]},"
    else
      echo "${JSON_ENTRIES[$i]}"
    fi
  done
  echo "]"
} > "$SUMMARY_JSON"

log ""
log "Results saved to: $RESULTS_DIR/"
log "JSON summary: $SUMMARY_JSON"
log "Done."
