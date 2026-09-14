# YouTrackDB YCSB Benchmarks

A [YCSB](https://github.com/brianfrankcooper/YCSB) driver and workload suite for exercising YouTrackDB's **storage-level** I/O paths (read, update, insert, scan, delete) via simple parameterized YQL statements. This module is intentionally distinct from [`jmh-ldbc`](../jmh-ldbc/README.md), which measures graph-traversal performance over the MATCH query engine — `ycsb` targets the underlying page cache, WAL, and index paths instead.

The driver runs YouTrackDB in embedded mode (`YourTracks.instance`) — no server needs to be started separately.

## Workloads

This module ships **five bespoke workloads**: `B`, `C`, `E`, `W`, `I`. Names `B`/`C`/`E` match upstream YCSB conventions, while `W` and `I` are YouTrackDB-specific profiles. Workloads `A`, `D`, and `F` from upstream YCSB are **not** included.

| Workload | Read % | Update % | Insert % | Scan % | Distribution | Notes |
|---|---|---|---|---|---|---|
| B | 95 | 5 | 0 | 0 | zipfian | Read-mostly — occasional updates |
| C | 100 | 0 | 0 | 0 | zipfian | Read-only — pure read throughput/latency |
| E | 0 | 0 | 5 | 95 | zipfian (insert key) / uniform (scan length) | Scan-heavy — `maxscanlength=100`, `scanlengthdistribution=uniform` |
| W | 50 | 50 | 0 | 0 | zipfian | 50% read / 50% update (balanced read/write) |
| I | 20 | 0 | 80 | 0 | zipfian | Insert-burst — exercises insert path and index maintenance |

All workloads share the parameters in [`workloads/workload-common.properties`](workloads/workload-common.properties):

- `recordcount=3500000` (~3.5M records, ~3.5 GB on disk with indexes — approximate)
- `operationcount=1000000` (1M ops per run)
- `fieldcount=10`, `fieldlength=100` (standard YCSB schema — 10 string fields × 100 bytes)
- `table=usertable`
- `requestdistribution=zipfian`
- `measurementtype=hdrhistogram`
- `hdrhistogram.percentiles=50,95,99,99.9`

Override any of these from the CLI with `-p <key>=<value>` (or via `run-ycsb.sh` flags for the common ones).

> Note: all four configured percentiles (including p99.9) are present in the raw pass-2 text output; only p50/p95/p99 are rolled up into `summary.json`.

## Two-pass execution model

For each workload, `run-ycsb.sh` runs **two passes**, restoring the database from a snapshot between them:

1. **Pass 1 — max throughput** (`-target 0`). Measures peak capacity. The latency numbers from this pass suffer from coordinated omission and should not be reported.
2. **Pass 2 — fixed throughput** at `floor(pass1_throughput × --throughput-ratio)` (default ratio `0.8`) with `measurement.interval=intended`. This enables YCSB's coordinated-omission correction: latency is measured against the **intended** operation start time, so slow operations inflate the tail instead of pushing subsequent operations out.

> **Fallback**: if pass 1 cannot parse a positive throughput, the script logs an error and **skips pass 2** for that workload; the summary reports `-1` for its latency percentiles.

## Prerequisites

- **JDK 21+**
- **~7 GB free disk** for the default dataset (~3.5 GB DB + same-size snapshot; approximate).
- **Bash 4+** — `run-ycsb.sh` uses `set -euo pipefail`, `BASH_SOURCE`, and arrays.
- No external services — YouTrackDB runs embedded inside the benchmark JVM.

## Quick Start

Smoke-test with a tiny dataset (seconds end-to-end):

```bash
./ycsb/run-ycsb.sh --record-count 1000 --operation-count 500 --workloads B,C
```

Full default run:

```bash
./ycsb/run-ycsb.sh
```

## Running benchmarks

There are three ways to run the benchmark. `run-ycsb.sh` is the recommended path.

### 1. `run-ycsb.sh` (recommended)

The runner script handles the full lifecycle: builds the uber-jar, loads data, snapshots the DB, runs both passes per workload with snapshot-restore between them, and collects results.

Run `./ycsb/run-ycsb.sh --help` to see the complete flag list. Defaults:

| Flag | Default | Description |
|---|---|---|
| `--workloads` | `B,C,E,W,I` | Comma-separated workload names. Each must have a matching `workloads/workload-<name>.properties`. |
| `--record-count` | *(from `workload-common.properties`: 3500000)* | Records to load. Overrides the property file. |
| `--operation-count` | *(from `workload-common.properties`: 1000000)* | Operations per workload run. Overrides the property file. |
| `--threads` | *host CPU count* (`nproc`) | Client thread count for load and run phases. |
| `--heap` | `4g` | JVM heap size (applied as `-Xms` and `-Xmx`). |
| `--throughput-ratio` | `0.8` | Pass 2 target = `max × R`; must be in `(0, 1.0]`. Non-numeric or out-of-range values abort the script. |
| `--db-path` | `$RESULTS_DIR/ytdb-data` | Database directory. The snapshot is always placed at `<db-path>-snapshot`. |
| `--results-dir` | `./ycsb-results` | Output directory (created if missing). Also holds the DB + snapshot by default. |
| `--skip-build` | *off* | Skip the Maven build. The uber-jar must already exist at `ycsb/target/youtrackdb-ycsb-*.jar`. |
| `--skip-load` | *off* | Skip the data-load phase. A snapshot must already exist at `<db-path>-snapshot`. Note that every workload pass still restores `$DB_PATH` from the snapshot — the snapshot is the source of truth, and whatever lives at `$DB_PATH` when the script starts is overwritten. |
| `--help` | — | Show usage. |

Full-run example:

```bash
./ycsb/run-ycsb.sh --threads 8 --heap 8g --throughput-ratio 0.7 --results-dir /data/ycsb-results
```

> **Warning**: do not run two instances concurrently unless **both** `--results-dir` *and* `--db-path` are distinct between them. The per-pass `restore_snapshot` step `rm -rf`s `$DB_PATH` and re-copies from the snapshot; if two runs share either path, one will corrupt the other. Note that by default `--db-path` lives inside `--results-dir` (`$RESULTS_DIR/ytdb-data`), so distinct `--results-dir` values alone are sufficient — but any explicit `--db-path` override must also be distinct.

### 2. Full repo build

The module is registered in the parent POM with no profile guard, so a standard full build compiles and packages `ycsb` along with everything else:

```bash
./mvnw clean package -DskipTests
```

This produces `ycsb/target/youtrackdb-ycsb-0.5.0-SNAPSHOT.jar` (the name follows `${revision}${sha1}${changelist}` from the root POM).

### 3. Manual uber-jar invocation

For CI hand-wiring or running outside the script, build the uber-jar directly:

```bash
./mvnw -pl ycsb -am package -DskipTests
```

Then launch it with the required JVM flags. The uber-jar main class is `com.jetbrains.youtrackdb.ycsb.Client`, the workload class is `com.jetbrains.youtrackdb.ycsb.workloads.CoreWorkload`, and the driver class is `com.jetbrains.youtrackdb.ycsb.binding.YouTrackDBYqlClient`. All three are already referenced in `workloads/workload-common.properties`.

Run the commands below **from the repo root**. The `-jar` argument is a shell glob that must match exactly one file; the build produces a single shaded jar named `youtrackdb-ycsb-<version>.jar` alongside an `original-*.jar` (filtered out by the glob below). If you have also built sources or javadoc jars, substitute the full versioned filename.

```bash
# Required --add-opens flags.
# Keep in sync with the JVM_ADD_OPENS array in ycsb/run-ycsb.sh and the
# <argLine> in ycsb/pom.xml. All flags below are required — omitting any of
# them causes InaccessibleObjectException at startup.
ADD_OPENS=(
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED
  --add-opens=java.base/sun.security.x509=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
)

# Load phase — leaves ytdb.newdb at its default `true` (fresh DB).
# NOTE: without a `-p recordcount=N` override, this loads the 3.5M-record
# default from workload-common.properties. Set `-p recordcount=...` and
# `-p operationcount=...` to match the scale you want.
java "${ADD_OPENS[@]}" -Xms4g -Xmx4g \
  -jar ycsb/target/youtrackdb-ycsb-*.jar \
  -load \
  -P ycsb/workloads/workload-common.properties \
  -P ycsb/workloads/workload-B.properties \
  -threads 8 \
  -p ytdb.url=./my-db \
  -p recordcount=100000

# Run phase — MUST pass ytdb.newdb=false, otherwise the driver drops and
# recreates the DB on init, destroying the loaded data.
java "${ADD_OPENS[@]}" -Xms4g -Xmx4g \
  -jar ycsb/target/youtrackdb-ycsb-*.jar \
  -t \
  -P ycsb/workloads/workload-common.properties \
  -P ycsb/workloads/workload-B.properties \
  -threads 8 \
  -p ytdb.url=./my-db \
  -p ytdb.newdb=false \
  -p operationcount=10000
```

## Configuration

The driver reads six properties, all prefixed `ytdb.`. Pass them with `-p <key>=<value>` on the YCSB command line.

| Property | Default | Description |
|---|---|---|
| `ytdb.url` | `./target/ycsb-db` | Path to the embedded database directory. **Not** `ytdb.path`. |
| `ytdb.dbname` | `ycsb` | Database name inside the YouTrackDB instance. |
| `ytdb.user` | `admin` | Database user. |
| `ytdb.password` | `admin` | Database password. |
| `ytdb.newdb` | `true` | If `true`, drop the database (when it exists) and recreate it in `init()`. Load uses the default; **every run invocation must pass `-p ytdb.newdb=false`** or the snapshot-restored DB is destroyed. |
| `ytdb.dbtype` | `DISK` | `DISK` or `MEMORY` (passed to `YouTrackDB.create(...)`). |

## Output and results

Each invocation of `run-ycsb.sh` writes to `$RESULTS_DIR`:

- **Per-workload raw output**:
  - `workload-<w>-pass1.txt` — pass-1 output (max throughput).
  - `workload-<w>-pass2.txt` — pass-2 output (fixed throughput with coordinated-omission correction).

  Each file is the full YCSB text report as captured via `tee` from the benchmark JVM's stdout. It contains `[OVERALL]`, per-operation sections (`[READ]`, `[UPDATE]`, `[SCAN]`, `[INSERT]`, ...) with throughput, latency distribution, and return-code counts. `summary.json` is the machine-readable view derived from these files.

- **Stdout summary table** (printed after all workloads complete):

  ```
  Workload   MaxThroughput    P50(us)    P95(us)    P99(us)       Op
  B                12345.6         87        150        220     READ
  ...
  ```

  Columns: max throughput from pass 1, then p50/p95/p99 latency for the **primary operation** from pass 2.

- **`summary.json`** — a JSON array, one object per workload, with the same fields as the stdout table.

  ```json
  [
    {
      "workload": "B",
      "primaryOperation": "READ",
      "maxThroughputOpsPerSec": 12345.6,
      "p50LatencyUs": 87,
      "p95LatencyUs": 150,
      "p99LatencyUs": 220
    },
    {
      "workload": "E",
      "primaryOperation": "SCAN",
      "maxThroughputOpsPerSec": 210.4,
      "p50LatencyUs": 3800,
      "p95LatencyUs": 9200,
      "p99LatencyUs": 14000
    }
  ]
  ```

  Fields:

  | Field | Type | Description |
  |---|---|---|
  | `workload` | string | Workload name (`B`, `C`, `E`, `W`, `I`). |
  | `primaryOperation` | string | Operation the latency columns describe: `E`→`SCAN`, `I`→`INSERT`, everything else→`READ`. |
  | `maxThroughputOpsPerSec` | number | Pass-1 overall throughput. `-1` when pass 1 failed or produced non-positive throughput. |
  | `p50LatencyUs` / `p95LatencyUs` / `p99LatencyUs` | number | Pass-2 latency percentiles for the primary operation, in microseconds. `-1` when pass 2 was skipped or the value could not be parsed. |

  `-1` is the sentinel for "not available" across all numeric fields — keep this in mind when post-processing.

### Output directory layout

By default `$RESULTS_DIR` holds **both** the result text/JSON files **and** the database + snapshot. On a default run, that is roughly 3.5 GB for the DB plus another 3.5 GB for the snapshot in addition to the small text files — budget ~7 GB total. Pass `--db-path` to relocate the database (and snapshot) independently of the results directory; the snapshot always sits next to the DB at `<db-path>-snapshot`.
