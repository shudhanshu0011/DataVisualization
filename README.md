# GLEIF LEI Master Pipeline

Production-grade PySpark + Python pipeline that builds and maintains a **Slowly Changing Dimension Type-2 (SCD2)** master dataset of Legal Entity Identifiers (LEI) from the [GLEIF Golden Copy](https://www.gleif.org/en/lei-data/gleif-golden-copy) data.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [File Structure](#file-structure)
- [Pipeline Flows](#pipeline-flows)
  - [Day 1 — Full Load](#day-1--full-load-full_loadpy)
  - [Day 2+ — Delta Merge (Spark)](#day-2--delta-merge-spark-delta_mergepy)
  - [Day 2+ — Delta Merge (Streaming)](#day-2--delta-merge-streaming-delta_merge_backuppy)
- [Shared Utilities — pipeline_utils.py](#shared-utilities--pipeline_utilspy)
- [Configuration](#configuration)
  - [config.json](#configjson)
  - [schema.json](#schemajson)
- [Production Features](#production-features)
  - [Pre-flight Checks](#pre-flight-checks)
  - [Gap Detection & Recovery](#gap-detection--recovery)
  - [Idempotency Guard](#idempotency-guard)
  - [Schema Validation](#schema-validation)
  - [Atomic S3 Uploads](#atomic-s3-uploads)
  - [Retry with Back-off](#retry-with-back-off)
  - [Post-flight Validation](#post-flight-validation)
  - [Notifications (AWS SNS)](#notifications-aws-sns)
  - [GLEIF UTC Date Alignment](#gleif-utc-date-alignment)
- [S3 Folder Layout](#s3-folder-layout)
- [SCD Type-2 Logic](#scd-type-2-logic)
- [How to Run](#how-to-run)
- [Prerequisites](#prerequisites)

---

## Architecture Overview

```
┌───────────────────────────────────────────────────────────┐
│                     GLEIF Golden Copy                      │
│            (L1 = Entity data, L2 = Relationship data)     │
└────────────────────────┬──────────────────────────────────┘
                         │  Day 1: ZIP from S3 manual_drop
                         │  Day 2+: GLEIF delta API
                         ▼
┌───────────────────────────────────────────────────────────┐
│               pipeline_utils.py                           │
│  Config · S3 · Retry · CSV · Preflight · Notifications    │
└────────────────────────┬──────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌────────────┐ ┌────────────┐ ┌────────────────────┐
   │ full_load  │ │delta_merge │ │delta_merge_backup   │
   │  (Day 1)   │ │ (Day 2+)  │ │(Day 2+ / Streaming) │
   └─────┬──────┘ └─────┬──────┘ └─────┬──────────────┘
         │               │               │
         ▼               ▼               ▼
┌───────────────────────────────────────────────────────────┐
│                      AWS S3                               │
│  gleif/raw/{date}/l1/        ← Bronze (raw archives)     │
│  gleif/raw/{date}/l2/                                     │
│  gleif/processed/{date}/     ← Silver (master CSV)        │
└───────────────────────────────────────────────────────────┘
```

---

## File Structure

| File | Purpose |
|---|---|
| **pipeline_utils.py** | Single shared-utilities module: config loading, S3 helpers, GLEIF date functions, retry decorator, CSV merge, pre-flight checks, post-flight validation, SNS notifications |
| **full_load.py** | Day 1 initial load — downloads L1+L2 ZIPs from S3, parses with Spark, builds master with SCD2 metadata, uploads to S3 |
| **delta_merge.py** | Day 2+ delta merge — downloads delta from GLEIF API, performs SCD2 merge using Spark for the full merge |
| **delta_merge_backup.py** | Day 2+ delta merge (memory-optimized) — same logic but uses pure-Python CSV streaming for the SCD2 merge, freeing Spark's JVM heap before the heaviest phase |
| **config.json** | AWS credentials, S3 paths, GLEIF API URLs, pipeline behavior settings, notification config |
| **schema.json** | Column mappings (L1/L2), relationship pivot config, derived columns, change-tracking flags, final output column order |

---

## Pipeline Flows

### Day 1 — Full Load (`full_load.py`)

Used for initial setup or to reset the master from a fresh GLEIF golden copy.

```
Phase 0   Pre-flight checks (S3 connectivity, config validation)
Phase 1   Download L1.zip + L2.zip from s3://bucket/gleif/manual_drop/
          ├── Extract CSV from ZIP
          ├── Extract data_date from GLEIF filename (e.g. 20260403-gleif-...)
          └── Schema validation against schema.json
Phase 2   Archive raw CSVs to bronze: s3://bucket/gleif/raw/{date}/l1|l2/
Phase 3   Spark parse & schema alignment (rename cols, add missing as null)
Phase 4   Build master:
          ├── Join L1 + L2 on LEI
          ├── Derive parent names via self-join lookup
          └── Compute record_hash (MD5 of tracked columns)
Phase 5   Write master CSV with SCD2 metadata:
          ├── effective_start_date, effective_end_date, is_current, pipeline_run_date
          ├── Column ordering per schema.json final_output_columns
          └── Atomic upload to s3://bucket/gleif/processed/{date}/lei_master.csv
Phase 6   Post-flight validation (file exists, non-empty, columns match, LEI present)
```

### Day 2+ — Delta Merge, Spark (`delta_merge.py`)

Full Spark-based SCD2 merge. Suitable when Spark memory is sufficient to hold both the existing master and the delta in memory.

```
Phase 0    Pre-flight checks (S3, GLEIF API health, previous day master)
Phase 1    Download delta L1+L2 from GLEIF API (with retry)
           ├── Extract data_date from GLEIF filename
           └── Schema validation
Phase 1b   Gap detection (compare last master date vs. data_date)
Phase 1c   Idempotency check (skip if data_date already processed)
Phase 1d   Schema validation on downloaded CSVs
Phase 2    Archive raw delta to bronze
Phase 3    Spark parse & build incoming DataFrame
Phase 4    SCD2 merge (all in Spark):
           ├── Step 1: Download latest master from S3 into Spark
           ├── Step 2: Split into current vs. historical
           ├── Step 3: Align incoming columns to match master
           ├── Step 4: Classify LEIs (brand new / matched / untouched)
           ├── Step 5: Hash comparison → changed vs. unchanged
           ├── Step 6: Build segments (close old, new versions, brand new, carry forward)
           ├── Step 7: Union all segments
           └── Step 8: Write merged master → atomic S3 upload
Phase 5    Post-flight validation
```

### Day 2+ — Delta Merge, Streaming (`delta_merge_backup.py`)

Memory-optimized variant. Uses Spark only for parsing the small delta, then switches to pure-Python CSV streaming for the SCD2 merge against the (potentially large) master.

```
Phases 0–3   Same as delta_merge.py
Phase 4      Materialize delta to local CSV + collect LEI→hash lookup dict
Phase 5      Stop Spark (frees ~2 GB JVM heap)
Phase 6      SCD2 stream merge (pure Python):
             ├── Download master CSV from S3
             ├── Stream line-by-line: compare hash, close/carry-forward
             ├── Append new versions + brand-new LEIs from delta
             └── Atomic upload merged master
Phase 7      Post-flight validation
```

**When to use which?**
- `delta_merge.py` — cleaner code, fully Spark; works when master fits in Spark driver memory (~5 GB default)
- `delta_merge_backup.py` — handles arbitrarily large masters with constant memory; better for constrained environments

Both produce identical output and are interchangeable.

---

## Shared Utilities — pipeline_utils.py

All shared logic lives in a single file organized by section:

| Section | Functions / Classes |
|---|---|
| **Config & Logging** | `load_config_and_schema()`, `setup_logger()`, `validate_config()` |
| **S3 Helpers** | `s3_client()`, `atomic_s3_upload()`, `find_all_master_dates()` |
| **GLEIF Date Helpers** | `gleif_utc_today()`, `gleif_utc_now_iso()`, `extract_date_from_filename()` |
| **Retry Decorator** | `retry_with_backoff()` — exponential back-off decorator |
| **CSV Helpers** | `merge_csv_parts()` — merge Spark part files into single CSV |
| **Pre-flight Checks** | `detect_gaps()`, `check_idempotency()`, `validate_csv_schema()`, `run_preflight_checks()` |
| **Post-flight Validation** | `validate_output_file()` |
| **Notifications** | `PipelineNotifier` class (SNS) with `notify_start`, `notify_success`, `notify_failure`, `notify_warning` |

---

## Configuration

### config.json

```json
{
  "aws": {
    "region": "us-east-1",
    "access_key": "...",
    "secret_key": "..."
  },
  "s3": {
    "bucket": "etl-etl-e3fc0456",
    "raw_prefix": "gleif/raw",
    "master_csv_path": "s3://etl-etl-e3fc0456/gleif/processed/lei_master.csv",
    "manual_drop_prefix": "gleif/manual_drop"
  },
  "gleif": {
    "golden_copy": {
      "l1_url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv",
      "l2_url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest.csv"
    },
    "delta_file": {
      "l1_url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv?delta=LastDay",
      "l2_url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest.csv?delta=LastDay"
    }
  },
  "pipeline": {
    "log_level": "INFO",
    "partition_by": "Jurisdiction",
    "spark_tmp_dir": "/home/ec2-user/gleif-proj/tmp_data/spark",
    "retry_max_attempts": 3,
    "retry_base_delay_seconds": 30,
    "row_count_drop_threshold_pct": 5,
    "gap_action": "abort",
    "idempotency_action": "abort"
  },
  "notifications": {
    "enabled": false,
    "sns_topic_arn": "",
    "notify_on": ["success", "failure", "warning"]
  }
}
```

| Key | Description |
|---|---|
| `gap_action` | `"abort"` = halt if days are missing between last master and today's data; `"warn"` = log warning and proceed |
| `idempotency_action` | `"abort"` = skip if data_date already processed; `"reprocess"` = overwrite |
| `retry_max_attempts` | Number of retry attempts for network operations |
| `notify_on` | Which event levels trigger SNS notifications: `info`, `success`, `failure`, `warning` |

### schema.json

Defines the data contract between GLEIF source files and the pipeline output:

- **l1_columns** — Column rename mapping from GLEIF L1 CSV headers to output names, with `track_changes` flag for hash computation
- **l2_config** — Relationship pivot configuration (which L2 relationship types become output columns)
- **derived_columns** — Self-join lookups to resolve parent LEI → parent name
- **final_output_columns** — Ordered list of columns in the master CSV output

---

## Production Features

### Pre-flight Checks

Run before any processing begins. Configured via `mode` parameter (`"full"` or `"delta"`):

| Check | Full Load | Delta | What it does |
|---|---|---|---|
| S3 Connectivity | Yes | Yes | `head_bucket` on the configured S3 bucket |
| GLEIF API Health | No | Yes | HTTP HEAD to delta endpoints — confirms API is reachable |
| Previous Day Master | No | Yes | Verifies at least one master folder exists with a date >= yesterday |

### Gap Detection & Recovery

Before processing a delta, the pipeline compares the last master date in S3 against the incoming data_date. If days are missing:

- **gap_action = "abort"** (default): pipeline halts with a clear error message telling you to run `full_load.py`
- **gap_action = "warn"**: pipeline continues but logs a warning and sends an SNS notification

This prevents silent data loss from missed delta files.

### Idempotency Guard

Checks if `gleif/processed/{data_date}/` already exists in S3:

- **idempotency_action = "abort"** (default): skips processing, logs "already processed"
- **idempotency_action = "reprocess"**: overwrites the existing master for that date

### Schema Validation

After downloading CSVs (before any Spark processing), the pipeline reads the CSV header row and compares against `schema.json`:

- **Missing expected columns** → pipeline aborts with notification
- **New/extra columns** → logs info-level warning, sends notification, continues (additive changes are safe)

### Atomic S3 Uploads

Master CSV uploads use a staging-then-promote pattern:

1. Upload to `{key}.__staging__`
2. S3 CopyObject to the final key
3. Delete the staging key

This prevents downstream consumers from reading an incomplete file.

### Retry with Back-off

Network-dependent functions (`download_and_extract`, `download_delta_from_gleif`, `archive_to_bronze`) are decorated with `@retry_with_backoff`:

- Default: 3 attempts with exponential back-off (30s, 60s, 120s)
- Respects `KeyboardInterrupt` and `SystemExit` (no retry on those)

### Post-flight Validation

After writing the master CSV but before declaring success:

- File exists and is non-empty
- Header row contains all `final_output_columns` from schema.json
- At least one data row exists
- `LEI` column is present

Issues are logged as warnings (non-fatal) and sent via SNS.

### Notifications (AWS SNS)

The `PipelineNotifier` class sends best-effort notifications to an SNS topic:

| Event | Level | When |
|---|---|---|
| Pipeline started | `info` | After pre-flight passes |
| Pipeline succeeded | `success` | After all phases complete |
| Pipeline failed | `failure` | On unhandled exception (includes traceback) |
| Gap detected | `warning` | When missing days found between master and delta |
| Schema change | `warning` | When CSV headers don't match schema.json |
| Pre-flight failed | `warning` | When any pre-flight check fails |
| Idempotent skip | `warning` | When data_date already processed |

Notification failures are logged but never propagate to the pipeline.

**Setup:**
1. Create an SNS topic in AWS console
2. Subscribe your email to the topic
3. Set `notifications.enabled = true` and `notifications.sns_topic_arn` in config.json

### GLEIF UTC Date Alignment

GLEIF publishes on a UTC schedule. The pipeline:

- Extracts the `data_date` from the embedded GLEIF filename (e.g. `20260403-gleif-...` → `2026-04-03`)
- Uses this as the S3 partition date — never the local machine clock
- `pipeline_run_date` uses `datetime.now(timezone.utc)` for the wallclock timestamp
- This ensures consistent folder partitioning regardless of which timezone the pipeline runs in

---

## S3 Folder Layout

```
s3://etl-etl-e3fc0456/
├── gleif/
│   ├── manual_drop/           ← Upload Day 1 ZIPs here
│   │   ├── L1.zip
│   │   └── L2.zip
│   ├── raw/                   ← Bronze layer (raw archives)
│   │   ├── 2026-04-03/
│   │   │   ├── l1/L1_raw.csv
│   │   │   └── l2/L2_raw.csv
│   │   ├── 2026-04-04/
│   │   │   ├── l1/L1_delta.csv
│   │   │   └── l2/L2_delta.csv
│   │   └── ...
│   └── processed/             ← Silver layer (master CSV)
│       ├── 2026-04-03/
│       │   └── lei_master.csv     ← Full load output
│       ├── 2026-04-04/
│       │   └── lei_master.csv     ← Delta merged output
│       └── ...
```

---

## SCD Type-2 Logic

Each row in `lei_master.csv` has these SCD2 tracking columns:

| Column | Description |
|---|---|
| `record_hash` | MD5 hash of all `track_changes: true` columns — used to detect real data changes |
| `effective_start_date` | Date this version became active (GLEIF data date) |
| `effective_end_date` | Date this version was superseded (`null` if current) |
| `is_current` | `"true"` for the latest version, `"false"` for historical |
| `pipeline_run_date` | UTC date when the pipeline ran |

**On each delta merge:**

| Incoming LEI status | Action |
|---|---|
| **Brand new** (not in master) | Insert with `is_current=true`, `effective_start_date=data_date` |
| **Matched, hash changed** | Close old row (`is_current=false`, `effective_end_date=data_date`), insert new version |
| **Matched, hash unchanged** | Carry forward existing row (no change) |
| **Not in delta** (untouched) | Carry forward existing row (no change) |
| **Historical rows** | Pass through unchanged |

---

## How to Run

### Day 1 — Initial Load

```bash
# 1. Upload GLEIF golden copy ZIPs to S3
aws s3 cp gleif_l1_golden.zip s3://etl-etl-e3fc0456/gleif/manual_drop/L1.zip
aws s3 cp gleif_l2_golden.zip s3://etl-etl-e3fc0456/gleif/manual_drop/L2.zip

# 2. Run full load
python full_load.py
```

### Day 2+ — Delta Merge

```bash
# Option A: Spark-based merge (simpler, needs more memory)
python delta_merge.py

# Option B: Streaming merge (memory-optimized, handles large masters)
python delta_merge_backup.py
```

Both options are interchangeable and produce identical output.

### Scheduling (cron / EventBridge)

```bash
# Example cron (runs daily at 06:00 UTC — after GLEIF publishes)
0 6 * * * cd /home/ec2-user/gleif-proj && python delta_merge.py >> /var/log/gleif.log 2>&1
```

---

## Prerequisites

- Python 3.8+
- Apache Spark 3.x (PySpark)
- AWS credentials with S3 read/write access
- (Optional) AWS SNS topic for notifications

```bash
pip install pyspark boto3 requests
```
