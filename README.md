# Databricks Lakehouse Medallion Pipeline

Portfolio-grade Data Engineering project using **Apache Spark, PySpark, Spark SQL, Databricks, Delta Lake, Unity Catalog, and Photon**.

**E-commerce Orders CSV → Bronze → Silver → Gold**

The project covers ingestion, schema handling, incremental processing / CDC, SCD Type 2 recovery, data quality, monitoring, troubleshooting, and Spark performance tuning with measured evidence.

---

## Technology Stack

- Apache Spark / PySpark
- Spark SQL
- Databricks
- Delta Lake
- Unity Catalog
- Photon
- Medallion Architecture
- Delta MERGE / UPSERT
- Delta Change Data Feed concepts

---

## Source Dataset

Main dataset: E-commerce Orders CSV

Observed source characteristics:

- ~5,249 rows
- ~65 columns
- Source column names include spaces and special characters
- Initial Delta ingestion hit `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`

The pipeline therefore normalizes column names before the Bronze Delta write.

---

## Architecture

Detailed Mermaid architecture:

[`docs/architecture/lakehouse_architecture.md`](docs/architecture/lakehouse_architecture.md)

High-level flow:

```text
E-commerce Orders CSV
        |
        v
Bronze: workspace.bronze.orders_raw
        |
        v
Silver: workspace.silver.orders_clean
        |
        v
Gold:
- fact_order_lines
- dim_product
- dim_sku
- daily_sales
- channel_performance
        |
        +--> Incremental / CDC
        +--> SCD2 / Recovery
        +--> Data Quality
        +--> Monitoring / Audit
        +--> Troubleshooting
        +--> Performance Tuning
```

---

## Bronze Layer

Primary table:

```text
workspace.bronze.orders_raw
```

Evidence:

![Bronze ingestion](docs/screenshots/bronze/bronze_ingestion.png)

Final evidence shows:

```text
Created table: workspace.bronze.orders_raw
Rows: 5249
```

---

## Silver Layer

Primary table:

```text
workspace.silver.orders_clean
```

Evidence:

![Silver transformation](docs/screenshots/silver/silver_transformation.png)

Final evidence shows:

```text
Created table: workspace.silver.orders_clean
Rows: 5249
```

---

## Gold Layer

Important Gold objects include:

```text
workspace.gold.fact_order_lines
workspace.gold.dim_product
workspace.gold.dim_sku
workspace.gold.daily_sales
workspace.gold.channel_performance
```

Gold fact evidence:

![Gold fact order lines](docs/screenshots/gold/gold_fact_order_lines.png)

Observed validation:

```text
fact_order_lines rows: 5249
Distinct order + sku: 5249
```

This supports the intended `(order_id, sku_id)` grain in the final run.

---

## Incremental Processing / CDC

Checkpoint object:

```text
workspace.gold.cdf_checkpoint
```

CDF evidence:

![CDF incremental changes](docs/screenshots/incremental/cdf_incremental_changes.png)

Observed CDF fields include:

```text
_change_type
_commit_version
_commit_timestamp
```

with change types such as:

```text
update_postimage
insert
```

Checkpoint decision evidence:

![CDF checkpoint decision](docs/screenshots/incremental/cdf_checkpoint_decision.png)

Observed decision:

```text
last_processed_version = 8
latest_version         = 8
new_versions_available = 0
pipeline_action        = NO_NEW_CHANGES
```

---

## SCD Type 2 / Recovery

Recovery sandbox:

```text
workspace.gold.dim_sku_scd2_recovery_sandbox
```

Evidence:

![SCD2 recovery](docs/screenshots/scd2/scd2_recovery.png)

Final validation:

```text
Version count: 2
Current version count: 1
Closed historical versions: 1
Portfolio SCD2 status: PASS
```

Additional lifecycle validation:

```text
Duplicate current SKU count : 0
Current rows with valid_to   : 0
Closed rows missing valid_to : 0
SCD2 validation status       : PASS
```

---

## Data Quality

Persisted summary:

```text
workspace.gold.data_quality_summary
```

Evidence:

![Data quality summary](docs/screenshots/data-quality/data_quality_summary.png)

Persisted checks:

| Check | Result |
|---|---|
| NULL_ORDER_ID | PASS |
| NULL_SKU_ID | PASS |
| DUPLICATE_ORDER_SKU | PASS |
| INVALID_QUANTITY | PASS |

Observed summary:

```text
total_rows       = 5249
completeness_pct = 100
```

> The persisted final summary shown here proves these four checks. Broader DQ dimensions should only be claimed where separate notebook evidence exists.

---

## Monitoring / Audit

Audit table:

```text
workspace.gold.pipeline_run_audit
```

Success evidence:

![Pipeline success](docs/screenshots/monitoring/pipeline_success.png)

Observed successful run:

```text
batch_id      = monitoring_batch_001
run_status    = SUCCESS
rows_read     = 5249
rows_written  = 5249
rows_rejected = 0
```

---

## Troubleshooting / Failure Evidence

Failure evidence:

![Read source failure](docs/screenshots/troubleshooting/read_source_failure.png)

Observed failure:

```text
failed run count = 1
batch_id         = monitoring_batch_002
run_status       = FAILED
failed_step      = READ_SOURCE
error_message    = TABLE_OR_VIEW_NOT_FOUND
```

---

## Spark Performance Tuning

Benchmark tables:

```text
workspace.gold.spark_performance_benchmark
workspace.gold.spark_performance_improvement_summary
```

### Benchmark Coverage

![Performance benchmark](docs/screenshots/performance/performance_benchmark.png)

Evidence rows by experiment:

```text
data_skew              = 3
data_skipping          = 2
join_strategy          = 2
materialization        = 2
partitioning_strategy  = 2
shuffle_partitions     = 4
small_files            = 2
```

Total benchmark evidence rows: `17`.

### Improvement Summary

![Performance improvement summary](docs/screenshots/performance/performance_improvement_summary.png)

| Experiment | Before | After | Improvement |
|---|---:|---:|---:|
| Join strategy | 0.510 s | 0.332 s | 34.90% |
| Shuffle partitions | 0.785 s | 0.622 s | 20.76% |
| Materialization | 0.541 s | 0.480 s | 11.28% |
| Data skew mitigation | 0.711 s | 0.642 s | 9.70% |
| Partitioning strategy | 0.784 s | 0.714 s | 8.93% |

These are workload-specific measurements, not universal Spark tuning rules.

---

## Spark Physical Plan Evidence

### Hash Partitioning
![Shuffle hash partitioning](docs/screenshots/performance/01_shuffle_hashpartitioning_16.png)

### Single Partition Exchange
![Single partition shuffle](docs/screenshots/performance/02_shuffle_single_partition.png)

### Executor Broadcast
![Executor broadcast](docs/screenshots/performance/03_executor_broadcast.png)

### Photon Broadcast Hash Join
![Photon broadcast hash join](docs/screenshots/performance/04_photon_broadcast_hash_join.png)

### Photon Support
![Photon fully supported](docs/screenshots/performance/05_photon_fully_supported.png)

Observed plan evidence includes:

```text
PhotonShuffleExchangeSink
hashpartitioning(..., 16)

PhotonShuffleMapStage
Arguments: EXECUTOR_BROADCAST

PhotonBroadcastHashJoin
Join type: LeftOuter

== Photon Explanation ==
The query is fully supported by Photon.
```

---

## Repository Structure

```text
databricks-lakehouse-medallion-pipeline/
│
├── README.md
├── .gitignore
├── notebooks/
│   └── 01_spark_foundation.py
├── src/
├── sql/
└── docs/
    ├── architecture/
    │   └── lakehouse_architecture.md
    └── screenshots/
        ├── bronze/
        ├── silver/
        ├── gold/
        ├── incremental/
        ├── scd2/
        ├── data-quality/
        ├── monitoring/
        ├── troubleshooting/
        └── performance/
```

---

## Portfolio Evidence Summary

| Capability | Evidence |
|---|---|
| Bronze ingestion | `bronze_ingestion.png` |
| Silver transformation | `silver_transformation.png` |
| Gold fact modeling | `gold_fact_order_lines.png` |
| CDC changes | `cdf_incremental_changes.png` |
| Checkpoint decision | `cdf_checkpoint_decision.png` |
| SCD2 recovery | `scd2_recovery.png` |
| Data quality | `data_quality_summary.png` |
| Monitoring success | `pipeline_success.png` |
| Failure troubleshooting | `read_source_failure.png` |
| Performance benchmark | `performance_benchmark.png` |
| Performance improvements | `performance_improvement_summary.png` |
| Hash partitioning plan | `01_shuffle_hashpartitioning_16.png` |
| Single partition exchange | `02_shuffle_single_partition.png` |
| Executor broadcast | `03_executor_broadcast.png` |
| Broadcast hash join | `04_photon_broadcast_hash_join.png` |
| Photon support | `05_photon_fully_supported.png` |

---

## Engineering Approach

```text
EXPLAIN
→ IMPLEMENT
→ TEST
→ TROUBLESHOOT
→ OPTIMIZE
→ PROVE WITH EVIDENCE
```

---

## Current Status

**Status: COMPLETE**

The core engineering implementation, validation, troubleshooting, performance experiments, architecture documentation, portfolio evidence, Git version control, and GitHub publication are complete.

Final repository validation completed:

- Bronze, Silver, and Gold evidence documented
- Incremental / CDC and checkpoint behavior documented
- SCD Type 2 recovery and lifecycle validation documented
- Data Quality summary documented
- Monitoring success and controlled failure evidence documented
- Spark performance benchmarks and improvement measurements documented
- Spark physical-plan evidence documented
- Mermaid Lakehouse architecture successfully rendered on GitHub
- Repository initialized with Git and committed to `main`
- Public GitHub repository created and pushed
- Final README and screenshot rendering reviewed

This repository is portfolio-ready.
