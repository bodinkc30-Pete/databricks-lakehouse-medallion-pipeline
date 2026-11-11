# Project 07 — Four-Task Synthetic Medallion Workflow / QA-Gated Evidence

Date: 2026-11-10 (Asia/Bangkok)
Scope: Databricks Serverless; **synthetic data only**; sandbox schema
`workspace.project07_sandbox`. No existing production/historical Bronze,
Silver, Gold tables or real customer files changed.

## Scope and architecture

`bronze_ingest -> silver_transform -> qa_quality_gate -> gold_publish`

- Source: synthetic 5,249-row / 65-column CSV at
  `/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv`
- Bronze target: `workspace.project07_sandbox.bronze_orders`
- Silver target: `workspace.project07_sandbox.silver_orders`
- Gold target: `workspace.project07_sandbox.gold_orders_by_status`
- Job: `Project07 Synthetic Medallion 4-Task QA-Gated Sandbox`
- Job ID: `657830955393862`
- Job parameter `inject_duplicate`: default `false`, overridden to `true`
  for failure simulation
- Each downstream task depends on upstream success (standard Jobs default
  run-if-success semantics).

## Verified runs

| Stage | Run ID | Result | Details |
| --- | --- | --- | --- |
| First attempt, schema contract lab | `841514907865946` | FAILED | Bronze Delta rejected source column names with spaces / special characters |
| Fixed source column normalization | `475151455541778` | SUCCESS | All four tasks SUCCEEDED; runtime 51,325 ms |
| Duplicate injected in-memory | `118108582813372` | FAILED (expected) | Bronze and Silver SUCCEEDED, QA FAILED, Gold SKIPPED due to upstream failure |

[All four stages passing (authenticated Databricks run)](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/657830955393862/run/475151455541778)

[Failure / Gold skipped (authenticated Databricks run)](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/657830955393862/run/118108582813372)

### Root cause — real Delta Lake schema error

First attempt error:
`[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES]` because source column
names such as `Order ID` and `SKU ID` contain spaces. The fix preserves
all 65 fields and normalizes them to lowercase snake_case before writing
the synthetic Bronze Delta table. This fix was deployed to the sandbox
notebooks and the next full run passed.

### Root cause — deliberate failure lab

For the failure run `inject_duplicate=true`, QA duplicated one synthetic
Silver line **in Spark memory**; it does not change source CSV or Delta data.

Error confirmed via `jobs get-run-output`:
`ValueError: DQ_GATE_FAILED: row_count,duplicate_keys`

The workflow correctly skipped `gold_publish` in this run. Serverless
automatically retried the failed QA task once, which also FAILED; no
Gold task executed as part of that failed run.

**Important operational limitation:** The previously published sandbox Gold
table from the successful run still exists. A failed later run prevents a
*new* Gold task execution but does NOT roll back or invalidate an old Gold
table. The next version should introduce versioned staging, batch IDs and
atomic publish/promotion with freshness/status metadata, and should avoid
raw overwrites of shared tables.

## Captured and visually checked Portfolio evidence

- `docs/screenshots/jobs-workflows/project07_medallion_four_tasks_success.png`
  — saved screenshot of four green Jobs tasks
- `docs/screenshots/jobs-workflows/project07_qa_fail_gold_skipped.png`
  — saved screenshot of failed QA and skipped Gold
- Cropped from actual authorized Opera screenshots on the ROG, excluding
  browser tabs, address bar and user details. Do not fabricate screenshots.
- Original full-screen private captures (including browser tabs) remain in:
  `Desktop/Workshop/Project-07-Databricks-Lakehouse/04-Private-Evidence/`
  and must **never** be copied into a public Git repository.

## Checks and operational distinctions

- Static and local QA test suite: 31/31 PASS (run verified locally before
  deployment after schema fix).
- QA Gate checks 5,249 rows, duplicate business keys, null/empty keys,
  nonpositive quantity and negative/invalid order amount.
- Synthetic source only; the real source from Workshop remains private.
- The historical original notebook is not re-run or edited.
- Sandbox Bronze/Silver/Gold use overwrite on **sandbox-exclusive table names**
  for demonstrations. This is not yet an incremental/idempotent production
  ingestion solution; those need MERGE/checkpoints and event-time rules.
- An existing CLI authentication profile was used to run the sandbox Job.
  Profile names are **not** an authorization boundary; validate actual
  identity permissions and configure a dedicated least-privileged profile
  before any real environment deployment.

## Next acceptance criteria

1. Learner performs a read-only SQL audit in Databricks SQL Editor.
2. Learner manually runs Workflow with `inject_duplicate=false` and
   documents recovery (new successful run after intentional failure).
3. Add SCD/incremental/idempotent MERGE and per-batch audit metrics.
4. Versioned Gold publish and rollback/freshness semantics.
5. More failure labs: schema drift, missing source file, bad amount, late data.
6. CI/CD, documentation, privacy reviews and curated GitHub/Portfolio release.

Status: New development artifacts and evidence remain **local/uncommitted**
on `feature/project07-portfolio-upgrade`. Nothing published to GitHub or
the public Portfolio during this workflow session.
