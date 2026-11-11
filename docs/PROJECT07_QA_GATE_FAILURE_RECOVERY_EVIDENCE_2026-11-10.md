# Project 07 — Data Quality Gate / Failure Recovery (verified)

Date: 2026-11-10 (Asia/Bangkok)
Environment: Databricks Free Edition / Serverless / sandbox ONLY
Original Orders and existing Bronze/Silver/Gold tables: NOT MODIFIED.

## Implementation

- Local portable test gate: `src/quality_gate.py`
- Tests including deliberate failure cases: `tests/test_quality_gate.py`
- Databricks PySpark gate: `notebooks/03_quality_gate_sandbox.py`
- Input: completely fabricated 5,249-row CSV at
  `/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv`.
- Job: `Project07 QA Gate - Synthetic Orders Sandbox`
- Job ID: `668780230435384`
- Checks: source schema (65 columns), row count, business-key uniqueness
  (Order ID + SKU ID), missing keys, invalid quantity and amount, and
  selected empty PII columns.
- Failure injection: `inject_duplicate=true` duplicates a synthetic
  source record in SPARK MEMORY ONLY, not in CSV/Delta.

## Real Databricks runs (all verified via CLI and Opera)

| Stage | Run ID | Result | Runtime | Evidence |
|---|---|---|---|---|
| Baseline input (5,249 rows) | `630829978307016` | SUCCESS | 36,841 ms | [Open PASS run](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/668780230435384/run/630829978307016) |
| Inject one duplicate | `467710366048828` | FAILED (expected) | 32,194 ms | [Open failed run](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/668780230435384/run/467710366048828) |
| Recovery, remove injection | `33028334945419` | SUCCESS | 17,393 ms | [Open recovery run](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/668780230435384/run/33028334945419) |

**Root cause verified from `jobs get-run-output`:**
`ValueError: DQ_GATE_FAILED: duplicate_keys`

Failure is the **expected QA result**, not a production incident.
The Job reported FAILURE because the gate correctly rejected duplicate keys.
It then reported SUCCESS again on recovery input.

## Verification & test results

- Existing + new local Python tests: 23/23 PASS.
- Original production Bronze/Silver/Gold tables untouched.
- No Delta writes or other mutations in the QA Notebook.
- All screenshots were captured directly from authenticated Opera tabs and
  displayed in the ChatGPT conversation. Screenshots have **NOT yet been
  saved as publishable PNG files inside this repository**; authenticated
  Run links above are the primary persisted proof until sanitized screenshots
  are exported and reviewed for exposed account information.

## Next implementation tasks

1. Build four-task synthetic Medallion Job (ingest, transform, DQ gate, publish).
2. Enforce downstream Gold dependency on DQ PASS; verify it is skipped on FAIL.
3. Add audit/rejected rows, DQ rule version, and run metrics, isolated from
   private production tables.
4. Test schema drift, file missing, API unavailable and retry policy.
5. Build GitHub Actions tests/deployment with controlled environment config.
6. Sanitize/run screenshots before public Portfolio publication.

Do not publish private Workshop Orders, SKU or Product files.
