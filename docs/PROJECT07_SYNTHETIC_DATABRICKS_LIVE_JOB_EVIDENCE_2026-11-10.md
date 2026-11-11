# Project 07 — Synthetic Orders PySpark Live Job Evidence

Verified: 2026-11-10 (Asia/Bangkok)
Execution environment: Databricks Free Edition — Serverless
Data classification: **Synthetic only**. Customer records were NOT uploaded.

## Source and reproducibility

- Generator: `src/generate_synthetic_orders.py`
- Source headers: 65 original-field names, values entirely fabricated
- Local output (not tracked): `data/synthetic_orders.csv`, 5,249 rows
- Volume path: `/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv`
- Notebook: `notebooks/02_synthetic_orders_sandbox.py`
- Workspace notebook: `/Workspace/Users/<workspace-user>/project-07-databricks-lakehouse/notebooks/02_synthetic_orders_sandbox`
- Saved Job: `Project07 Synthetic ReadOnly Validation (Sandbox)`
- Job ID: `110106888696175`
- Run ID: `820692454275348`
- Task run ID: `857527225098630`
- Job status: **SUCCESS** (run lifecycle TERMINATED)
- Runtime reported by Jobs API: **33,063 milliseconds**
- Databricks Run UI reported: **Succeeded**, Rows read: **5,249**

[View authenticated run evidence](https://dbc-381faf33-7320.cloud.databricks.com/?o=7474648569137518#job/110106888696175/run/820692454275348)

## Validation scope

Notebook runs a read-only Spark CSV ingestion. It asserts:
- 65 columns including Order ID, SKU ID, Quantity, Order Amount
- 5,249 data records
- 5,249 distinct (Order ID, SKU ID) key pairs
- zero missing Order IDs
- zero nonempty Phone # and Detail Address fields

The notebook performs **no Delta write, no MERGE, and no Gold publication**.
A SUCCESS here proves this narrow synthetic-source validation, not that all
medallion transformations or orchestration are production ready.

## Security / publication

The original Orders CSV under `Desktop/Workshop` contains personal data and
must NOT be committed/published. The synthetic generator is safe for public
review, but the generated CSV remains ignored by existing `.gitignore`.
Review any screenshots for email addresses and user-identifying details before
adding them to public GitHub. No screenshot has been committed yet.

## Next acceptance milestone

1. Build a separate Bronze/Silver/Gold sandbox using the synthetic CSV.
2. Add quality gates to prevent downstream updates on invalid source data.
3. Add idempotent incremental replay and batch-level audit/recovery.
4. Include automated testing of ETL functions and a governed CI/CD deployment.
5. Only after reviewing outputs and screenshot privacy: update README, GitHub,
   and portfolio website; leave existing production tables untouched.
