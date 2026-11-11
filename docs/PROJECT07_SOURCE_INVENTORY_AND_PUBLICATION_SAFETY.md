# Project 07 — Verified Source Inventory & Publication Safety

As-of: 2026-11-10
Status: VERIFIED LOCAL SOURCE; PUBLIC DATASET NOT APPROVED
Project: Databricks Lakehouse Medallion Pipeline
Development branch: `feature/project07-portfolio-upgrade`

## Source of truth: original datasets (PRIVATE)

| Role | Original file | Verified local source | Notes |
| --- | --- | --- | --- |
| Orders | `ทั้งหมด คำสั่งซื้อ-2026-08-13-12_26.csv` | `Desktop/Workshop/` | 5,249 data records, 65 source columns, 5,224,938 bytes |
| Product/SKU reference | `product_sku_list.xlsx` | `Desktop/Workshop/` | 7,918 bytes; schema/content still needs validation before changing pipeline |
| Product reference | `product_list_20260214.xlsx` | `Desktop/Workshop/` | 20,671 bytes; schema/content still needs validation before changing pipeline |

A separate local backup was verified byte-identical to the source.
Its private workstation path is intentionally omitted.
SHA-256 for both copies (read-only verification):
`296F4FA4B8A99B18EFA959CB779B20C4693421EF3D808B4592A6FA8A87258825`

The original order source is explicitly referenced in
`notebooks/01_spark_foundation.py` (around line 93) as
`/Volumes/workspace/bronze/source_files/orders/ทั้งหมด คำสั่งซื้อ-2026-08-13-12_26.csv`.
Product/SKU and product reference Excel files are also referenced by that notebook.

## Security classification

**Do not upload, commit, publish, or share the raw CSV or raw source XLSX files.**
The orders CSV contains buyer and recipient identifiers, phone numbers,
shipping addresses, buyer messages, tax-invoice details, and other fields
that may be personal, sensitive, or confidential business information.

For a PUBLIC GitHub repository and Portfolio website:
- Use fully synthetic test data created independently of identifiable records
  but preserving required field names, types and representative business rules.
- Keep the real dataset in its existing private location and use it only in
  an authorized local/private Databricks learning context.
- Never copy actual phone, address, tax identifier, username, recipient,
  free-text notes, screenshots with identifiable order rows, or OAuth credentials
  into commits, README, CI logs, issues, screenshots or published artifacts.
- Inspect existing screenshots and notebook output for identifying information
  BEFORE publishing new screenshots or exporting HTML.
- Do not claim a synthetic dataset is a real production dataset.
- Record source/target row counts, business-key uniqueness, DQ results,
  execution durations, and sanitized evidence without customer-level values.
- Do not change or overwrite existing Bronze/Silver/Gold tables while
  restructuring Project 07 without separate approval and an isolated sandbox.

## Project 07 implementation priorities (job-responsibility based)

1. Audit current repo, workspace objects and portfolio evidence; keep existing
   successful outputs immutable.
2. Create synthetic 65-column CSV fixture and a source-schema contract.
   Check that the new fixture is non-sensitive; preserve original raw source.
3. Modularize the giant exploratory notebook into ingest/transform/validate/publish
   modules with a repeatable configuration and environment boundaries.
4. Build Jobs/Workflows for the synthetic dataset: dependency graph, timeout,
   retry, run parameters, no-op incremental replay, DQ gating.
5. Instrument batch/run metrics (read/written/rejected, duration, stage, run ID),
   sanitized error handling and a human-reviewed recovery runbook.
6. Add PostgreSQL/Mock API/CSV ingestion examples and provenance tracking.
7. Add unit/integration tests, GitHub Actions, deployment validation and governed
   release procedures (Databricks Declarative Automation Bundles where supported).
8. Demonstrate documented Unity Catalog access, lineage and PII protection using
   synthetic records and supported workspace permissions.
9. Validate screenshots and test outcomes, then update Project 07 GitHub README
   AND portfolio site `databricks-lakehouse` entry together.

## Naming consistency

Project source repository is historically `project-07-databricks-lakehouse`,
while the website currently labels the featured project as **02** and uses
`databricks-lakehouse` in its URL. Treat these as separate numbering conventions;
preserve the existing site links and layout unless an explicit design change
is requested.

## Current checkpoint

- Verified original orders source and SHA-256: DONE.
- Public-safe source fixture: NOT YET CREATED.
- Synthetic job execution and run evidence: NOT YET CREATED.
- Portfolio website edit/publish: NOT YET DONE.
- Git commit/push to GitHub: NOT YET DONE.
- Existing data or Production tables modified: NO.

Next step: build and test a synthetic fixture with the SAME source-field
contract, and audit any existing exposed screenshots before new publication.
