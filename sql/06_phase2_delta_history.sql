-- Project 07 Phase 2: Read-only independent Delta history inspection.
-- Check numSourceRows, numTargetRowsInserted, numTargetRowsUpdated and
-- numTargetFilesAdded/Removed for bootstrap, delta, and replay.
-- A new history version may exist even if a replay changes zero data rows.
DESCRIBE HISTORY workspace.project07_sandbox.p2_incremental_order_lines;
