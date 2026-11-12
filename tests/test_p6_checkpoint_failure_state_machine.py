"""Phase 6 state-machine and partial-commit recovery contracts.

No remote credentials needed. Real Spark/Delta checks run in Databricks and
must be separately verified by SQL Warehouse and DESCRIBE HISTORY.
"""
from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
SOURCE = BASE / "notebooks" / "16_p6_crash_after_watermark_state_machine_sandbox.py"


class Phase6CheckpointRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = SOURCE.read_text(encoding="utf-8")

    def test_expected_increments(self):
        self.assertEqual(sum((i % 4) + 1 for i in range(5275, 5278)), 7)
        self.assertEqual(13192 + 1 + 7, 13200)
        additions = sum(((i % 19) + 1) * 100 for i in range(5275, 5278))
        self.assertEqual(additions, 4200)
        self.assertEqual(Decimal("5270890.00") + Decimal(10 + additions),
                         Decimal("5275100.00"))

    def test_composite_key_grain(self):
        self.assertIn('t.order_id=s.order_id AND t.sku_id=s.sku_id', self.code)
        self.assertIn('F.countDistinct("order_id", "sku_id")', self.code)
        self.assertIn("P6_DUPLICATE_BATCH_COMPLETION", self.code)

    def test_failure_and_recovery_modes(self):
        for mode in ('"inject_crash"', '"recover"', '"replay"'):
            self.assertIn(mode, self.code)
        self.assertIn("P6_INJECTED_CRASH_AFTER_WATERMARK", self.code)

    def test_state_machine_has_expected_statuses(self):
        for status in ("STARTED", "MERGED", "CHECKPOINTED", "FAILED", "COMPLETED"):
            self.assertIn('"'+status+'"', self.code)
        self.assertIn("ALLOWED_NEXT", self.code)
        self.assertIn("P6_INVALID_STATE_TRANSITION", self.code)

    def test_state_events_have_per_attempt_identity(self):
        self.assertIn("str(uuid.uuid4())", self.code)
        self.assertIn('SCHEMA + ".p6_state_events"', self.code)
        self.assertIn('"step_index"', self.code)
        self.assertIn('mode("append").saveAsTable(EVENTS)', self.code)

    def test_crash_occurs_after_checkpoint_before_completion(self):
        target_merge = self.code.index("MERGE INTO {TARGET} t")
        checkpoint_merge = self.code.index("MERGE INTO {WATERMARK} t", target_merge)
        fail = self.code.index('if mode == "inject_crash":\n    # Intentionally record')
        audit_merge = self.code.index("MERGE INTO {COMPLETION} t")
        self.assertLess(target_merge, checkpoint_merge)
        self.assertLess(checkpoint_merge, fail)
        self.assertLess(fail, audit_merge)

    def test_checkpoint_updates_only_monotonically(self):
        self.assertIn("WHEN MATCHED AND s.last_source_updated_at > t.last_source_updated_at",
                      self.code)
        self.assertIn('"2026-02-04 00:00:00"', self.code)

    def test_recovery_accepts_advanced_watermark_but_missing_audit(self):
        self.assertIn('checkpoint_before == BATCH_DATE', self.code)
        self.assertIn('completion_before == 0', self.code)
        self.assertIn('"FAILED"', self.code)
        self.assertIn("P6_RECOVERY_REFUSED", self.code)

    def test_injection_does_not_repeat_after_commit(self):
        self.assertIn('existing_events == 0 and completion_before == 0', self.code)
        self.assertIn('P6_INJECTION_REFUSED: target not pristine', self.code)

    def test_recovery_and_replay_are_data_level_noops(self):
        self.assertIn('(inserted, updated, unchanged) == (0, 0, 4)', self.code)
        self.assertIn("P6_RECOVERY_IDEMPOTENCY_FAILED", self.code)
        self.assertIn('s.row_hash <> t.row_hash', self.code)

    def test_batch_completion_is_idempotent(self):
        self.assertIn('ON t.batch_id=s.batch_id', self.code)
        self.assertIn('WHEN NOT MATCHED THEN INSERT (', self.code)
        self.assertIn('len(recorded) == 1', self.code)

    def test_older_gold_is_only_read(self):
        self.assertIn('SCHEMA + ".p3_publish_manifest"', self.code)
        self.assertIn('"p3_v2_valid"', self.code)
        self.assertIn('P6_GOLD_UNCHANGED=p3_v2_valid', self.code)

    def test_only_isolated_p6_objects_are_write_targets(self):
        for s in ('p6_recovery_target','p6_recovery_watermark',
                  'p6_state_events','p6_completion_audit'):
            self.assertIn(s,self.code)
        self.assertIn('"workspace.project07_sandbox.p6_"', self.code)
        for unsafe in ('mode("overwrite")','DROP TABLE','DELETE FROM','TRUNCATE TABLE'):
            self.assertNotIn(unsafe,self.code)

    def test_replay_noop_records_final_state(self):
        self.assertIn('"STARTED", "MERGED", "CHECKPOINTED", "COMPLETED"', self.code)
        self.assertIn('P6_LAB02_RESULT=PASS', self.code)

if __name__=="__main__":
    unittest.main()
