"""Local contract tests for crash-after-MERGE recovery.

All values are fabricated, and the test suite needs no Databricks credentials.
Real Delta MERGE and the deliberate failure must also be validated on Databricks.
"""
from __future__ import annotations
import unittest
from pathlib import Path
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "15_p5_crash_after_merge_recovery_sandbox.py"


def batch_rows():
    changes = {
        f"SYN-ORDER-{key:08d}": {
            "quantity_delta": 2, "amount_delta": Decimal("20.00")
        } for key in (5250, 5251)
    }
    for number in range(5270, 5275):
        changes[f"SYN-ORDER-{number:08d}"] = {
            "quantity_delta": (number % 4) + 1,
            "amount_delta": Decimal(((number % 19) + 1) * 100),
        }
    return changes


class CrashRecoveryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = NOTEBOOK.read_text(encoding="utf-8")

    def test_batch_has_two_corrections_five_new_rows(self):
        rows = batch_rows()
        self.assertEqual(len(rows), 7)
        self.assertEqual(len({k for k in rows if int(k[-8:]) <= 5269}), 2)
        self.assertEqual(len({k for k in rows if int(k[-8:]) > 5269}), 5)

    def test_expected_totals_after_committed_merge(self):
        rows = batch_rows()
        quantity = 13175 + sum(x["quantity_delta"] for x in rows.values())
        amount = Decimal("5265850.00") + sum(
            (x["amount_delta"] for x in rows.values()), Decimal("0.00")
        )
        self.assertEqual(quantity, 13192)
        self.assertEqual(amount, Decimal("5270890.00"))

    def test_failure_mode_is_explicit(self):
        self.assertIn('"inject_crash"', self.code)
        self.assertIn('"recover"', self.code)
        self.assertIn('"replay"', self.code)
        self.assertIn("P5_INJECTED_CRASH_AFTER_MERGE", self.code)

    def test_failure_injected_after_merge_before_checkpoint(self):
        merge = self.code.index("MERGE INTO {TARGET} AS t")
        failure = self.code.index("raise RuntimeError(")
        watermark = self.code.index("WHEN MATCHED AND s.last_source_updated_at >")
        audit_append = self.code.index('.mode("append").saveAsTable(AUDIT)')
        self.assertLess(merge, failure)
        self.assertLess(failure, watermark)
        self.assertLess(failure, audit_append)

    def test_checkpoint_has_initial_seed_and_monotonic_guard(self):
        self.assertIn("p5_initial_checkpoint", self.code)
        self.assertIn("WHEN MATCHED AND s.last_source_updated_at >", self.code)
        self.assertIn('P5_RECOVERY_REFUSED:', self.code)

    def test_merges_on_composite_key_and_row_hash(self):
        self.assertIn("t.order_id = s.order_id AND t.sku_id = s.sku_id", self.code)
        self.assertIn("s.row_hash <> t.row_hash", self.code)
        self.assertIn("s.source_updated_at > t.source_updated_at", self.code)

    def test_recovery_and_replay_do_not_change_business_data(self):
        self.assertIn('assert (inserted, updated, unchanged) == (0, 0, 7)', self.code)
        self.assertIn("P5_IDEMPOTENCY_FAILED", self.code)
        self.assertIn("P5_GOLD_UNCHANGED=p3_v2_valid", self.code)

    def test_data_quality_guards_are_present(self):
        self.assertIn("P5_DQ_FAILED", self.code)
        self.assertIn("distinct().count() == EXPECTED_BATCH_ROWS", self.code)
        self.assertIn('source.filter(~F.col("order_id").startswith("SYN-ORDER-"))', self.code)
        self.assertIn("P5_SOURCE_CORRECTION_CONTRACT_FAILED", self.code)

    def test_only_p5_tables_are_written(self):
        self.assertIn('"workspace.project07_sandbox.p5_"', self.code)
        self.assertIn('SCHEMA + ".p5_recovery_target"', self.code)
        self.assertIn('SCHEMA + ".p5_recovery_watermark"', self.code)
        self.assertIn('SCHEMA + ".p5_recovery_audit"', self.code)
        for unsafe in ('mode("overwrite")', "DROP TABLE", "TRUNCATE TABLE", "DELETE FROM"):
            self.assertNotIn(unsafe, self.code)

    def test_recovery_audit_keeps_unique_attempt_records(self):
        self.assertIn("str(uuid.uuid4())", self.code)
        self.assertIn('"RECOVERED" if mode == "recover" else "REPLAY_NOOP"', self.code)
        self.assertIn("P5_COMPLETED_AUDIT_ATTEMPTS=", self.code)


if __name__ == "__main__":
    unittest.main()
