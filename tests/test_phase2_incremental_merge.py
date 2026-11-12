"""Portable CI contracts for Project 07 Phase 2.

No customer data, Databricks credentials or ignored local CSV required.
"""
from __future__ import annotations
import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from generate_synthetic_orders import make_order

NOTEBOOK = ROOT / "notebooks" / "09_incremental_merge_sandbox.py"


def baseline():
    """Independent Python representation of the fabricated Silver table."""
    return {
        (r["Order ID"], r["SKU ID"]): {
            "order_status": r["Order Status"],
            "quantity": int(r["Quantity"]),
            "amount": Decimal(r["Order Amount"]),
            "ts": "2026-02-01",
        }
        for r in (make_order(i) for i in range(1, 5250))
    }


def incremental_batch():
    """Independent business scenario: 3 corrections + 20 new fabricated rows."""
    records = {}
    for i in (1, 4, 7):
        r = make_order(i)
        records[(r["Order ID"], r["SKU ID"])] = {
            "order_status": "Completed",
            "quantity": int(r["Quantity"]) + 1,
            "amount": Decimal(r["Order Amount"]) + Decimal("50.00"),
            "ts": "2026-02-02",
        }
    for i in range(5250, 5270):
        r = make_order(i)
        records[(r["Order ID"], r["SKU ID"])] = {
            "order_status": r["Order Status"],
            "quantity": int(r["Quantity"]),
            "amount": Decimal(r["Order Amount"]),
            "ts": "2026-02-02",
        }
    return records


def merge(target, incoming):
    """Reference behavior, not a substitute for a real Delta Lake MERGE run."""
    inserts, updates, unchanged = 0, 0, 0
    for key, record in incoming.items():
        old = target.get(key)
        if old is None:
            target[key] = record.copy()
            inserts += 1
        elif record["ts"] > old["ts"] and any(
            record[field] != old[field]
            for field in ("order_status", "quantity", "amount")
        ):
            target[key] = record.copy()
            updates += 1
        else:
            unchanged += 1
    return inserts, updates, unchanged


class Phase2IncrementalContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = NOTEBOOK.read_text(encoding="utf-8")

    def test_fabricated_baseline_contract(self):
        rows = baseline()
        self.assertEqual(len(rows), 5249)
        self.assertEqual(sum(x["quantity"] for x in rows.values()), 13122)
        self.assertEqual(
            sum((x["amount"] for x in rows.values()), Decimal("0.00")),
            Decimal("5246000.00"),
        )

    def test_incremental_batch_has_expected_grain(self):
        new_data = incremental_batch()
        self.assertEqual(len(new_data), 23)
        self.assertEqual(len([k for k in new_data if int(k[0][-8:]) <= 5249]), 3)
        self.assertEqual(len([k for k in new_data if int(k[0][-8:]) > 5249]), 20)

    def test_merge_and_exact_replay_are_idempotent(self):
        table = {}
        self.assertEqual(merge(table, baseline()), (5249, 0, 0))
        self.assertEqual(merge(table, incremental_batch()), (20, 3, 0))
        after_first = {key: value.copy() for key, value in table.items()}
        self.assertEqual(merge(table, incremental_batch()), (0, 0, 23))
        self.assertEqual(table, after_first)
        self.assertEqual(len(table), 5269)

    def test_stale_update_does_not_overwrite_latest(self):
        target = baseline()
        merge(target, incremental_batch())
        latest = {k: v.copy() for k, v in target.items()}
        self.assertEqual(merge(target, baseline()), (0, 0, 5249))
        self.assertEqual(target, latest)

    def test_notebook_write_targets_isolated(self):
        self.assertIn('SCHEMA = "workspace.project07_sandbox"', self.source)
        self.assertIn('SCHEMA + ".p2_incremental_order_lines"', self.source)
        self.assertIn('SCHEMA + ".p2_batch_audit"', self.source)
        self.assertIn('SCHEMA + ".p2_watermark"', self.source)
        self.assertNotIn('saveAsTable("workspace.gold.', self.source)
        self.assertNotIn("TRUNCATE TABLE", self.source)
        self.assertNotIn('mode("overwrite")', self.source)

    def test_merge_has_timestamp_and_hash_guards(self):
        self.assertIn("WHEN MATCHED AND s.source_updated_at > t.source_updated_at", self.source)
        self.assertIn("s.row_hash <> t.row_hash", self.source)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", self.source)
        self.assertIn("P2_IDEMPOTENCY_FAILED", self.source)
        self.assertIn("P2_STALE_BATCH_REJECTED", self.source)

    def test_data_quality_and_reconciliation_gates(self):
        for phrase in (
            "P2_DQ_FAILED: duplicate business keys",
            "P2_DQ_FAILED: invalid input row",
            "P2_RECONCILIATION_FAILED",
            "P2_WATERMARK_FAILED",
            "P2_RESULT=PASS",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.source)


if __name__ == "__main__":
    unittest.main()
