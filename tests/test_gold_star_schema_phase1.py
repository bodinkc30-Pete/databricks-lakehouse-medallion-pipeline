"""Phase 1 model acceptance tests: synthetic-only business-grain contract."""
from __future__ import annotations

import sys
import unittest
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "08_gold_star_schema_sandbox.py"
sys.path.insert(0, str(ROOT / "src"))
from generate_synthetic_orders import make_order


class GoldStarSchemaPhase1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.notebook = NOTEBOOK.read_text(encoding="utf-8")
        # Generated entirely in memory: CI never requires an ignored private CSV.
        cls.rows = [make_order(i) for i in range(1, 5250)]

    def test_source_row_and_grain_contract(self):
        self.assertEqual(len(self.rows), 5249)
        self.assertEqual(len({r["Order ID"] for r in self.rows}), 5249)
        self.assertEqual(len({(r["Order ID"], r["SKU ID"]) for r in self.rows}), 5249)

    def test_sku_product_mapping_has_no_conflict(self):
        mapping = defaultdict(set)
        for row in self.rows:
            mapping[row["SKU ID"]].add(
                (row["Seller SKU"], row["Product Name"], row["Product Category"])
            )
        self.assertEqual(len(mapping), 53)
        self.assertTrue(all(len(values) == 1 for values in mapping.values()))
        self.assertEqual(len({r["Product Name"] for r in self.rows}), 53)

    def test_dim_date_has_five_calendar_days(self):
        dates = {
            datetime.strptime(row["Created Time"], "%d/%m/%Y %H:%M:%S").date()
            for row in self.rows
        }
        self.assertEqual(len(dates), 5)

    def test_reconciliation_source_measures(self):
        self.assertEqual(sum(int(row["Quantity"]) for row in self.rows), 13122)
        self.assertEqual(
            sum((Decimal(row["Order Amount"]) for row in self.rows), Decimal("0.00")),
            Decimal("5246000.00"),
        )

    def test_cancelled_amount_is_not_recognized_revenue(self):
        cancelled = [r for r in self.rows if r["Order Status"] == "Cancelled"]
        self.assertTrue(cancelled)
        self.assertTrue(any(Decimal(r["Order Amount"]) > 0 for r in cancelled))
        self.assertIn("completed_order_amount", self.notebook)
        self.assertIn("source_order_amount", self.notebook)

    def test_isolation_and_model_inventory(self):
        self.assertIn('CATALOG_SCHEMA = "workspace.project07_sandbox"', self.notebook)
        for name in (
            "gold_dim_sku", "gold_dim_product", "gold_dim_date",
            "gold_fact_orders", "gold_fact_order_lines",
        ):
            with self.subTest(table=name):
                self.assertIn(name, self.notebook)
        self.assertNotIn("ทั้งหมด คำสั่งซื้อ", self.notebook)
        self.assertNotIn("saveAsTable('workspace.gold", self.notebook)
        self.assertNotIn('saveAsTable("workspace.gold', self.notebook)

    def test_safety_gates_are_present_before_write(self):
        validate_pos = self.notebook.index("GOLD_DQ_FAILED:")
        write_pos = self.notebook.index('.saveAsTable(OUTPUTS[name])')
        self.assertLess(validate_pos, write_pos)
        self.assertIn('"left_anti"', self.notebook)
        self.assertIn("unit count reconciliation", self.notebook)
        self.assertIn("source amount reconciliation", self.notebook)


if __name__ == "__main__":
    unittest.main()
