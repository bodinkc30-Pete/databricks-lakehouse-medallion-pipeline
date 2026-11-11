"""Static safety checks for isolated Project 07 four-task Databricks workflow."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
FILES = {
    "bronze": "04_bronze_ingest_sandbox.py",
    "silver": "05_silver_transform_sandbox.py",
    "qa": "06_quality_gate_silver_sandbox.py",
    "gold": "07_gold_publish_sandbox.py",
}
SOURCES = {
    key: (ROOT / "notebooks" / file).read_text(encoding="utf-8")
    for key, file in FILES.items()
}


class MedallionWorkflowSafetyTests(unittest.TestCase):
    def test_all_four_notebooks_exist(self):
        self.assertEqual(len(SOURCES), 4)
        for label, source in SOURCES.items():
            with self.subTest(step=label):
                self.assertIn("PROJECT 07", source)

    def test_all_table_refs_are_isolated(self):
        for label, source in SOURCES.items():
            with self.subTest(step=label):
                self.assertNotIn('saveAsTable("workspace.gold.', source)
                self.assertNotIn('saveAsTable("workspace.silver.', source)
                self.assertNotIn('saveAsTable("workspace.bronze.', source)
                self.assertNotIn('spark.table("workspace.gold.', source)
                self.assertIn("project07_sandbox", source)

    def test_only_synthetic_raw_dataset_used(self):
        self.assertIn("project07_portfolio_sandbox/synthetic_orders.csv", SOURCES["bronze"])
        for label, source in SOURCES.items():
            with self.subTest(step=label):
                self.assertNotIn("ทั้งหมด คำสั่งซื้อ", source)
                self.assertNotIn("orders_sales_2026-08-13_private.csv", source)

    def test_dq_raises_before_gold(self):
        self.assertIn("DQ_GATE_FAILED", SOURCES["qa"])
        self.assertIn("duplicate_keys", SOURCES["qa"])
        self.assertIn("QA_RESULT=PASS", SOURCES["qa"])
        self.assertNotIn("saveAsTable", SOURCES["qa"])

    def test_only_gold_publishes_gold_table(self):
        self.assertIn("gold_orders_by_status", SOURCES["gold"])
        self.assertIn("RECONCILED_ROWS", SOURCES["gold"])
        for label in ("bronze", "silver", "qa"):
            self.assertNotIn("gold_orders_by_status", SOURCES[label])


if __name__ == "__main__":
    unittest.main()
