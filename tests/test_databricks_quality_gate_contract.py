"""Repository contract checks for the live Databricks synthetic QA gate."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "03_quality_gate_sandbox.py"


class DatabricksQualityGateContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = NOTEBOOK.read_text(encoding="utf-8")

    def test_isolated_synthetic_input_only(self):
        self.assertIn("project07_portfolio_sandbox/synthetic_orders.csv", self.source)
        self.assertNotIn("orders_sales_2026-08-13_private", self.source)

    def test_quality_failure_prevents_success(self):
        self.assertIn("if errors:", self.source)
        self.assertIn('raise ValueError("DQ_GATE_FAILED:', self.source)
        self.assertIn('print("QA_RESULT=PASS")', self.source)
        self.assertIn("duplicate_keys", self.source)

    def test_does_not_mutate_delta_or_export_customer_records(self):
        lowered = self.source.lower()
        for forbidden in (
            ".write", "saveastable", "insert into", "merge into",
            "delete from", "drop table", "truncate table",
            ".show(", "display(df", "display(source_df",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, lowered)


if __name__ == "__main__":
    unittest.main()
