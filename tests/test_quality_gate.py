"""Tests and incident simulations for the Project 07 data quality gate."""
from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from quality_gate import validate_orders
from generate_synthetic_orders import make_order


class DataQualityGateTests(unittest.TestCase):
    def setUp(self):
        self.clean_rows = [make_order(i) for i in range(1, 11)]

    def test_good_batch_passes(self):
        result = validate_orders(self.clean_rows)
        self.assertTrue(result.passed)
        self.assertEqual(result.total_rows, 10)
        self.assertEqual(result.failed_rules, ())
        result.require_pass()

    def assert_failure(self, rows, expected_rule):
        result = validate_orders(rows)
        self.assertFalse(result.passed)
        self.assertIn(expected_rule, result.failed_rules)
        with self.assertRaisesRegex(ValueError, "DATA_QUALITY_GATE_FAILED"):
            result.require_pass()

    def test_duplicate_business_key(self):
        self.assert_failure(self.clean_rows + [self.clean_rows[0].copy()],
                            "duplicate_business_key")

    def test_missing_order_id(self):
        rows = [row.copy() for row in self.clean_rows]
        rows[0]["Order ID"] = ""
        self.assert_failure(rows, "missing_business_key")

    def test_invalid_quantity(self):
        for value in ("0", "-3", "abc", "1.5", "NaN"):
            with self.subTest(value=value):
                rows = [row.copy() for row in self.clean_rows]
                rows[0]["Quantity"] = value
                self.assert_failure(rows, "invalid_quantity")

    def test_invalid_amount(self):
        for value in ("-1", "abc", "Infinity", ""):
            with self.subTest(value=value):
                rows = [row.copy() for row in self.clean_rows]
                rows[0]["Order Amount"] = value
                self.assert_failure(rows, "invalid_amount")

    def test_sensitive_field_populated(self):
        for name in ("Phone #", "Detail Address", "Tax Info - Buyer Tax ID"):
            with self.subTest(field=name):
                rows = [row.copy() for row in self.clean_rows]
                rows[0][name] = "SYNTHETIC-BAD-VALUE"
                self.assert_failure(rows, "sensitive_field_populated")

    def test_missing_columns(self):
        rows = [row.copy() for row in self.clean_rows]
        rows[0].pop("Quantity")
        self.assert_failure(rows, "missing_columns")

    def test_empty_batch(self):
        self.assert_failure([], "empty_batch")

    def test_original_synthetic_file_if_present(self):
        path = ROOT / "data" / "synthetic_orders.csv"
        if not path.exists():
            self.skipTest("Generated synthetic CSV not found")
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        result = validate_orders(rows)
        self.assertTrue(result.passed, result.failed_rules)
        self.assertEqual(result.total_rows, 5249)


if __name__ == "__main__":
    unittest.main()
