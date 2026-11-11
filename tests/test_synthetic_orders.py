"""Tests for standalone safe Project 07 synthetic dataset generation."""
from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from generate_synthetic_orders import SOURCE_COLUMNS, generate_orders, make_order


class SyntheticOrdersTests(unittest.TestCase):
    def test_exact_source_header_contract(self):
        self.assertEqual(len(SOURCE_COLUMNS), 65)
        self.assertEqual(len(set(SOURCE_COLUMNS)), 65)
        self.assertEqual(SOURCE_COLUMNS[0], "Order ID")
        self.assertEqual(SOURCE_COLUMNS[-1], "Tax Info - Address Type")

    def test_private_fields_are_empty(self):
        row = make_order(1)
        for name in (
            "Phone #", "Detail Address", "Additional address information",
            "Tax Info - Buyer Tax ID", "Tax Info - Email",
            "Tax Info - Phone Number", "Tax Info - Registered Address",
            "Buyer Message", "Seller Note",
        ):
            self.assertEqual(row[name], "")

    def test_identifiers_and_nonnegative_amounts(self):
        for i in range(1, 101):
            row = make_order(i)
            self.assertTrue(row["Order ID"].startswith("SYN-ORDER-"))
            self.assertTrue(row["SKU ID"].startswith("SYN-SKU-"))
            self.assertGreater(int(row["Quantity"]), 0)
            self.assertGreater(float(row["Order Amount"]), 0)

    def test_csv_roundtrip_and_unique_business_keys(self):
        with tempfile.TemporaryDirectory() as td:
            out = generate_orders(120, Path(td) / "orders.csv")
            with out.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                self.assertEqual(tuple(reader.fieldnames or ()), SOURCE_COLUMNS)
                rows = list(reader)
            self.assertEqual(len(rows), 120)
            pairs = {(r["Order ID"], r["SKU ID"]) for r in rows}
            self.assertEqual(len(pairs), len(rows))
            self.assertTrue(all(r["Buyer Username"].startswith("synthetic_")
                                for r in rows))

    def test_generation_is_deterministic(self):
        with tempfile.TemporaryDirectory() as td:
            left = generate_orders(15, Path(td) / "a.csv")
            right = generate_orders(15, Path(td) / "b.csv")
            self.assertEqual(left.read_bytes(), right.read_bytes())

    def test_invalid_row_counts_fail_early(self):
        with tempfile.TemporaryDirectory() as td:
            for value in (0, -1, 10001, True):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    generate_orders(value, Path(td) / "bad.csv")
            self.assertFalse((Path(td) / "bad.csv").exists())


if __name__ == "__main__":
    unittest.main()
