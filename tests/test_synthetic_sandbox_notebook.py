"""Guard the real Databricks sandbox notebook against unintended writes."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "02_synthetic_orders_sandbox.py"


class SyntheticNotebookSafetyTests(unittest.TestCase):
    def test_notebook_is_source_controlled_and_mentions_synthetic_volume(self):
        source = NOTEBOOK.read_text(encoding="utf-8")
        self.assertIn("project07_portfolio_sandbox/synthetic_orders.csv", source)
        self.assertIn("DATA_CLASSIFICATION=SYNTHETIC_ONLY", source)
        self.assertIn("RESULT=PASS", source)

    def test_no_table_mutations_or_job_trigger_commands(self):
        source = NOTEBOOK.read_text(encoding="utf-8").lower()
        for forbidden in ("saveastable", ".write", "insert into", "merge into",
                          "truncate table", "drop table", "delete from",
                          "dbutils.notebook.run", "run_now", "jobs submit"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)

    def test_checks_counts_and_privacy_without_displaying_rows(self):
        source = NOTEBOOK.read_text(encoding="utf-8")
        for required in ('== 5249', '== 65', 'nonempty_phones',
                         'nonempty_addresses', 'unique_keys'):
            self.assertIn(required, source)
        self.assertNotIn("orders.show(", source)
        self.assertNotIn("display(orders", source)


if __name__ == "__main__":
    unittest.main()
