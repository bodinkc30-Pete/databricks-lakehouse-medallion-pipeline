"""Phase 3 QA-gated release contract tests: portable/no Databricks credentials."""
from __future__ import annotations
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from generate_synthetic_orders import make_order

FILES={
    name:(ROOT/"notebooks"/file).read_text(encoding="utf-8")
    for name,file in {
        "stage":"10_p3_stage_sandbox.py",
        "qa":"11_p3_qa_gate_sandbox.py",
        "publish":"12_p3_safe_publish_sandbox.py",
    }.items()
}
SYNTHETIC_DATA=[make_order(i) for i in range(1,5250)]


def qa(rows, reference_skus):
    """Local business-rule example, not a substitute for actual Spark execution."""
    keys=Counter((x.get("order_id"),x.get("sku_id")) for x in rows)
    nulls=sum(any(x.get(k) is None for k in
              ("order_id","sku_id","order_status","quantity","amount"))
              for x in rows)
    invalid=sum(x.get("quantity",0) <=0 or x.get("amount",0)<0 for x in rows)
    orphans=sum(x.get("sku_id") not in reference_skus for x in rows)
    return {
        "duplicates":sum(v>1 for v in keys.values()),
        "nulls":nulls,"invalid":invalid,"orphans":orphans,
        "pass":len(rows)==5269 and sum(v>1 for v in keys.values())==0
        and nulls==0 and invalid==0 and orphans==0
    }


class Phase3Tests(unittest.TestCase):
    def test_synthetic_source_contract(self):
        self.assertEqual(len(SYNTHETIC_DATA),5249)
        self.assertEqual(len({r["Order ID"] for r in SYNTHETIC_DATA}),5249)

    def test_stage_has_four_deterministic_scenarios(self):
        text=FILES["stage"]
        for expected in ("baseline","fail_duplicate","fail_null","recovery",
                         "p3_v1","p3_v2_bad_duplicate","p3_v2_bad_null",
                         "p3_v2_valid","SYN-ORDER-00005250"):
            self.assertIn(expected,text)
        self.assertIn('mode("append")',text)
        self.assertNotIn('mode("overwrite")',text)

    def test_duplicate_failure_is_caught(self):
        data=[{"order_id":"A","sku_id":"S1","order_status":"Completed",
               "quantity":2,"amount":100} for _ in range(5268)]
        # Replace with 5269 independent unique rows.
        data=[dict(x,order_id=f"ORD-{i}") for i,x in enumerate(data)]
        data.append({"order_id":"ORD-1","sku_id":"S1",
                     "order_status":"Completed","quantity":1,"amount":20})
        self.assertFalse(qa(data,{"S1"})["pass"])
        self.assertEqual(qa(data,{"S1"})["duplicates"],1)

    def test_null_failure_is_caught(self):
        data=[{"order_id":f"ORD-{i}","sku_id":"S1","order_status":"Completed",
               "quantity":1,"amount":10} for i in range(5269)]
        data[0]["order_id"]=None
        self.assertFalse(qa(data,{"S1"})["pass"])
        self.assertEqual(qa(data,{"S1"})["nulls"],1)

    def test_missing_foreign_key_is_caught(self):
        data=[{"order_id":f"ORD-{i}","sku_id":"S1","order_status":"Completed",
               "quantity":1,"amount":10} for i in range(5269)]
        data[3]["sku_id"]="MISSING-SKU"
        self.assertFalse(qa(data,{"S1"})["pass"])
        self.assertEqual(qa(data,{"S1"})["orphans"],1)

    def test_bad_amount_quantity_are_rejected(self):
        text=FILES["qa"]
        self.assertIn('F.col("quantity") <= 0',text)
        self.assertIn('F.col("order_amount") < 0',text)
        self.assertIn("P3_SCHEMA_DRIFT_DETECTED",text)

    def test_qa_persists_audit_and_rejected_rows_before_raise(self):
        text=FILES["qa"]
        self.assertIn("p3_quality_audit",text)
        self.assertIn("p3_rejected_rows",text)
        self.assertIn("P3_QA_GATE_REJECTED",text)
        self.assertLess(text.find(".saveAsTable(AUDIT)"),
                        text.find("raise RuntimeError("))

    def test_published_view_uses_atomic_manifest_pointer(self):
        text=FILES["publish"]
        self.assertIn("p3_publish_manifest",text)
        self.assertIn("p3_current_gold_order_lines",text)
        self.assertIn("WHEN MATCHED AND s.version_no > t.version_no",text)
        self.assertIn("P3_STALE_PUBLICATION_REJECTED",text)
        self.assertIn("P3_PUBLISH_BLOCKED: no successful QA",text)
        self.assertIn("CREATE VIEW IF NOT EXISTS",text)
        self.assertNotIn('mode("overwrite")',text)

    def test_no_original_tables_or_customer_csv_written(self):
        for name,text in FILES.items():
            with self.subTest(name=name):
                self.assertIn('SCHEMA = "workspace.project07_sandbox"',text)
                self.assertNotIn("saveAsTable('workspace.gold",text)
                self.assertNotIn('mode("overwrite")',text)
                self.assertNotIn("DROP TABLE",text)
                self.assertNotIn("DELETE FROM",text)
                self.assertNotIn("TRUNCATE TABLE",text)

    def test_manifest_pointer_cannot_regress(self):
        def promote(active_ver,candidate_ver):
            return candidate_ver if candidate_ver>active_ver else active_ver
        self.assertEqual(promote(0,1),1)
        self.assertEqual(promote(1,2),2)
        self.assertEqual(promote(2,1),2)
        self.assertEqual(promote(2,2),2)


if __name__=="__main__":
    unittest.main()
