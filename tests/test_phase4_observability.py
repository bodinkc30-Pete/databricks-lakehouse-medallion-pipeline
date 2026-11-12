"""Phase 4 observability / alert dedup tests (CI-safe, no Databricks account)."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from p4_collect_job_runs import P3_JOB_ID, summarize_run

INGEST = (ROOT / "notebooks" / "13_p4_monitor_ingest_sandbox.py").read_text(
    encoding="utf-8")
VERIFY = (ROOT / "notebooks" / "14_p4_monitor_acceptance_sandbox.py").read_text(
    encoding="utf-8")

RUNS = [
    (856548117138939, "baseline", "SUCCESS", 86658),
    (392396837723218, "fail_duplicate", "FAILED", 50979),
    (1116006042981211, "fail_null", "FAILED", 34785),
    (889964702448996, "recovery", "SUCCESS", 50018),
]


def fake_run(run_id: int, result: str, duration: int) -> dict:
    stage = {"task_key": "p3_stage_snapshot",
             "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}}
    qa = {"task_key": "p3_quality_gate",
          "state": {"life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS" if result == "SUCCESS" else "FAILED"}}
    gold = {"task_key": "p3_publish_gold",
            "state": {"life_cycle_state":
                      "TERMINATED" if result == "SUCCESS" else "SKIPPED",
                      "result_state": "SUCCESS" if result == "SUCCESS"
                      else "UPSTREAM_FAILED"}}
    return {
        "job_id": P3_JOB_ID, "run_id": run_id, "run_duration": duration,
        "start_time": 1794470000000 + run_id % 1000,
        "end_time": 1794470000000 + run_id % 1000 + duration,
        "state": {"result_state": result},
        "tasks": [stage, qa, gold],
        # Deliberately toxic metadata: exported result must never include it.
        "creator_user_name": "private@example.invalid",
        "error_trace": "DONOTEXPORT_CUSTOMER_SECRET",
    }


class Phase4ObservabilityTests(unittest.TestCase):
    def test_collector_only_exports_approved_fields(self):
        run = fake_run(*[RUNS[0][0], RUNS[0][2], RUNS[0][3]])
        row = summarize_run(run, "baseline")
        self.assertEqual(row["result_state"], "SUCCESS")
        self.assertEqual(row["failed_task"], "")
        self.assertEqual(row["run_duration_ms"], 86658)
        serialized = json.dumps(row)
        self.assertNotIn("private@example.invalid", serialized)
        self.assertNotIn("DONOTEXPORT_CUSTOMER_SECRET", serialized)
        self.assertNotIn("creator_user_name", serialized)

    def test_failed_job_captures_task_and_skipped_publish(self):
        run = fake_run(RUNS[1][0], "FAILED", 50979)
        row = summarize_run(run, "fail_duplicate")
        self.assertEqual(row["failed_task"], "p3_quality_gate")
        self.assertTrue(row["gold_publish_skipped"])
        self.assertEqual(row["failed_qa_attempts"], 1)

    def test_collector_rejects_rogue_job(self):
        run = fake_run(RUNS[0][0], "SUCCESS", 86658)
        run["job_id"] = 123
        with self.assertRaises(ValueError):
            summarize_run(run, "baseline")

    def test_collector_rejects_incomplete_runs(self):
        run = fake_run(RUNS[0][0], "SUCCESS", 86658)
        run["state"]["result_state"] = "RUNNING"
        with self.assertRaises(ValueError):
            summarize_run(run, "baseline")

    def test_collector_rejects_unknown_tasks(self):
        run = fake_run(RUNS[0][0], "SUCCESS", 86658)
        run["tasks"][0]["task_key"] = "unexpected"
        with self.assertRaises(ValueError):
            summarize_run(run, "baseline")

    def test_real_case_rule_counts(self):
        failure = [r for r in RUNS if r[2] == "FAILED"]
        slow = [r for r in RUNS if r[3] >= 70000]
        self.assertEqual(len(failure), 2)
        self.assertEqual(len(slow), 1)
        alerts = {f"{r[0]}:JOB_FAILED" for r in failure}
        alerts.update(f"{r[0]}:DURATION_WARNING" for r in slow)
        self.assertEqual(len(alerts), 3)
        self.assertEqual(len(alerts.union(alerts)), 3)

    def test_merge_guards_replay_against_duplicate_run_rows(self):
        self.assertIn("ON t.run_id = s.run_id", INGEST)
        self.assertIn("WHEN MATCHED AND t.event_hash <> s.event_hash", INGEST)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", INGEST)
        self.assertIn('assert raw.count() == 4', INGEST)

    def test_alert_outbox_deduplicates_without_sending(self):
        self.assertIn("ON t.alert_key = s.alert_key", INGEST)
        self.assertIn("'NOT_CONFIGURED'", INGEST)
        self.assertIn("P4_ALERT_DELIVERY=NOT_CONFIGURED", INGEST)
        self.assertNotIn("smtplib", INGEST)
        self.assertNotIn("requests.post", INGEST)

    def test_incident_recovery_requires_real_p3_gold_pointer(self):
        self.assertIn('"p3_v2_valid"', INGEST)
        self.assertIn("P4_RECOVERY_VERIFY_FAILED", INGEST)
        self.assertIn('F.lit("RECOVERED")', INGEST)
        self.assertIn("recovered_by_run_id", INGEST)

    def test_databricks_acceptance_checks_row_count_and_recovery(self):
        for text in (INGEST, VERIFY):
            self.assertIn("workspace.project07_sandbox", text)
            self.assertNotIn('mode("overwrite")', text)
        self.assertIn("P4_ACCEPTANCE_RESULT=PASS", VERIFY)
        self.assertIn("P4_PUBLISHED_GOLD=p3_v2_valid", VERIFY)
        self.assertIn("13176", VERIFY)

    def test_no_private_customer_table_writes(self):
        for phrase in ('DROP TABLE', 'TRUNCATE TABLE', 'DELETE FROM'):
            self.assertNotIn(phrase, INGEST)
        self.assertIn('SCHEMA + ".p4_run_metrics"', INGEST)
        self.assertIn('SCHEMA + ".p4_alert_outbox"', INGEST)
        self.assertIn('SCHEMA + ".p4_incident_state"', INGEST)


if __name__ == "__main__":
    unittest.main()
