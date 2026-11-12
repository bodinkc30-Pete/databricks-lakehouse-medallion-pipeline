"""Collect sanitized real Databricks Jobs run metadata for Project 07 monitoring.

Reads through an authenticated Databricks CLI profile; never serializes email,
credentials, raw stack traces, arbitrary task parameters or customer records.
Example:
  python src/p4_collect_job_runs.py --cli <databricks.exe> --profile <profile> \
      --output data/p4_job_events.jsonl
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

P3_JOB_ID = 824168112776513
P3_RUN_IDS = (
    856548117138939,   # baseline good
    392396837723218,   # duplicate injection
    1116006042981211,  # null injection
    889964702448996,   # recovered
)
SCENARIOS = ("baseline", "fail_duplicate", "fail_null", "recovery")
ALLOWED_TASK_KEYS = frozenset(
    ("p3_stage_snapshot", "p3_quality_gate", "p3_publish_gold")
)


def summarize_run(raw: dict[str, Any], scenario: str) -> dict[str, Any]:
    """Use a strict allowlist; no PII or stack traces in the export."""
    if int(raw["job_id"]) != P3_JOB_ID:
        raise ValueError("Unexpected Databricks Job ID")
    if raw["state"]["result_state"] not in ("SUCCESS", "FAILED"):
        raise ValueError("Run must be completed before monitoring")
    tasks = raw.get("tasks", [])
    if not tasks:
        raise ValueError("Missing Job task metadata")
    if any(t.get("task_key") not in ALLOWED_TASK_KEYS for t in tasks):
        raise ValueError("Unknown task name in job")
    final_result = raw["state"]["result_state"]
    qa_failed = any(
        t["task_key"] == "p3_quality_gate"
        and t["state"].get("result_state") == "FAILED"
        for t in tasks
    )
    skipped = any(
        t["task_key"] == "p3_publish_gold"
        and t["state"].get("life_cycle_state") == "SKIPPED"
        for t in tasks
    )
    successful_publish = any(
        t["task_key"] == "p3_publish_gold"
        and t["state"].get("result_state") == "SUCCESS"
        for t in tasks
    )
    if final_result == "FAILED" and not (qa_failed and skipped):
        raise ValueError("Failure run does not match expected QA incident")
    if final_result == "SUCCESS" and not successful_publish:
        raise ValueError("Successful run did not publish Gold")
    failed_attempts = sum(
        t["task_key"] == "p3_quality_gate"
        and t["state"].get("result_state") == "FAILED"
        for t in tasks
    )
    failed_task = "p3_quality_gate" if qa_failed else ""
    duration = int(raw["run_duration"])
    started = int(raw["start_time"])
    ended = int(raw["end_time"])
    if duration < 0 or started <= 0 or ended <= started:
        raise ValueError("Invalid run timestamps")
    as_utc = lambda milliseconds: datetime.fromtimestamp(
        milliseconds / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "job_id": int(raw["job_id"]),
        "run_id": int(raw["run_id"]),
        "scenario": scenario,
        "started_at_utc": as_utc(started),
        "ended_at_utc": as_utc(ended),
        "run_duration_ms": duration,
        "result_state": final_result,
        "failed_task": failed_task,
        "failed_qa_attempts": failed_attempts,
        "gold_publish_skipped": skipped,
        "gold_publish_succeeded": successful_publish,
        "is_synthetic": True,
    }


def load_run(cli: str, profile: str, run_id: int) -> dict[str, Any]:
    cmd = [
        cli, "jobs", "get-run", str(run_id), "--profile", profile,
        "--output", "json",
    ]
    res = subprocess.run(
        cmd, capture_output=True, text=True, timeout=35, check=False,
    )
    if res.returncode:
        raise RuntimeError(
            f"Databricks Jobs API failed for run {run_id}; credentials not logged"
        )
    return json.loads(res.stdout)


def collect(cli: str, profile: str) -> list[dict[str, Any]]:
    rows = []
    for run_id, scenario in zip(P3_RUN_IDS, SCENARIOS):
        row = summarize_run(load_run(cli, profile, run_id), scenario)
        if row["run_id"] != run_id:
            raise ValueError("Response run_id mismatch")
        rows.append(row)
    if len({row["run_id"] for row in rows}) != 4:
        raise ValueError("Duplicate run_id")
    if [row["result_state"] for row in rows] != [
        "SUCCESS", "FAILED", "FAILED", "SUCCESS"
    ]:
        raise ValueError("Unexpected failure/recovery sequence")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    rows = collect(args.cli, args.profile)
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    print("P4_COLLECT_RESULT=PASS")
    print("P4_COLLECT_COUNT=" + str(len(rows)))
    for row in rows:
        print(
            "P4_RUN=" + str(row["run_id"])
            + " scenario=" + row["scenario"]
            + " result=" + row["result_state"]
            + " duration_ms=" + str(row["run_duration_ms"])
            + " failed_task=" + row["failed_task"]
        )
    print("P4_EXPORT_PATH=" + str(destination))


if __name__ == "__main__":
    main()
