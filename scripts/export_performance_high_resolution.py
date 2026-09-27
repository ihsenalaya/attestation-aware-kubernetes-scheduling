#!/usr/bin/env python3
"""Export the Article-1 high-resolution performance raw CSV.

The source is the scheduling harness CSV. If no monotonic scheduler timing is
present, the script emits a single NOT_EXECUTED row instead of inventing data.
"""

import argparse
import csv
import datetime as dt
import os
import sys


FIELDS = [
    "timestamp",
    "env",
    "run_id",
    "seed",
    "baseline",
    "scenario",
    "node",
    "admission_latency_ms",
    "prefilter_latency_ms",
    "filter_latency_ms",
    "score_latency_ms",
    "permit_wait_ms",
    "reserve_latency_ms",
    "prebind_latency_ms",
    "bind_latency_ms",
    "scheduler_total_latency_ms",
    "pod_pending_duration_ms",
    "pod_admission_to_running_ms",
    "success",
    "status",
    "reason",
    "raw_log_path",
]


def pick(row, *names):
    for name in names:
        value = (row.get(name) or "").strip()
        if value:
            return value
    return ""


def has_high_resolution(row):
    return bool(
        pick(row, "scheduler_total_latency_ms", "sched_total_precise_ms")
        and pick(row, "filter_latency_ms", "filter_precise_ms")
        and pick(row, "bind_latency_ms", "bind_precise_ms")
    )


def not_executed(env, reason):
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = {field: "" for field in FIELDS}
    row.update(
        {
            "timestamp": now,
            "env": env,
            "run_id": "not-executed",
            "baseline": "B5-proposed",
            "scenario": "high-resolution-scheduling",
            "success": "failure",
            "status": "NOT_EXECUTED",
            "reason": reason,
        }
    )
    return row


def convert_row(row, env):
    out = {field: "" for field in FIELDS}
    out.update(
        {
            "timestamp": pick(row, "timestamp"),
            "env": pick(row, "env") or env,
            "run_id": pick(row, "run_id"),
            "seed": pick(row, "seed"),
            "baseline": pick(row, "baseline") or "B5-proposed",
            "scenario": pick(row, "scenario") or "high-resolution-scheduling",
            "node": pick(row, "node"),
            "admission_latency_ms": pick(row, "admission_latency_ms"),
            "prefilter_latency_ms": pick(row, "prefilter_latency_ms"),
            "filter_latency_ms": pick(row, "filter_latency_ms", "filter_precise_ms"),
            "score_latency_ms": pick(row, "score_latency_ms", "score_precise_ms"),
            "permit_wait_ms": pick(row, "permit_wait_ms"),
            "reserve_latency_ms": pick(row, "reserve_latency_ms"),
            "prebind_latency_ms": pick(row, "prebind_latency_ms", "reserve_prebind_precise_ms"),
            "bind_latency_ms": pick(row, "bind_latency_ms", "bind_precise_ms"),
            "scheduler_total_latency_ms": pick(row, "scheduler_total_latency_ms", "sched_total_precise_ms"),
            "pod_pending_duration_ms": pick(row, "pod_pending_duration_ms", "pending_ms"),
            "pod_admission_to_running_ms": pick(row, "pod_admission_to_running_ms", "admission_to_running_ms"),
            "success": pick(row, "success") or "success",
            "status": "EXECUTED",
            "reason": "",
            "raw_log_path": pick(row, "raw_log_path"),
        }
    )
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument(
        "--not-executed-reason",
        default="high_resolution_scheduler_fields_absent_in_source",
    )
    args = parser.parse_args()

    rows = []
    if os.path.exists(args.input):
        with open(args.input, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    out_rows = [convert_row(row, args.env) for row in rows if has_high_resolution(row)]
    if not out_rows:
        out_rows = [not_executed(args.env, args.not_executed_reason)]

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(out_rows)

    executed = sum(1 for row in out_rows if row["status"] == "EXECUTED")
    print(f"wrote {args.output} rows={len(out_rows)} executed={executed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
