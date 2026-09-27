#!/usr/bin/env python3
"""Analyze high-resolution performance CSVs."""

import argparse
import csv
import os
import statistics


METRICS = [
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
]


def values(rows, metric):
    out = []
    for row in rows:
        if row.get("status") == "NOT_EXECUTED" or str(row.get("run_id", "")).startswith("warmup"):
            continue
        raw = (row.get(metric) or "").strip()
        if not raw:
            continue
        try:
            out.append(float(raw))
        except ValueError:
            pass
    return sorted(out)


def pct(xs, p):
    if not xs:
        return ""
    k = (len(xs) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    frac = k - lo
    return round(xs[lo] * (1 - frac) + xs[hi] * frac, 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--env", default="")
    args = ap.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    env = args.env or (rows[0].get("env", "") if rows else "")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "metric", "n", "median_ms", "p95_ms", "p99_ms", "mean_ms"])
        for metric in METRICS:
            xs = values(rows, metric)
            w.writerow([
                env,
                metric,
                len(xs),
                round(statistics.median(xs), 3) if xs else "",
                pct(xs, 95),
                pct(xs, 99),
                round(statistics.fmean(xs), 3) if xs else "",
            ])
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
