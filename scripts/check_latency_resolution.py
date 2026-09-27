#!/usr/bin/env python3
"""Fail if supposedly high-resolution latency values look 1s-quantized."""

import argparse
import csv
import math
import sys


def numeric_values(rows, columns):
    values = []
    for row in rows:
        if str(row.get("run_id", "")).startswith("warmup"):
            continue
        if str(row.get("status", "")).upper() == "NOT_EXECUTED":
            continue
        if str(row.get("success", "")).lower() == "failure":
            continue
        for col in columns:
            raw = (row.get(col) or "").strip()
            if not raw:
                continue
            try:
                values.append(float(raw))
            except ValueError:
                continue
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument(
        "--columns",
        default="scheduler_total_latency_ms,sched_total_precise_ms",
        help="Comma-separated latency columns to inspect.",
    )
    parser.add_argument("--threshold", type=float, default=0.90)
    args = parser.parse_args()

    with open(args.csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    values = numeric_values(rows, [c.strip() for c in args.columns.split(",") if c.strip()])
    if not values:
        print(f"FAIL: no numeric high-resolution latency values in {args.csv}", file=sys.stderr)
        return 1

    multiples = [
        v
        for v in values
        if v > 0 and math.isclose(v % 1000.0, 0.0, abs_tol=1e-9)
    ]
    ratio = len(multiples) / len(values)
    print(
        f"latency_resolution_check file={args.csv} n={len(values)} "
        f"multiples_of_1000={len(multiples)} ratio={ratio:.3f}"
    )
    if ratio > args.threshold:
        print(
            f"FAIL: {ratio:.1%} of latency values are exact multiples of 1000 ms",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
