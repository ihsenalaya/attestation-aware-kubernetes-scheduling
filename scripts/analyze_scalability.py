#!/usr/bin/env python3
"""Summarize kind/KWOK scalability CSVs.

These summaries are regression/artifact outputs for Article 1; they are not
main-paper AKS security or performance evidence.
"""

from pathlib import Path
ARTIFACT_ROOT = Path(__file__).resolve().parents[1]

import argparse
import csv
import os
import statistics


def read(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def floats(rows, col):
    out = []
    for row in rows:
        if row.get("status") == "NOT_EXECUTED":
            continue
        raw = (row.get(col) or "").strip()
        if not raw:
            continue
        try:
            out.append(float(raw))
        except ValueError:
            pass
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", default=str(ARTIFACT_ROOT / "results/raw/kind/scalability_kind.csv"))
    ap.add_argument("--kwok", default=str(ARTIFACT_ROOT / "results/raw/kwok/scalability_scheduler_only.csv"))
    ap.add_argument("--output", default=str(ARTIFACT_ROOT / "results/tables/scalability.csv"))
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "scope", "size", "n", "median_makespan_ms", "median_throughput_pods_per_s", "status"])
        for scope, path in (("kind", args.kind), ("kwok", args.kwok)):
            rows = read(path)
            sizes = sorted({row.get("batch_size") or row.get("pods_count") for row in rows if row.get("batch_size") or row.get("pods_count")})
            if not sizes and rows:
                w.writerow([rows[0].get("env", ""), scope, "", 0, "", "", rows[0].get("status", "NOT_EXECUTED")])
            for size in sizes:
                group = [row for row in rows if (row.get("batch_size") or row.get("pods_count")) == size]
                makespan = floats(group, "makespan_ms")
                throughput = floats(group, "throughput_pods_per_s")
                status = "EXECUTED" if makespan else (group[0].get("status", "NOT_EXECUTED") if group else "NOT_EXECUTED")
                w.writerow([
                    group[0].get("env", "") if group else "",
                    scope,
                    size,
                    len(makespan),
                    round(statistics.median(makespan), 3) if makespan else "",
                    round(statistics.median(throughput), 3) if throughput else "",
                    status,
                ])
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
