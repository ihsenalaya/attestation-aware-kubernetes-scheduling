#!/usr/bin/env python3
"""Summarize fine-grained ablation rows."""

from pathlib import Path
ARTIFACT_ROOT = Path(__file__).resolve().parents[1]

import argparse
import csv
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(ARTIFACT_ROOT / "results/raw/aks/ablation.csv"))
    ap.add_argument("--output", default=str(ARTIFACT_ROOT / "results/tables/ablation.csv"))
    args = ap.parse_args()

    rows = []
    if os.path.exists(args.input):
        with open(args.input, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "layer", "configuration", "executed", "blocked", "total", "status"])
        by_layer = {}
        for row in rows:
            config = row.get("configuration") or row.get("config", "")
            actual = (row.get("actual") or row.get("status") or "").strip()
            executed = actual not in {"", "NOT_EXECUTED", "REQUIRES_BUILD_FLAG", "REQUIRES_E2E"}
            key = (row.get("env", ""), row.get("layer", ""), config)
            by_layer.setdefault(key, [0, 0, 0, ""])
            if executed:
                by_layer[key][0] += 1
                by_layer[key][2] += 1
                if row.get("blocked") == "yes":
                    by_layer[key][1] += 1
            else:
                by_layer[key][3] = actual or "NOT_EXECUTED"
        for (env, layer, config), vals in sorted(by_layer.items()):
            executed, blocked, total, status = vals
            w.writerow([env, layer, config, executed, blocked, total, "EXECUTED" if executed else status])
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
