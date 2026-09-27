#!/usr/bin/env python3
"""Summarize scheduler RBAC self-security tests."""

from pathlib import Path
ARTIFACT_ROOT = Path(__file__).resolve().parents[1]

import argparse
import csv
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(ARTIFACT_ROOT / "results/raw/aks/scheduler_security_tests.csv"))
    ap.add_argument("--output", default=str(ARTIFACT_ROOT / "results/tables/scheduler_security.csv"))
    args = ap.parse_args()

    rows = []
    if os.path.exists(args.input):
        with open(args.input, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    passed = sum(1 for row in rows if row.get("pass") == "PASS")
    total = len(rows)
    env = rows[0].get("env", "") if rows else ""
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "passed", "total", "pass_rate", "status"])
        w.writerow([env, passed, total, round(passed / total, 4) if total else "", "PASS" if total and passed == total else "FAIL"])
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
