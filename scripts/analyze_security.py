#!/usr/bin/env python3
"""Aggregate Article-1 attack CSVs by attack_id."""

import argparse
import csv
import glob
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--env", default="")
    args = ap.parse_args()

    rows = []
    for pattern in args.inputs:
        for path in glob.glob(pattern):
            with open(path, newline="", encoding="utf-8") as f:
                rows.extend(csv.DictReader(f))

    by_attack = {}
    for row in rows:
        if row.get("status") == "NOT_EXECUTED":
            continue
        attack = row.get("attack_id", "")
        if not attack:
            continue
        by_attack.setdefault(attack, [0, 0])
        by_attack[attack][1] += 1
        if str(row.get("blocked", "")).lower() in ("yes", "true"):
            by_attack[attack][0] += 1

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "attack_id", "blocked", "total", "block_rate"])
        for attack in sorted(by_attack):
            blocked, total = by_attack[attack]
            w.writerow([args.env, attack, blocked, total, round(blocked / total, 4) if total else ""])
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
