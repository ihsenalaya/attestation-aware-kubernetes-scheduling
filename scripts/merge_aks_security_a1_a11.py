#!/usr/bin/env python3
"""Merge AKS security campaign rows into an honest A1-A11 matrix."""

from pathlib import Path
ARTIFACT_ROOT = Path(__file__).resolve().parents[1]

import csv
import datetime as dt
import os


OUT_FIELDS = [
    "timestamp",
    "env",
    "run_id",
    "attack_id",
    "adversary",
    "scenario",
    "expected",
    "actual",
    "blocked",
    "status",
    "reason",
    "raw_log_path",
]


def now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def normalize_core(row):
    return {
        "timestamp": row.get("timestamp", ""),
        "env": row.get("env", "aks-real-sevsnp"),
        "run_id": row.get("run_id", ""),
        "attack_id": row.get("attack_id", ""),
        "adversary": row.get("adversary", ""),
        "scenario": row.get("scenario", ""),
        "expected": row.get("expected", ""),
        "actual": row.get("actual", ""),
        "blocked": row.get("blocked", ""),
        "status": row.get("status") or "EXECUTED",
        "reason": row.get("reason", ""),
        "raw_log_path": row.get("raw_log_path", ""),
    }


def not_executed(attack_id, reason):
    return {
        "timestamp": now(),
        "env": "aks-real-sevsnp",
        "run_id": "not-executed",
        "attack_id": attack_id,
        "adversary": "adversarial-principal",
        "scenario": f"{attack_id} AKS rerun required",
        "expected": "BLOCKED",
        "actual": "NOT_EXECUTED",
        "blocked": "no",
        "status": "NOT_EXECUTED",
        "reason": reason,
        "raw_log_path": "",
    }


def main():
    raw_dir = str(ARTIFACT_ROOT / "results/raw/aks")
    rows = [normalize_core(row) for row in read(os.path.join(raw_dir, "security_attacks_A1_A10.csv"))]
    rows.extend(read(os.path.join(raw_dir, "security_attacks_missing_A2_A3_A6_A7_A9.csv")))
    for row in read(os.path.join(raw_dir, "a11_gpu_required_no_evidence.csv")):
        rows.append({
            "timestamp": row.get("timestamp", ""),
            "env": row.get("env", "aks-real-sevsnp"),
            "run_id": "run-1" if row.get("status") == "EXECUTED" else "not-executed",
            "attack_id": row.get("attack_id", "A11"),
            "adversary": "adversarial-principal",
            "scenario": row.get("scenario", "gpu-required-no-evidence"),
            "expected": row.get("expected", "BLOCKED"),
            "actual": row.get("actual", ""),
            "blocked": row.get("blocked", ""),
            "status": row.get("status", ""),
            "reason": row.get("reason", ""),
            "raw_log_path": row.get("raw_log_path", ""),
        })

    if not any(row.get("attack_id") == "A11" for row in rows):
        rows.append(not_executed("A11", "confidential_gpu_out_of_scope_no_aks_gpu_attestation"))

    out = os.path.join(raw_dir, "security_attacks_A1_A11.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in OUT_FIELDS} for row in rows)
    print(f"wrote {out} rows={len(rows)}")


if __name__ == "__main__":
    main()
