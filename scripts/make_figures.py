#!/usr/bin/env python3
"""Generate Article-1 hardening figures from summary/raw CSVs."""

from pathlib import Path
ARTIFACT_ROOT = Path(__file__).resolve().parents[1]

import argparse
import csv
import os


def read(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def attack_sort_key(row):
    attack_id = str(row.get("attack_id", ""))
    if attack_id.startswith("A"):
        suffix = attack_id[1:]
        digits = "".join(ch for ch in suffix if ch.isdigit())
        rest = suffix[len(digits):]
        if digits:
            return (int(digits), rest)
    return (10**9, attack_id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--figures", default=str(ARTIFACT_ROOT / "paper/figures"))
    ap.add_argument("--performance", default=str(ARTIFACT_ROOT / "results/raw/aks/performance_high_resolution.csv"))
    ap.add_argument("--security", default=str(ARTIFACT_ROOT / "results/tables/security.csv"))
    ap.add_argument("--scalability", default=str(ARTIFACT_ROOT / "results/tables/scalability.csv"))
    ap.add_argument("--ablation", default=str(ARTIFACT_ROOT / "results/tables/ablation.csv"))
    ap.add_argument("--scheduler-security", default=str(ARTIFACT_ROOT / "results/raw/aks/scheduler_security_tests.csv"))
    ap.add_argument("--identity", default=str(ARTIFACT_ROOT / "results/raw/aks/identity_binding.csv"))
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"matplotlib unavailable: {exc}")
        return 0

    os.makedirs(args.figures, exist_ok=True)

    perf = [
        row for row in read(args.performance)
        if row.get("status") != "NOT_EXECUTED" and not row.get("run_id", "").startswith("warmup")
    ]
    xs = sorted(v for v in (as_float(row.get("scheduler_total_latency_ms")) for row in perf) if v is not None)
    if xs:
        ys = [(i + 1) / len(xs) for i in range(len(xs))]
        plt.figure(figsize=(5, 3.2))
        plt.plot(xs, ys, drawstyle="steps-post")
        plt.xlabel("scheduler total latency (ms)")
        plt.ylabel("CDF")
        plt.grid(True, alpha=0.25)
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "performance_high_resolution_cdf.pdf"))
        plt.close()

    security = sorted(read(args.security), key=attack_sort_key)
    if security:
        labels = [row["attack_id"] for row in security]
        rates = [as_float(row.get("block_rate")) or 0 for row in security]
        plt.figure(figsize=(6, 3.2))
        plt.bar(labels, rates)
        plt.ylim(0, 1.05)
        plt.ylabel("block rate")
        plt.xlabel("attack")
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "security_bar.pdf"))
        plt.close()

    sched_sec = read(args.scheduler_security)
    if sched_sec:
        labels = [row.get("test_id", str(i)) for i, row in enumerate(sched_sec)]
        passed = [1 if row.get("pass") == "PASS" else 0 for row in sched_sec]
        plt.figure(figsize=(8, 2.6))
        plt.imshow([passed], aspect="auto", vmin=0, vmax=1)
        plt.yticks([])
        plt.xticks(range(len(labels)), labels, rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "scheduler_security_matrix.pdf"))
        plt.close()

    scalability = [row for row in read(args.scalability) if row.get("status") == "EXECUTED"]
    if scalability:
        labels = [f"{row.get('scope')}:{row.get('size')}" for row in scalability]
        makespan = [as_float(row.get("median_makespan_ms")) or 0 for row in scalability]
        plt.figure(figsize=(6, 3.2))
        plt.plot(labels, makespan, marker="o")
        plt.ylabel("median makespan (ms)")
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "scalability_line.pdf"))
        plt.close()

    ablation = read(args.ablation)
    if ablation:
        labels = [row.get("layer", "") for row in ablation]
        blocked = [as_float(row.get("blocked")) or 0 for row in ablation]
        plt.figure(figsize=(5, 3.2))
        plt.bar(labels, blocked)
        plt.ylabel("blocked executed attacks")
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "ablation_security_latency.pdf"))
        plt.close()

    identity = read(args.identity)
    if identity:
        labels = [row.get("test_id") or row.get("scenario") or row.get("case") or str(i) for i, row in enumerate(identity)]
        passed = [1 if row.get("pass") == "PASS" or row.get("result") == "PASS" else 0 for row in identity]
        plt.figure(figsize=(6, 2.8))
        plt.imshow([passed], aspect="auto", vmin=0, vmax=1)
        plt.yticks([])
        plt.xticks(range(len(labels)), labels, rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(args.figures, "identity_binding_matrix.pdf"))
        plt.close()

    print(f"figures written under {args.figures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
