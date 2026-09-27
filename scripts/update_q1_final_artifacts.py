#!/usr/bin/env python3
"""Regenerate security tables/figure and the B4/B5 feature comparison."""

from __future__ import annotations

import csv
import argparse
import sys
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_AKS = ROOT / "results" / "raw" / "aks"
RESULT_TABLES = ROOT / "results" / "tables"
PAPER_TABLES = ROOT / "paper" / "tables"
FIGURES = ROOT / "paper" / "figures"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def attack_key(attack_id: str) -> tuple[int, str]:
    if attack_id.startswith("A"):
        suffix = attack_id[1:]
        digits = "".join(ch for ch in suffix if ch.isdigit())
        rest = suffix[len(digits):]
        if digits:
            return int(digits), rest
    return 10**9, attack_id


def wilson_ci(blocked: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    phat = blocked / total
    denom = 1 + z * z / total
    center = (phat + z * z / (2 * total)) / denom
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


ATTACK_META = {
    "A1": ("label forgery", "Placement safety", "scheduler Filter"),
    "A2": ("stale evidence", "Evidence freshness", "scheduler Filter"),
    "A3": ("revoked evidence", "Evidence revocation", "scheduler Filter"),
    "A4": ("wrong TEE", "TEE binding", "scheduler Filter"),
    "A5": ("forbidden runtime", "Runtime consistency", "admission webhook"),
    "A5b": ("missing model digest", "Workload identity", "admission webhook"),
    "A6": ("policy race", "Policy consistency", "PreBind re-check"),
    "A7": ("revocation race", "Evidence freshness", "PreBind re-check"),
    "A8": ("token tamper", "Verifiable placement", "offline verifier"),
    "A9": ("simulated runtime in production", "Runtime consistency", "admission webhook"),
    "A10": ("forged raw report", "Verifier trust chain", "central verifier/RBAC"),
    "A11": ("GPU evidence unavailable", "Fail-closed unsupported TEE", "policy/scheduler denial"),
}


def generate_security_matrix() -> None:
    raw_path = RAW_AKS / "security_attacks_A1_A11.csv"
    rows = [r for r in read_csv(raw_path) if r.get("status") != "NOT_EXECUTED"]
    by_attack: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_attack[row.get("attack_id", "?")].append(row)
    out_rows = []
    for attack_id in sorted(by_attack, key=attack_key):
        attack_rows = by_attack[attack_id]
        total = len(attack_rows)
        blocked = sum(1 for r in attack_rows if r.get("blocked", "").lower() == "yes")
        lo, hi = wilson_ci(blocked, total)
        adversary, target, defense = ATTACK_META.get(attack_id, ("adversary", "unknown", "unknown"))
        out_rows.append(
            {
                "attack_id": attack_id,
                "adversary": adversary,
                "target_property": target,
                "defense_point": defense,
                "runs": total,
                "blocked": blocked,
                "block_rate": round(blocked / total, 4) if total else 0,
                "ci95_low": round(lo, 4),
                "ci95_high": round(hi, 4),
                "primary_raw_file": str(raw_path),
            }
        )
    fields = [
        "attack_id",
        "adversary",
        "target_property",
        "defense_point",
        "runs",
        "blocked",
        "block_rate",
        "ci95_low",
        "ci95_high",
        "primary_raw_file",
    ]
    write_csv(PAPER_TABLES / "security_attack_matrix.csv", fields, out_rows)
    write_csv(RESULT_TABLES / "security_attack_matrix.csv", fields, out_rows)

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        labels = [r["attack_id"] for r in out_rows]
        rates = [float(r["block_rate"]) for r in out_rows]
        runs = [int(r["runs"]) for r in out_rows]
        fig, ax = plt.subplots(figsize=(7.2, 2.8))
        image = ax.imshow([rates], aspect="auto", vmin=0, vmax=1, cmap="Greens")
        ax.set_yticks([])
        ax.set_xticks(range(len(labels)), labels, rotation=35, ha="right")
        for i, (rate, n) in enumerate(zip(rates, runs)):
            ax.text(i, 0, f"{rate:.2f}\n(n={n})", ha="center", va="center", fontsize=7)
        ax.set_title("AKS adversarial campaign block rate")
        fig.colorbar(image, ax=ax, fraction=0.025, pad=0.02)
        fig.tight_layout()
        FIGURES.mkdir(parents=True, exist_ok=True)
        fig.savefig(FIGURES / "security_attack_heatmap.pdf")
        plt.close(fig)
    except Exception as exc:
        raise RuntimeError(f"security_attack_heatmap generation failed: {exc}") from exc


def generate_b4_b5_comparison() -> None:
    raw = "results/raw/aks/b4_vs_b5.csv"
    rows = [
        {
            "feature": "real evidence check",
            "B4_schedulingGate": "yes, external controller",
            "B5_attestation_scheduler": "yes, scheduler Filter",
            "raw_evidence": raw,
        },
        {
            "feature": "freshness check",
            "B4_schedulingGate": "yes, before gate removal",
            "B5_attestation_scheduler": "yes, Filter plus PreBind",
            "raw_evidence": raw,
        },
        {
            "feature": "revocation check",
            "B4_schedulingGate": "yes, before gate removal",
            "B5_attestation_scheduler": "yes, Filter plus PreBind",
            "raw_evidence": raw,
        },
        {
            "feature": "TEE check",
            "B4_schedulingGate": "yes",
            "B5_attestation_scheduler": "yes",
            "raw_evidence": raw,
        },
        {
            "feature": "scheduler-native PreBind re-check",
            "B4_schedulingGate": "no",
            "B5_attestation_scheduler": "yes",
            "raw_evidence": raw,
        },
        {
            "feature": "gate-removal-to-bind window",
            "B4_schedulingGate": "present, measured",
            "B5_attestation_scheduler": "no externally schedulable gate-removal window",
            "raw_evidence": "results/tables/b4_vs_b5_stats.csv",
        },
        {
            "feature": "offline-verifiable placement token",
            "B4_schedulingGate": "no",
            "B5_attestation_scheduler": "yes, Ed25519 token",
            "raw_evidence": "results/raw/aks/e2e-placement-token.json",
        },
        {
            "feature": "independent verify-placement",
            "B4_schedulingGate": "no",
            "B5_attestation_scheduler": "yes",
            "raw_evidence": "results/raw/aks/e2e-verify-placement.txt",
        },
    ]
    fields = ["feature", "B4_schedulingGate", "B5_attestation_scheduler", "raw_evidence"]
    write_csv(PAPER_TABLES / "b4_b5_comparison.csv", fields, rows)


def main() -> int:
    global RAW_AKS, RESULT_TABLES, PAPER_TABLES, FIGURES
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, default=RAW_AKS)
    parser.add_argument("--tables", type=Path, default=RESULT_TABLES)
    parser.add_argument("--paper-tables", type=Path, default=PAPER_TABLES)
    parser.add_argument("--figures", type=Path, default=FIGURES)
    args = parser.parse_args()
    RAW_AKS, RESULT_TABLES, PAPER_TABLES, FIGURES = args.raw, args.tables, args.paper_tables, args.figures
    if not (RAW_AKS / "security_attacks_A1_A11.csv").is_file():
        parser.error("Missing security_attacks_A1_A11.csv in --raw")
    for path in (RESULT_TABLES, PAPER_TABLES, FIGURES):
        path.mkdir(parents=True, exist_ok=True)
    generate_security_matrix()
    generate_b4_b5_comparison()
    print("Security and B4/B5 analysis artifacts updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
