#!/usr/bin/env python3
"""analyze_all.py — reproducible stats + figures for the Article-1 campaign.

Consumes raw CSVs under --raw and produces summary tables under --tables and PDF
figures under --figures. Statistics: median, p50/p95/p99, bootstrap 95% CI, and
Mann-Whitney U for B4-vs-B5 comparisons when both are present.

HONEST: warm-up rows (run_id starts with 'warmup') are excluded from stats but
retained in the raw file. Nothing is fabricated; missing inputs -> skipped with a
note, never invented.
"""
import argparse
import csv
import glob
import math
import os
import random
import statistics
import sys

import numpy as np

try:
    from scipy import stats as scistats
    HAVE_SCIPY = True
except Exception:
    HAVE_SCIPY = False


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_floats(rows, col):
    out = []
    for r in rows:
        v = (r.get(col) or "").strip()
        if not v:
            continue
        try:
            out.append(float(v))
        except ValueError:
            pass
    return np.array(out, dtype=float)


def fnum(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def b1b5_comparable_latency(row):
    """One client-observed scheduling-path metric for every B1--B5 baseline."""
    direct = fnum(row.get("client_scheduling_latency_ms"))
    if direct is not None:
        return direct
    if row.get("baseline") == "B5":
        pending = fnum(row.get("pod_pending_duration_ms"))
        admission = fnum(row.get("admission_latency_ms"))
        if pending is not None and admission is not None:
            return pending - admission
    total = fnum(row.get("scheduler_total_latency_ms"))
    if total is not None:
        return total
    pending = fnum(row.get("pod_pending_duration_ms"))
    admission = fnum(row.get("admission_latency_ms"))
    if pending is not None and admission is not None:
        return pending - admission
    return pending


def b1b5_internal_latency(row):
    direct = fnum(row.get("scheduler_internal_latency_ms"))
    if direct is not None:
        return direct
    if row.get("baseline") == "B5":
        # Backward compatibility: older raw B5 rows stored the in-process
        # scheduler phase in scheduler_total_latency_ms.
        return fnum(row.get("scheduler_total_latency_ms"))
    return None


def b1b5_success(row):
    return row.get("success") == "success" and row.get("status") != "NOT_EXECUTED"


def pct_list(xs, pct):
    if not xs:
        return ""
    xs = sorted(xs)
    k = (len(xs) - 1) * pct / 100.0
    lo = int(np.floor(k))
    hi = int(np.ceil(k))
    if lo == hi:
        return xs[lo]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)


def bootstrap_median_ci(xs, reps=2000):
    if not xs:
        return "", ""
    rnd = random.Random(20260706)
    meds = []
    for _ in range(reps):
        sample = [xs[rnd.randrange(len(xs))] for _ in xs]
        meds.append(statistics.median(sample))
    return pct_list(meds, 2.5), pct_list(meds, 97.5)


def mann_whitney_p(a, b):
    if not a or not b:
        return ""
    values = [(x, 0) for x in a] + [(x, 1) for x in b]
    values.sort(key=lambda item: item[0])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(values):
        j = i + 1
        while j < len(values) and values[j][0] == values[i][0]:
            j += 1
        rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[k] = rank
        i = j
    r1 = sum(rank for rank, (_, group) in zip(ranks, values) if group == 0)
    n1, n2 = len(a), len(b)
    u1 = r1 - n1 * (n1 + 1) / 2.0
    mean = n1 * n2 / 2.0
    var = n1 * n2 * (n1 + n2 + 1) / 12.0
    if var <= 0:
        return ""
    z = abs((u1 - mean) / math.sqrt(var))
    return float(math.erfc(z / math.sqrt(2)))


def bootstrap_ci(x, iters=10000, alpha=0.05, seed=42):
    if len(x) < 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    means = np.array([rng.choice(x, size=len(x), replace=True).mean() for _ in range(iters)])
    lo = float(np.percentile(means, 100 * alpha / 2))
    hi = float(np.percentile(means, 100 * (1 - alpha / 2)))
    return (lo, hi)


def summarize(x):
    if len(x) == 0:
        return dict(n=0, median="", p50="", p95="", p99="", mean="", ci_lo="", ci_hi="")
    lo, hi = bootstrap_ci(x)
    return dict(
        n=len(x),
        median=round(float(np.median(x)), 3),
        p50=round(float(np.percentile(x, 50)), 3),
        p95=round(float(np.percentile(x, 95)), 3),
        p99=round(float(np.percentile(x, 99)), 3),
        mean=round(float(np.mean(x)), 3),
        ci_lo=round(lo, 3),
        ci_hi=round(hi, 3),
    )


def exclude_warmup(rows):
    return [r for r in rows if not str(r.get("run_id", "")).startswith("warmup")]


def attack_sort_key(attack_id):
    s = str(attack_id)
    if s.startswith("A"):
        suffix = s[1:]
        digits = "".join(ch for ch in suffix if ch.isdigit())
        rest = suffix[len(digits):]
        if digits:
            return (int(digits), rest)
    return (10**9, s)


def analyze_b1_b5_performance(raw_dir, tables_dir):
    path = os.path.join(raw_dir, "performance_b1_b5_high_resolution.csv")
    if not os.path.exists(path):
        return None
    rows = read_rows(path)
    groups = {b: [] for b in ["B1", "B2", "B3", "B4", "B5"]}
    internal_groups = {b: [] for b in ["B1", "B2", "B3", "B4", "B5"]}
    for row in rows:
        if str(row.get("warmup", "")).lower() == "true" or not b1b5_success(row):
            continue
        baseline = row.get("baseline")
        if baseline not in groups:
            continue
        value = b1b5_comparable_latency(row)
        if value is not None:
            groups[baseline].append(float(value))
        internal = b1b5_internal_latency(row)
        if internal is not None:
            internal_groups[baseline].append(float(internal))

    out = os.path.join(tables_dir, "performance.csv")
    b1_med = statistics.median(groups["B1"]) if groups["B1"] else None
    b4_med = statistics.median(groups["B4"]) if groups["B4"] else None
    b5 = groups["B5"]
    fields = [
        "baseline", "n", "median_ms", "p95_ms", "p99_ms", "ci95_low", "ci95_high",
        "overhead_vs_B1_ms", "overhead_vs_B1_pct", "overhead_vs_B4_ms",
        "mannwhitney_p", "measurement_basis", "scheduler_internal_median_ms",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for baseline in ["B1", "B2", "B3", "B4", "B5"]:
            xs = groups[baseline]
            if not xs:
                writer.writerow({"baseline": baseline, "n": 0})
                continue
            med = statistics.median(xs)
            lo, hi = bootstrap_median_ci(xs)
            overhead_b1 = med - b1_med if b1_med is not None else ""
            overhead_b1_pct = overhead_b1 / b1_med * 100.0 if b1_med not in (None, 0, "") else ""
            overhead_b4 = med - b4_med if b4_med is not None else ""
            p = mann_whitney_p(b5, xs) if baseline in ("B1", "B4") and b5 else ""
            internal = internal_groups[baseline]
            writer.writerow({
                "baseline": baseline,
                "n": len(xs),
                "median_ms": round(med, 3),
                "p95_ms": round(pct_list(xs, 95), 3),
                "p99_ms": round(pct_list(xs, 99), 3),
                "ci95_low": round(lo, 3) if lo != "" else "",
                "ci95_high": round(hi, 3) if hi != "" else "",
                "overhead_vs_B1_ms": round(overhead_b1, 3) if overhead_b1 != "" else "",
                "overhead_vs_B1_pct": round(overhead_b1_pct, 3) if overhead_b1_pct != "" else "",
                "overhead_vs_B4_ms": round(overhead_b4, 3) if overhead_b4 != "" else "",
                "mannwhitney_p": round(p, 8) if p != "" else "",
                "measurement_basis": "client_observed_scheduling_path",
                "scheduler_internal_median_ms": round(statistics.median(internal), 3) if internal else "",
            })

    values = [v for xs in groups.values() for v in xs]
    multiple_ratio = sum(1 for v in values if abs(v % 1000.0) < 1e-9) / len(values) if values else 1.0
    quality_path = os.path.join(tables_dir, "performance_b1_b5_quality.json")
    with open(quality_path, "w", encoding="utf-8") as f:
        import json
        json.dump({
            "raw_csv": path,
            "table_csv": out,
            "measurement_basis": "client_observed_scheduling_path",
            "b5_scheduler_internal_metric": "reported_separately_not_used_for_b1_b5_cdf",
            "b5_scheduler_internal_median_ms": round(statistics.median(internal_groups["B5"]), 3) if internal_groups["B5"] else "",
            "measured_success_values": len(values),
            "multiples_of_1000_ratio": multiple_ratio,
            "anti_quantization_status": "PASS" if multiple_ratio <= 0.90 else "FAIL",
        }, f, indent=2)
    print(f"  [b1-b5] wrote {out} and {quality_path} (n_values={len(values)})")
    return {"kind": "b1b5", "path": path, "rows": rows, "groups": groups}


def analyze_scheduling(raw_dir, tables_dir, env):
    b1b5 = analyze_b1_b5_performance(raw_dir, tables_dir)
    if b1b5:
        return b1b5
    path = os.path.join(raw_dir, "scheduling_latency.csv")
    if not os.path.exists(path):
        print(f"  [scheduling] no {path}, skipped")
        return None
    rows = exclude_warmup(read_rows(path))
    metrics = ["pending_ms", "admission_to_running_ms", "filter_score_ms",
               "reserve_bind_ms", "sched_total_ms",
               # precise ms-resolution scheduler-phase metrics (monotonic clock):
               "prefilter_latency_ms", "filter_latency_ms", "score_latency_ms",
               "permit_wait_ms", "reserve_latency_ms", "prebind_latency_ms",
               "bind_latency_ms", "scheduler_total_latency_ms",
               "filter_precise_ms", "score_precise_ms",
               "reserve_prebind_precise_ms", "bind_precise_ms",
               "sched_total_precise_ms"]
    out = os.path.join(tables_dir, "performance.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "metric", "n", "median", "p50", "p95", "p99", "mean", "ci95_lo", "ci95_hi"])
        summ = {}
        for m in metrics:
            s = summarize(to_floats(rows, m))
            summ[m] = s
            w.writerow([env, m, s["n"], s["median"], s["p50"], s["p95"], s["p99"], s["mean"], s["ci_lo"], s["ci_hi"]])
    print(f"  [scheduling] wrote {out} (n_rows={len(rows)})")
    return {"kind": "legacy", "path": path, "rows": rows, "metrics": metrics}


def analyze_security(raw_dir, tables_dir, env):
    # Prefer the merged AKS matrix so A2/A3/A6/A7/A9 reruns are included.
    # A11 is a separate fail-closed GPU-scope check, not confidential-GPU
    # evaluation evidence, so it is intentionally excluded from the default
    # security figure/table.
    merged_path = os.path.join(raw_dir, "security_attacks_A1_A11.csv")
    core_path = os.path.join(raw_dir, "security_attacks_A1_A10.csv")
    path = merged_path if os.path.exists(merged_path) else core_path
    if not os.path.exists(path):
        print(f"  [security] no security_attacks_A1_A11.csv or security_attacks_A1_A10.csv under {raw_dir}, skipped")
        return
    rows = [
        r for r in read_rows(path)
        if r.get("status") != "NOT_EXECUTED" and r.get("attack_id") != "A11"
    ]
    by_attack = {}
    for r in rows:
        a = r.get("attack_id", "?")
        by_attack.setdefault(a, [0, 0])
        by_attack[a][1] += 1
        if str(r.get("blocked", "")).lower() == "yes":
            by_attack[a][0] += 1
    out = os.path.join(tables_dir, "security.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["env", "attack_id", "blocked", "total", "block_rate"])
        for a in sorted(by_attack, key=attack_sort_key):
            b, t = by_attack[a]
            w.writerow([env, a, b, t, round(b / t, 4) if t else 0])
    print(f"  [security] wrote {out} ({len(by_attack)} attacks)")
    return {"path": path, "by_attack": by_attack}


def analyze_b4_b5(raw_dir, tables_dir):
    path = os.path.join(raw_dir, "b4_vs_b5.csv")
    if not os.path.exists(path):
        print(f"  [b4_vs_b5] no {path}, skipped (needs B4 baseline run)")
        return None
    rows = read_rows(path)
    b4 = to_floats([r for r in rows if r.get("baseline") == "B4"], "toctou_window_ms")
    b5 = to_floats([r for r in rows if r.get("baseline") == "B5"], "toctou_window_ms")
    out = os.path.join(tables_dir, "b4_vs_b5_stats.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "B4_median", "B5_median", "n_B4", "n_B5",
                    "mannwhitney_U", "p_value", "cliffs_delta", "effect"])
        s4, s5 = summarize(b4), summarize(b5)
        U, p, delta, eff = ("", "", "", "")
        if len(b4) > 1 and len(b5) > 1:
            if HAVE_SCIPY:
                U, p = scistats.mannwhitneyu(b4, b5, alternative="two-sided")
                U, p = round(float(U), 3), round(float(p), 6)
            delta = cliffs_delta(b4, b5)
            mag = abs(delta)
            eff = ("negligible" if mag < 0.147 else "small" if mag < 0.33
                   else "medium" if mag < 0.474 else "large")
            delta = round(delta, 3)
        w.writerow(["toctou_window_ms", s4["median"], s5["median"],
                    len(b4), len(b5), U, p, delta, eff])
    print(f"  [b4_vs_b5] wrote {out} (Cliff's delta={delta}, {eff})")
    return {"path": path, "rows": rows}


def cliffs_delta(a, b):
    """Cliff's delta effect size: P(a>b) - P(a<b), in [-1,1]."""
    import numpy as _np
    a = _np.asarray(a); b = _np.asarray(b)
    gt = sum((a[:, None] > b[None, :]).sum(axis=1))
    lt = sum((a[:, None] < b[None, :]).sum(axis=1))
    n = len(a) * len(b)
    return (gt - lt) / n if n else 0.0


def make_figures(sched, security, b4b5, figures_dir, env):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"  [figures] matplotlib unavailable: {e}")
        return
    os.makedirs(figures_dir, exist_ok=True)
    if sched:
        if sched.get("kind") == "b1b5":
            plt.figure(figsize=(6.4, 4.1))
            for baseline in ["B1", "B2", "B3", "B4", "B5"]:
                xs = np.sort(np.array(sched["groups"].get(baseline, []), dtype=float))
                if len(xs) == 0:
                    continue
                ys = np.arange(1, len(xs) + 1) / len(xs)
                plt.step(xs, ys, where="post", label=f"{baseline} median={np.median(xs):.1f} ms")
            plt.xlabel("Client-observed scheduling-path latency (ms)")
            plt.ylabel("Empirical CDF")
            plt.grid(True, alpha=0.25)
            plt.legend(ncol=3, fontsize=8)
            plt.tight_layout()
            out = os.path.join(figures_dir, "scheduling_latency_cdf.pdf")
            plt.savefig(out)
            plt.close()
            print(f"  [figures] wrote {out}")
        else:
            x = to_floats(exclude_warmup(sched["rows"]), "sched_total_ms")
            if len(x) > 0:
                xs = np.sort(x)
                ys = np.arange(1, len(xs) + 1) / len(xs)
                plt.figure(figsize=(5, 3.2))
                plt.plot(xs, ys, drawstyle="steps-post")
                plt.xlabel("scheduling latency (ms)")
                plt.ylabel("CDF")
                plt.title(f"Scheduling latency CDF ({env})")
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                out = os.path.join(figures_dir, "scheduling_latency_cdf.pdf")
                plt.savefig(out)
                plt.close()
                print(f"  [figures] wrote {out}")
    if security:
        attacks = sorted(security["by_attack"], key=attack_sort_key)
        rates = []
        for attack in attacks:
            blocked, total = security["by_attack"][attack]
            rates.append(blocked / total if total else 0)
        if attacks:
            plt.figure(figsize=(5.4, 3.2))
            plt.bar(attacks, rates)
            plt.ylim(0, 1.05)
            plt.ylabel("block rate")
            plt.xlabel("attack")
            plt.title(f"Attack block rate ({env})")
            plt.grid(True, axis="y", alpha=0.25)
            plt.tight_layout()
            out = os.path.join(figures_dir, "security_bar.pdf")
            plt.savefig(out)
            plt.close()
            print(f"  [figures] wrote {out}")
    if b4b5:
        rows = b4b5["rows"]
        b4 = to_floats([r for r in rows if r.get("baseline") == "B4"], "toctou_window_ms")
        b5 = to_floats([r for r in rows if r.get("baseline") == "B5"], "toctou_window_ms")
        if len(b4) > 0 and len(b5) > 0:
            plt.figure(figsize=(4.8, 3.2))
            plt.boxplot([b4, b5], tick_labels=["B4 gate", "B5 proposed"], showfliers=True)
            plt.ylabel("TOCTOU window (ms)")
            plt.title(f"B4 vs B5 TOCTOU ({env})")
            plt.grid(True, axis="y", alpha=0.25)
            plt.tight_layout()
            out = os.path.join(figures_dir, "toctou_window_b4_b5.pdf")
            plt.savefig(out)
            plt.close()
            print(f"  [figures] wrote {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--tables", required=True)
    ap.add_argument("--figures", required=True)
    ap.add_argument("--env", default="unknown")
    args = ap.parse_args()
    os.makedirs(args.tables, exist_ok=True)
    os.makedirs(args.figures, exist_ok=True)
    print(f"== analyze_all env={args.env} raw={args.raw} scipy={HAVE_SCIPY}")
    sched = analyze_scheduling(args.raw, args.tables, args.env)
    security = analyze_security(args.raw, args.tables, args.env)
    b4b5 = analyze_b4_b5(args.raw, args.tables)
    make_figures(sched, security, b4b5, args.figures, args.env)
    print("== analysis complete")


if __name__ == "__main__":
    main()
