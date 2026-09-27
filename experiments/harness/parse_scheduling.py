#!/usr/bin/env python3
"""parse_scheduling.py — derive per-phase scheduling latency for one pod.

Reads the attestation-scheduler JSON logs and the pod's own timestamps and emits
a single CSV row. Phase timings come from the scheduler's own structured log
lines (real, observable; no fabricated numbers):

  t_start  = "attestation-scheduler: scheduling pod"
  t_select = "attestation-scheduler: node selected"   (after Filter+Score)
  t_bound  = "attestation-scheduler: pod bound"        (after Reserve+PreBind+Bind)

filter_score_ms = t_select - t_start
reserve_bind_ms = t_bound  - t_select
sched_total_ms  = t_bound  - t_start

Pod API timestamps give admission-to-running and pending duration.
Any value that cannot be computed is left EMPTY (never guessed).
"""
import json
import sys
import datetime
import csv


def parse_ts(s):
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        try:
            base = s.split("+")[0]
            return datetime.datetime.strptime(base[:19], "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except Exception:
            return None


def ms(a, b):
    if a is None or b is None:
        return ""
    return int((b - a).total_seconds() * 1000)


def scheduler_events(log_path, pod, ns):
    """Return dict phase->timestamp for this pod from scheduler JSON logs."""
    starts, selects, bounds = [], [], []
    try:
        with open(log_path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or pod not in line:
                    continue
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("pod") != pod and e.get("Pod", {}).get("name") != pod:
                    # controller-runtime nests pod under "Pod"
                    if e.get("namespace") != ns:
                        continue
                msg = e.get("msg", "")
                ts = parse_ts(e.get("ts") or e.get("time"))
                if ts is None:
                    continue
                if "scheduling pod" in msg:
                    starts.append(ts)
                elif "node selected" in msg:
                    selects.append(ts)
                elif "pod bound" in msg:
                    bounds.append(ts)
    except FileNotFoundError:
        pass
    return (
        max(starts) if starts else None,
        max(selects) if selects else None,
        max(bounds) if bounds else None,
    )


def scheduler_phase_us(log_path, pod, ns):
    """Extract the in-process 'phase timings' log line (microseconds) for a pod.

    Returns the LAST matching record's dict of *_us fields, or {} if none. These
    are monotonic-clock, millisecond-resolution measurements of the scheduler's
    own phases, independent of Kubernetes' 1s timestamp granularity.
    """
    last = {}
    try:
        with open(log_path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or pod not in line or "phase timings" not in line:
                    continue
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("pod") != pod and e.get("Pod", {}).get("name") != pod:
                    if e.get("namespace") != ns:
                        continue
                rec = {}
                for k in ("prefilter_us", "filter_us", "permit_wait_us", "score_us",
                          "reserve_us", "prebind_us", "reserve_prebind_us",
                          "reserve_total_us", "bind_us", "total_us", "scheduler_total_us"):
                    if k in e:
                        try:
                            rec[k] = int(e[k])
                        except (TypeError, ValueError):
                            pass
                if rec:
                    last = rec
    except FileNotFoundError:
        pass
    return last


def us_to_ms(v):
    return "" if v is None else round(v / 1000.0, 3)


def phase_ms(ph, *keys):
    for key in keys:
        if key in ph:
            return us_to_ms(ph.get(key))
    return ""


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--pod", required=True)
    ap.add_argument("--ns", required=True)
    ap.add_argument("--pod-json", required=True)
    ap.add_argument("--sched-log", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--seed", required=True)
    ap.add_argument("--baseline", default="B5-proposed")
    ap.add_argument("--scenario", default="scheduling-overhead")
    args = ap.parse_args()

    with open(args.pod_json, encoding="utf-8") as f:
        pod = json.load(f)

    created = parse_ts(pod.get("metadata", {}).get("creationTimestamp"))
    node = pod.get("spec", {}).get("nodeName", "")
    scheduled = None
    ready = None
    for c in pod.get("status", {}).get("conditions", []):
        if c.get("type") == "PodScheduled" and c.get("status") == "True":
            scheduled = parse_ts(c.get("lastTransitionTime"))
        if c.get("type") in ("Ready", "ContainersReady") and c.get("status") == "True":
            r = parse_ts(c.get("lastTransitionTime"))
            if ready is None or (r and r > ready):
                ready = r

    t_start, t_select, t_bound = scheduler_events(args.sched_log, args.pod, args.ns)
    ph = scheduler_phase_us(args.sched_log, args.pod, args.ns)

    pending_ms = str(ms(created, scheduled))
    admission_to_running_ms = str(ms(created, ready))
    filter_score_ms = str(ms(t_start, t_select))
    reserve_bind_ms = str(ms(t_select, t_bound))
    sched_total_ms = str(ms(t_start, t_bound))
    scheduler_total_latency_ms = phase_ms(ph, "scheduler_total_us", "total_us")
    reserve_prebind_precise_ms = phase_ms(ph, "reserve_prebind_us", "reserve_total_us")
    prebind_latency_ms = phase_ms(ph, "prebind_us")
    if prebind_latency_ms == "":
        # Older scheduler builds emitted only the combined Reserve+PreBind field.
        # Keep this explicit fallback so old raw kind logs remain analyzable.
        prebind_latency_ms = reserve_prebind_precise_ms

    row = [
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        args.env,
        args.run_id,
        args.seed,
        args.baseline,
        args.scenario,
        node,
        pending_ms,                  # pending_ms          (K8s, 1s grain)
        admission_to_running_ms,     # admission_to_running_ms (K8s, 1s grain)
        filter_score_ms,             # filter_score_ms     (K8s log, 1s grain)
        reserve_bind_ms,             # reserve_bind_ms     (K8s log, 1s grain)
        sched_total_ms,              # sched_total_ms      (K8s log, 1s grain)
        "success" if node else "failure",
        args.sched_log,
        # Precise ms-resolution phase timings (monotonic, from scheduler):
        str(phase_ms(ph, "filter_us")),                         # filter_precise_ms
        str(phase_ms(ph, "score_us")),                          # score_precise_ms
        str(reserve_prebind_precise_ms),                         # reserve_prebind_precise_ms
        str(phase_ms(ph, "bind_us")),                           # bind_precise_ms
        str(scheduler_total_latency_ms),                         # sched_total_precise_ms
        str(ms(created, t_start)),                               # admission_latency_ms
        str(phase_ms(ph, "prefilter_us")),                       # prefilter_latency_ms
        str(phase_ms(ph, "filter_us")),                          # filter_latency_ms
        str(phase_ms(ph, "score_us")),                           # score_latency_ms
        str(phase_ms(ph, "permit_wait_us")),                     # permit_wait_ms
        str(phase_ms(ph, "reserve_us")),                         # reserve_latency_ms
        str(prebind_latency_ms),                                 # prebind_latency_ms
        str(phase_ms(ph, "bind_us")),                            # bind_latency_ms
        str(scheduler_total_latency_ms),                         # scheduler_total_latency_ms
        pending_ms,                                              # pod_pending_duration_ms
        admission_to_running_ms,                                 # pod_admission_to_running_ms
    ]
    writer = csv.writer(sys.stdout, lineterminator="")
    writer.writerow(row)


if __name__ == "__main__":
    main()
