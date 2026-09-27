#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# AKS B1-B5 scheduling latency campaign for Article 1.
#
# This script is AKS-only. It records high-resolution client-side timing for all
# baselines and scheduler phase timing for B5 from the attestation scheduler log.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source experiments/harness/lib.sh

ENV_NAME="${ENV_NAME:-aks-real-sevsnp}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
NS="${NS:-article1-performance}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
TABLE_DIR="${TABLE_DIR:-results/tables}"
FIG_DIR="${FIG_DIR:-paper/figures}"
RAW_DIR="${OUT_DIR}/performance-b1-b5"
CSV="${CSV:-${OUT_DIR}/performance_b1_b5_high_resolution.csv}"
TABLE_CSV="${TABLE_CSV:-${TABLE_DIR}/performance.csv}"
QUALITY_JSON="${QUALITY_JSON:-${OUT_DIR}/performance_b1_b5_quality.json}"
FIGURE="${FIGURE:-${FIG_DIR}/scheduling_latency_cdf.pdf}"
N_RUNS="${N_RUNS_PERFORMANCE:-30}"
WARMUP="${WARMUP:-2}"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
IMAGE="${BENCH_IMAGE:-registry.k8s.io/pause:3.9}"
EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
EVIDENCE_ANN="${EVIDENCE_ANN:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"

mkdir -p "${OUT_DIR}" "${TABLE_DIR}" "${FIG_DIR}" "${RAW_DIR}"

header="timestamp,env,run_id,warmup,baseline,scheduler,node,success,admission_latency_ms,prefilter_latency_ms,filter_latency_ms,score_latency_ms,permit_wait_ms,prebind_latency_ms,bind_latency_ms,scheduler_total_latency_ms,client_scheduling_latency_ms,scheduler_internal_latency_ms,pod_pending_duration_ms,pod_admission_to_running_ms,scheduler_cpu_millicores,scheduler_memory_mib,raw_log_path"
printf '%s\n' "${header}" > "${CSV}"

now_ms() {
  date +%s%3N
}

append_row() {
  ROW_TIMESTAMP="$1" ROW_RUN_ID="$2" ROW_WARMUP="$3" ROW_BASELINE="$4" ROW_SCHED="$5" \
  ROW_NODE="$6" ROW_SUCCESS="$7" ROW_ADMISSION="$8" ROW_PREFILTER="$9" ROW_FILTER="${10}" \
  ROW_SCORE="${11}" ROW_PERMIT="${12}" ROW_PREBIND="${13}" ROW_BIND="${14}" ROW_TOTAL="${15}" \
  ROW_CLIENT_TOTAL="${16}" ROW_INTERNAL_TOTAL="${17}" ROW_PENDING="${18}" ROW_RUNNING="${19}" \
  ROW_CPU="${20}" ROW_MEM="${21}" ROW_RAW="${22}" \
  ROW_ENV="${ENV_NAME}" ROW_CSV="${CSV}" python3 - <<'PY'
import csv, os
row = [
    os.environ["ROW_TIMESTAMP"],
    os.environ["ROW_ENV"],
    os.environ["ROW_RUN_ID"],
    os.environ["ROW_WARMUP"],
    os.environ["ROW_BASELINE"],
    os.environ["ROW_SCHED"],
    os.environ["ROW_NODE"],
    os.environ["ROW_SUCCESS"],
    os.environ["ROW_ADMISSION"],
    os.environ["ROW_PREFILTER"],
    os.environ["ROW_FILTER"],
    os.environ["ROW_SCORE"],
    os.environ["ROW_PERMIT"],
    os.environ["ROW_PREBIND"],
    os.environ["ROW_BIND"],
    os.environ["ROW_TOTAL"],
    os.environ["ROW_CLIENT_TOTAL"],
    os.environ["ROW_INTERNAL_TOTAL"],
    os.environ["ROW_PENDING"],
    os.environ["ROW_RUNNING"],
    os.environ["ROW_CPU"],
    os.environ["ROW_MEM"],
    os.environ["ROW_RAW"],
]
with open(os.environ["ROW_CSV"], "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerow(row)
PY
}

metric_to_number() {
  python3 - "$1" <<'PY'
import re, sys
s = sys.argv[1].strip()
if not s:
    print("")
    raise SystemExit(0)
m = re.fullmatch(r"([0-9.]+)([A-Za-z]+)?", s)
if not m:
    print("")
    raise SystemExit(0)
v = float(m.group(1))
u = (m.group(2) or "").lower()
if u == "n":
    v = v / 1_000_000.0
elif u == "u":
    v = v / 1000.0
elif u in ("m", ""):
    pass
elif u in ("ki", "k"):
    v = v / 1024.0
elif u in ("mi", "mib"):
    pass
elif u in ("gi", "gib"):
    v = v * 1024.0
print(round(v, 3))
PY
}

scheduler_metrics() {
  local raw cpu mem
  raw="$(kubectl -n "${PLATFORM_NS}" top pod -l app=attestation-scheduler --no-headers 2>/dev/null | head -n 1 || true)"
  if [ -z "${raw}" ]; then
    printf ','
    return 0
  fi
  cpu="$(printf '%s\n' "${raw}" | awk '{print $2}')"
  mem="$(printf '%s\n' "${raw}" | awk '{print $3}')"
  printf '%s,%s' "$(metric_to_number "${cpu}")" "$(metric_to_number "${mem}")"
}

wait_for_node() {
  local pod="$1" timeout="${2:-60}" node
  for _ in $(seq 1 "${timeout}"); do
    node="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    if [ -n "${node}" ]; then
      printf '%s\n' "${node}"
      return 0
    fi
    sleep 1
  done
  printf '\n'
  return 1
}

wait_for_running_or_succeeded() {
  local pod="$1" timeout="${2:-90}" phase
  for _ in $(seq 1 "${timeout}"); do
    phase="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Running|Succeeded) return 0 ;;
      Failed) return 1 ;;
    esac
    sleep 1
  done
  return 1
}

verified_evidence_node() {
  kubectl get attestationevidences -A -o json | REQUIRED_TEE="${REQUIRED_TEE}" EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" python3 -c '
import json, os, sys
required = os.environ["REQUIRED_TEE"].upper()
expected = os.environ.get("EXPECTED_EVIDENCE_MODE", "")
data = json.load(sys.stdin)
for item in data.get("items", []):
    spec = item.get("spec", {})
    status = item.get("status", {})
    if str(spec.get("tee", "")).upper() != required:
        continue
    if expected and status.get("evidenceMode") != expected:
        continue
    if status.get("verified") and not status.get("revoked"):
        node = spec.get("subjectRef", {}).get("name", "")
        if node:
            print(node)
            raise SystemExit(0)
raise SystemExit(1)
'
}

phase_timings_for_pod() {
  local pod="$1" raw="$2"
  kubectl logs -n "${PLATFORM_NS}" "deploy/${SCHED_DEPLOY}" --tail=100000 > "${raw}.scheduler.log" 2>/dev/null || true
  python3 - "${raw}.scheduler.log" "${pod}" <<'PY'
import json, sys
path, pod = sys.argv[1:3]
fields = {
    "prefilter_us": "",
    "filter_us": "",
    "score_us": "",
    "permit_wait_us": "",
    "prebind_us": "",
    "bind_us": "",
    "total_us": "",
}
for line in open(path, encoding="utf-8", errors="replace"):
    if pod not in line or "phase timings" not in line:
        continue
    try:
        event = json.loads(line)
    except Exception:
        continue
    for key in fields:
        if key in event:
            fields[key] = round(float(event[key]) / 1000.0, 3)
print(",".join(str(fields[k]) for k in [
    "prefilter_us", "filter_us", "score_us", "permit_wait_us",
    "prebind_us", "bind_us", "total_us",
]))
PY
}

emit_manifest() {
  local baseline="$1" pod="$2" raw="$3"
  local tolerations='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
  case "${baseline}" in
    B1)
      kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: baseline, baseline: "B1" }
spec:
  schedulerName: default-scheduler
  restartPolicy: Always
  containers: [{ name: app, image: ${IMAGE} }]
EOF
      ;;
    B2)
      kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: baseline, baseline: "B2" }
spec:
  schedulerName: default-scheduler
  nodeSelector: { ai.sovereign.io/tee: "${REQUIRED_TEE}" }
${tolerations}
  restartPolicy: Always
  containers: [{ name: app, image: ${IMAGE} }]
EOF
      ;;
    B3)
      kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: baseline, baseline: "B3" }
spec:
  schedulerName: default-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
  restartPolicy: Always
  containers: [{ name: app, image: ${IMAGE} }]
EOF
      ;;
    B4)
      kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: bench, baseline: "B4" }
  annotations:
    ai.sovereign.io/model-digest: "sha256:${pod}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulingGates:
    - name: ai.sovereign.io/attestation
  runtimeClassName: ${RUNTIME_CLASS}
  nodeSelector: { ai.sovereign.io/tee: "${REQUIRED_TEE}" }
${tolerations}
  restartPolicy: Always
  containers: [{ name: app, image: ${IMAGE} }]
EOF
      ;;
    B5)
      kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: bench, baseline: "B5" }
  annotations:
    ai.sovereign.io/model-digest: "sha256:${pod}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
  nodeSelector: { ai.sovereign.io/tee: "${REQUIRED_TEE}" }
${tolerations}
  restartPolicy: Always
  containers: [{ name: app, image: ${IMAGE} }]
EOF
      ;;
  esac
}

run_one() {
  local baseline="$1" index="$2" warmup="$3"
  local pod raw t0 t_apply t_gate t_bound t_running node success admission pending running total
  local client_total internal_total total_from_log
  local prefilter filter score permit prebind bind cpu mem metrics checked_node phase_csv
  pod="$(printf '%s-%03d-%s' "$(printf '%s' "${baseline}" | tr '[:upper:]' '[:lower:]')" "${index}" "$(date +%H%M%S)")"
  raw="${RAW_DIR}/${pod}.log"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true

  t0="$(now_ms)"
  emit_manifest "${baseline}" "${pod}" "${raw}"
  t_apply="$(now_ms)"
  admission=$((t_apply - t0))

  if [ "${baseline}" = "B4" ]; then
    checked_node="$(verified_evidence_node || true)"
    if [ -n "${checked_node}" ]; then
      kubectl -n "${NS}" patch pod "${pod}" --type=json \
        -p='[{"op":"remove","path":"/spec/schedulingGates"}]' >> "${raw}" 2>&1
    else
      echo "no verified evidence available for B4 gate release" >> "${raw}"
    fi
    t_gate="$(now_ms)"
  else
    t_gate="${t_apply}"
  fi

  node="$(wait_for_node "${pod}" 60 || true)"
  t_bound="$(now_ms)"
  if wait_for_running_or_succeeded "${pod}" 90; then
    success="success"
  else
    success="failure"
  fi
  t_running="$(now_ms)"

  kubectl -n "${NS}" get pod "${pod}" -o yaml >> "${raw}" 2>&1 || true
  kubectl -n "${NS}" describe pod "${pod}" >> "${raw}" 2>&1 || true

  pending=""
  running=""
  total=""
  client_total=""
  internal_total=""
  prefilter=""; filter=""; score=""; permit=""; prebind=""; bind=""
  if [ -n "${node}" ]; then
    pending=$((t_bound - t0))
    client_total=$((t_bound - t_gate))
    total="${client_total}"
    running=$((t_running - t_apply))
  fi
  if [ "${baseline}" = "B5" ]; then
    phase_csv="$(phase_timings_for_pod "${pod}" "${raw}")"
    IFS=',' read -r prefilter filter score permit prebind bind total_from_log <<< "${phase_csv}"
    if [ -n "${total_from_log:-}" ]; then
      internal_total="${total_from_log}"
    fi
  fi

  metrics="$(scheduler_metrics)"
  cpu="${metrics%,*}"
  mem="${metrics#*,}"
  sched="default-scheduler"
  [ "${baseline}" = "B5" ] && sched="${SCHED_NAME}"
  append_row "$(now_iso)" "${baseline}-${index}" "${warmup}" "${baseline}" "${sched}" "${node}" "${success}" \
    "${admission}" "${prefilter}" "${filter}" "${score}" "${permit}" "${prebind}" "${bind}" "${total}" \
    "${client_total}" "${internal_total}" "${pending}" "${running}" "${cpu}" "${mem}" "${raw}"
  printf '  %-2s run %2d warmup=%-5s node=%-34s client=%sms internal=%sms pending=%sms success=%s\n' \
    "${baseline}" "${index}" "${warmup}" "${node:-PENDING}" "${client_total:-NA}" "${internal_total:-NA}" "${pending:-NA}" "${success}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

analyze_results() {
  python3 - "${CSV}" "${TABLE_CSV}" "${QUALITY_JSON}" "${FIGURE}" <<'PY'
import csv, json, math, random, statistics, sys
from collections import defaultdict

raw_csv, table_csv, quality_json, figure = sys.argv[1:5]
rows = list(csv.DictReader(open(raw_csv, encoding="utf-8")))
groups = defaultdict(list)
internal_groups = defaultdict(list)

def fnum(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None

def comparable_latency(row):
    """Use one client-observed scheduling-path metric for every baseline."""
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

def internal_latency(row):
    direct = fnum(row.get("scheduler_internal_latency_ms"))
    if direct is not None:
        return direct
    if row.get("baseline") == "B5":
        # Backward compatibility with old raw CSVs where this column stored
        # B5's internal scheduler phase instead of the client-observed path.
        return fnum(row.get("scheduler_total_latency_ms"))
    return None

for row in rows:
    if row["warmup"] == "true" or row["success"] != "success":
        continue
    value = comparable_latency(row)
    if value is not None:
        groups[row["baseline"]].append(float(value))
    internal = internal_latency(row)
    if internal is not None:
        internal_groups[row["baseline"]].append(float(internal))

def percentile(xs, pct):
    if not xs:
        return ""
    xs = sorted(xs)
    k = (len(xs) - 1) * pct / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return xs[int(k)]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)

def bootstrap_ci(xs, reps=2000):
    if not xs:
        return "", ""
    rnd = random.Random(20260706)
    meds = []
    for _ in range(reps):
        sample = [xs[rnd.randrange(len(xs))] for _ in xs]
        meds.append(statistics.median(sample))
    return percentile(meds, 2.5), percentile(meds, 97.5)

def mann_whitney_p(a, b):
    if not a or not b:
        return ""
    values = [(x, 0) for x in a] + [(x, 1) for x in b]
    values.sort(key=lambda x: x[0])
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
    r1 = sum(rank for rank, (_, g) in zip(ranks, values) if g == 0)
    n1, n2 = len(a), len(b)
    u1 = r1 - n1 * (n1 + 1) / 2.0
    mean = n1 * n2 / 2.0
    var = n1 * n2 * (n1 + n2 + 1) / 12.0
    if var <= 0:
        return ""
    z = abs((u1 - mean) / math.sqrt(var))
    p = math.erfc(z / math.sqrt(2))
    return p

baselines = ["B1", "B2", "B3", "B4", "B5"]
b1_med = statistics.median(groups["B1"]) if groups.get("B1") else None
b4_med = statistics.median(groups["B4"]) if groups.get("B4") else None
b5 = groups.get("B5", [])
fieldnames = [
    "baseline", "n", "median_ms", "p95_ms", "p99_ms", "ci95_low", "ci95_high",
    "overhead_vs_B1_ms", "overhead_vs_B1_pct", "overhead_vs_B4_ms",
    "mannwhitney_p", "measurement_basis", "scheduler_internal_median_ms",
]
with open(table_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for baseline in baselines:
        xs = groups.get(baseline, [])
        if not xs:
            writer.writerow({"baseline": baseline, "n": 0})
            continue
        med = statistics.median(xs)
        lo, hi = bootstrap_ci(xs)
        overhead_b1 = med - b1_med if b1_med is not None else ""
        overhead_b1_pct = (overhead_b1 / b1_med * 100.0) if b1_med not in (None, 0, "") else ""
        overhead_b4 = med - b4_med if b4_med is not None else ""
        p = ""
        if baseline in ("B1", "B4") and b5:
            p = mann_whitney_p(b5, xs)
        internal_xs = internal_groups.get(baseline, [])
        writer.writerow({
            "baseline": baseline,
            "n": len(xs),
            "median_ms": round(med, 3),
            "p95_ms": round(percentile(xs, 95), 3),
            "p99_ms": round(percentile(xs, 99), 3),
            "ci95_low": round(lo, 3) if lo != "" else "",
            "ci95_high": round(hi, 3) if hi != "" else "",
            "overhead_vs_B1_ms": round(overhead_b1, 3) if overhead_b1 != "" else "",
            "overhead_vs_B1_pct": round(overhead_b1_pct, 3) if overhead_b1_pct != "" else "",
            "overhead_vs_B4_ms": round(overhead_b4, 3) if overhead_b4 != "" else "",
            "mannwhitney_p": round(p, 8) if p != "" else "",
            "measurement_basis": "client_observed_scheduling_path",
            "scheduler_internal_median_ms": round(statistics.median(internal_xs), 3) if internal_xs else "",
        })

values = [comparable_latency(r) for r in rows
          if r["warmup"] == "false" and r["success"] == "success" and comparable_latency(r) is not None]
multiple_ratio = sum(1 for v in values if abs(v % 1000.0) < 1e-9) / len(values) if values else 1.0
quality = {
    "raw_csv": raw_csv,
    "table_csv": table_csv,
    "measurement_basis": "client_observed_scheduling_path",
    "b5_scheduler_internal_metric": "reported_separately_not_used_for_b1_b5_cdf",
    "b5_scheduler_internal_median_ms": round(statistics.median(internal_groups.get("B5", [])), 3) if internal_groups.get("B5") else "",
    "measured_success_values": len(values),
    "multiples_of_1000_ratio": multiple_ratio,
    "anti_quantization_status": "PASS" if multiple_ratio <= 0.90 else "FAIL",
}
with open(quality_json, "w", encoding="utf-8") as f:
    json.dump(quality, f, indent=2)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.figure(figsize=(6.4, 4.1))
    for baseline in baselines:
        xs = sorted(groups.get(baseline, []))
        if not xs:
            continue
        ys = [(i + 1) / len(xs) for i in range(len(xs))]
        plt.step(xs, ys, where="post", label=f"{baseline} median={statistics.median(xs):.1f} ms")
    plt.xlabel("Client-observed scheduling-path latency (ms)")
    plt.ylabel("Empirical CDF")
    plt.grid(True, alpha=0.25)
    plt.legend(ncol=3, fontsize=8)
    plt.tight_layout()
    plt.savefig(figure)
except Exception as exc:
    with open(quality_json, "r+", encoding="utf-8") as f:
        q = json.load(f)
        q["figure_error"] = str(exc)
        f.seek(0)
        json.dump(q, f, indent=2)
        f.truncate()
PY
}

if [[ "${ENV_NAME}" != aks* ]]; then
  echo "FAIL: run_aks_b1_b5_latency.sh is AKS-only" >&2
  exit 1
fi
if [ -z "${EVIDENCE_ANN}" ]; then
  echo "FAIL: no verified ${EXPECTED_EVIDENCE_MODE} ${REQUIRED_TEE} evidence found" >&2
  exit 1
fi

echo "== [perf] namespace=${NS} N=${N_RUNS} warmup=${WARMUP} evidence=${EVIDENCE_ANN}"
ensure_ns "${NS}"
kubectl label namespace "${NS}" ai.sovereign.io/sensitivity=high --overwrite >/dev/null
BENCH_RUNTIME_CLASS="${RUNTIME_CLASS}" REQUIRE_CONFIDENTIAL_CONTAINERS=false apply_bench_policy "${NS}" "${REQUIRED_TEE}"

total=$((N_RUNS + WARMUP))
for baseline in B1 B2 B3 B4 B5; do
  echo "== [perf] baseline ${baseline}"
  for i in $(seq 1 "${total}"); do
    warmup="false"
    [ "${i}" -le "${WARMUP}" ] && warmup="true"
    run_one "${baseline}" "${i}" "${warmup}"
  done
done

analyze_results
echo "== [perf] wrote ${CSV}, ${TABLE_CSV}, ${QUALITY_JSON}, ${FIGURE}"
