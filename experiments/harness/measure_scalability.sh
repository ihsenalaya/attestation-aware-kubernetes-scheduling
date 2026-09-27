#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# measure_scalability.sh — Exp3 scheduler scalability (env-parameterised).
#
# For each batch size in SIZES (default 10 50 100), submits that many sensitive
# "bench" pods at once and measures the makespan (wall-clock time until ALL are
# bound) and the resulting throughput (pods/s), repeated N_RUNS_SCALABILITY times.
# This stresses the attestation-aware scheduler's Filter/Score/PreBind/Bind path.
#
# HONEST: on kind this is labelled kind-live-simulated (single-node, simulated
# evidence) and characterises SCHEDULER THROUGHPUT, not cloud placement latency.
# 500 pods only if HOST_SUPPORTS_500=true.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source "${HARNESS_DIR}/lib.sh"

SIZES="${SIZES:-10 50 100}"
N_RUNS="${N_RUNS_SCALABILITY:-10}"
NS="${NS:-bench-scale}"
REQUIRED_TEE="${REQUIRED_TEE:-TDX}"
RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-tdx}"
NODE="${NODE:-ai-platform-control-plane}"
EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-${OUT_DIR}/scalability.csv}"
mkdir -p "${OUT_DIR}"

echo "env,run_id,batch_size,scheduled,makespan_ms,throughput_pods_per_s,raw_log_path" > "${CSV}"
ensure_ns "${NS}"
BENCH_RUNTIME_CLASS="${RUNTIME_CLASS}" apply_bench_policy "${NS}" "${REQUIRED_TEE}" 2>/dev/null || apply_bench_policy "${NS}" "${REQUIRED_TEE}"

echo "== [scalability] env=${ENV_NAME} sizes='${SIZES}' N=${N_RUNS}"
for B in ${SIZES}; do
  for r in $(seq 1 "${N_RUNS}"); do
    run_label="scale-${B}-${r}"
    kubectl -n "${NS}" delete pods -l "app=bench,scale-run=${run_label}" --wait=false >/dev/null 2>&1 || true
    # brief settle so deletions don't race the new batch
    sleep 1
    # build a single manifest of B pods and submit at once
    manifest="$(mktemp)"
    for i in $(seq 1 "${B}"); do
      cat >> "${manifest}" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: scale-${B}-${r}-${i}
  namespace: ${NS}
  labels:
    app: bench
    scale-run: "${run_label}"
  annotations:
    ai.sovereign.io/model-digest: "sha256:s-${B}-${r}-${i}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
---
EOF
    done
    t0=$(date +%s%3N)
    kubectl apply -n "${NS}" -f "${manifest}" >/dev/null 2>&1
    rm -f "${manifest}"
    # wait until all B are bound (nodeName set), max 5 min
    scheduled=0
    for _ in $(seq 1 300); do
      scheduled="$(kubectl -n "${NS}" get pods -l "app=bench,scale-run=${run_label}" -o json 2>/dev/null \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for p in d.get("items",[]) if p.get("spec",{}).get("nodeName")))' 2>/dev/null || echo 0)"
      [ "${scheduled}" -ge "${B}" ] && break
      sleep 1
    done
    t1=$(date +%s%3N)
    makespan=$((t1 - t0))
    thr="$(python3 -c "print(round(${scheduled}/(${makespan}/1000.0),3) if ${makespan}>0 else 0)")"
    echo "${ENV_NAME},run-${r},${B},${scheduled},${makespan},${thr},${OUT_DIR}/scalability" >> "${CSV}"
    printf '  batch=%3d run %2d/%d scheduled=%d makespan=%sms thr=%s/s\n' "${B}" "${r}" "${N_RUNS}" "${scheduled}" "${makespan}" "${thr}"
    kubectl -n "${NS}" delete pods -l "app=bench,scale-run=${run_label}" --wait=false >/dev/null 2>&1 || true
  done
done
echo "== scalability done (CSV: ${CSV})"
