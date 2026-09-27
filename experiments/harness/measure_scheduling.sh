#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# measure_scheduling.sh — Exp2 scheduling-overhead harness (env-parameterised).
#
# Creates N sensitive "bench" pods one at a time, waits for each to schedule, and
# records real per-phase latency (from scheduler logs + pod API timestamps) as a
# raw CSV. Works identically on kind (simulated) and AKS (real SEV-SNP) — only
# ENV_NAME / NODE / evidence differ. Warm-up runs are KEPT but flagged.
#
# Honesty: no || true to hide scheduling failures; a pod that never binds is
# recorded as success=failure with its logs retained.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source "${HARNESS_DIR}/lib.sh"

N_RUNS="${N_RUNS:-30}"
WARMUP="${WARMUP:-3}"
NS="${NS:-bench-sched}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
OUT_CSV="${OUT_CSV:-${OUT_DIR}/scheduling_latency.csv}"
IMAGE="${BENCH_IMAGE:-registry.k8s.io/pause:3.9}"
REQUIRED_TEE="${REQUIRED_TEE:-$(default_required_tee)}"
RUNTIME_CLASS="${RUNTIME_CLASS:-${BENCH_RUNTIME_CLASS:-$(default_runtime_class)}}"
BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
if is_aks_env; then
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  NODE="${NODE:-$(detect_verified_node "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
  EVIDENCE_ANN="${EVIDENCE_ANN:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
else
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-simulated}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
  NODE="${NODE:-ai-platform-control-plane}"
  EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
fi
NODE="${NODE:-ai-platform-control-plane}"
EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
DELETE_PODS_AFTER_RUN="${DELETE_PODS_AFTER_RUN:-true}"
mkdir -p "${OUT_DIR}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi

echo "timestamp,env,run_id,seed,baseline,scenario,node,pending_ms,admission_to_running_ms,filter_score_ms,reserve_bind_ms,sched_total_ms,success,raw_log_path,filter_precise_ms,score_precise_ms,reserve_prebind_precise_ms,bind_precise_ms,sched_total_precise_ms,admission_latency_ms,prefilter_latency_ms,filter_latency_ms,score_latency_ms,permit_wait_ms,reserve_latency_ms,prebind_latency_ms,bind_latency_ms,scheduler_total_latency_ms,pod_pending_duration_ms,pod_admission_to_running_ms" > "${OUT_CSV}"

echo "== [measure_scheduling] env=${ENV_NAME} N=${N_RUNS} warmup=${WARMUP} node=${NODE}"
ensure_ns "${NS}"
apply_bench_policy "${NS}" "${REQUIRED_TEE}"

# Evidence: on kind the node-agent/verifier produce simulated evidence for the
# node already; we additionally ensure a matching evidence exists for NODE.
# (On AKS real SEV-SNP the node-attestation-agent + central-verifier produce it.)
POLICY_HASH=""  # webhook stamps the pod; we don't precompute here.

total=$((N_RUNS + WARMUP))
for i in $(seq 1 "${total}"); do
  seed=$((1000 + i))
  pod="bench-${i}"
  phase="measured"; [ "${i}" -le "${WARMUP}" ] && phase="warmup"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: bench }
  annotations:
    ai.sovereign.io/model-digest: "sha256:bench-${seed}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers:
    - name: app
      image: ${IMAGE}
EOF

  # wait up to 30s for a node assignment
  node=""
  for _ in $(seq 1 30); do
    node="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    [ -n "${node}" ] && break
    sleep 1
  done

  pod_json="${OUT_DIR}/.pod-${i}.json"
  sched_log="${OUT_DIR}/.sched-${i}.log"
  kubectl -n "${NS}" get pod "${pod}" -o json > "${pod_json}" 2>/dev/null || echo '{}' > "${pod_json}"
  dump_scheduler_logs "${sched_log}"

  row="$(python3 "${HARNESS_DIR}/parse_scheduling.py" \
    --pod "${pod}" --ns "${NS}" --pod-json "${pod_json}" --sched-log "${sched_log}" \
    --env "${ENV_NAME}" --run-id "${phase}-${i}" --seed "${seed}" \
    --baseline "B5-proposed" --scenario "scheduling-overhead")"
  echo "${row}" >> "${OUT_CSV}"
  printf '  run %2d/%d (%s) node=%s\n' "${i}" "${total}" "${phase}" "${node:-PENDING}"
  # keep raw pod json/log for the last few; clean intermediate to save space
  rm -f "${pod_json}"
  [ "${i}" -gt $((total - 3)) ] || rm -f "${sched_log}"
  if [ "${DELETE_PODS_AFTER_RUN}" = "true" ]; then
    kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
done

echo "== done. raw CSV: ${OUT_CSV}"
echo "   warm-up runs are labelled run_id=warmup-* and excluded by the analysis script."
