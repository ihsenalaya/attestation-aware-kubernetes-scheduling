#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# run_b4_vs_b5.sh — G3 strong-baseline comparison (env-parameterised).
#
# B5 (proposed): the attestation-aware scheduler selects an attested node and
# creates an independently verifiable placement token before binding.
#
# B4 (strong baseline): an external schedulingGate controller verifies the same
# real AttestationEvidence, removes the scheduling gate, then lets the default
# scheduler bind the pod. This is a strong baseline, but it has a measurable
# gate-removal -> bind TOCTOU window and no offline-verifiable placement token.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source experiments/harness/lib.sh

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
N_RUNS="${N_RUNS_RACE:-30}"
NS="${NS:-bench-b4b5}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-${OUT_DIR}/b4_vs_b5.csv}"
REQUIRED_TEE="${REQUIRED_TEE:-$(default_required_tee)}"
RUNTIME_CLASS="${RUNTIME_CLASS:-$(default_runtime_class)}"
BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
if is_aks_env; then
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  EVIDENCE_ANN="${EVIDENCE_ANN:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
else
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-simulated}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
  EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
fi
EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
IMAGE="${BENCH_IMAGE:-registry.k8s.io/pause:3.9}"
mkdir -p "${OUT_DIR}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi

echo "env,run_id,baseline,attack_id,gate_check_time_ms,bind_time_ms,toctou_window_ms,placement_verified,blocked,latency_p50,latency_p95,latency_p99,notes" > "${CSV}"
ensure_ns "${NS}"
BENCH_RUNTIME_CLASS="${RUNTIME_CLASS}" apply_bench_policy "${NS}" "${REQUIRED_TEE}"

verified_evidence_node() {
  kubectl get attestationevidences -A -o json | REQUIRED_TEE="${REQUIRED_TEE}" EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" python3 -c '
import json
import os
import sys

required = os.environ["REQUIRED_TEE"].upper()
expected_mode = os.environ.get("EXPECTED_EVIDENCE_MODE", "")
data = json.load(sys.stdin)
for item in data.get("items", []):
    spec = item.get("spec", {})
    status = item.get("status", {})
    if str(spec.get("tee", "")).upper() != required:
        continue
    if expected_mode and status.get("evidenceMode") != expected_mode:
        continue
    if not status.get("verified") or status.get("revoked"):
        continue
    if status.get("evidenceMode") == "unverified":
        continue
    node = spec.get("subjectRef", {}).get("name", "")
    if node:
        print(node)
        raise SystemExit(0)
raise SystemExit(1)
'
}

wait_bound() { # namespace pod max_seconds
  local ns="$1" pod="$2" max="${3:-30}" node=""
  for _ in $(seq 1 "${max}"); do
    node="$(kubectl -n "${ns}" get pod "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    if [ -n "${node}" ]; then
      echo "${node}"
      return 0
    fi
    sleep 1
  done
  return 1
}

decision_has_token() { # namespace pod
  local ns="$1" pod="$2" decision
  decision="${pod}-${ns}"
  decision="${decision:0:63}"
  kubectl -n "${ns}" get aiplacementdecision "${decision}" \
    -o jsonpath='{.metadata.annotations.ai\.sovereign\.io/placement-token}' 2>/dev/null | grep -q .
}

echo "== [b4_vs_b5] env=${ENV_NAME} N=${N_RUNS} tee=${REQUIRED_TEE}"
echo "== [b4_vs_b5] runtime=${RUNTIME_CLASS} evidence=${EVIDENCE_ANN} mode=${EXPECTED_EVIDENCE_MODE}"

for r in $(seq 1 "${N_RUNS}"); do
  pod="b5-${r}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  t0=$(date +%s%3N)
  kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: bench }
  annotations:
    ai.sovereign.io/model-digest: "sha256:b5-${r}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
  nodeSelector: { ai.sovereign.io/tee: "${REQUIRED_TEE}" }
${TOLERATIONS_YAML}
  containers: [{ name: app, image: ${IMAGE} }]
EOF
  node="$(wait_bound "${NS}" "${pod}" 30 || true)"
  t1=$(date +%s%3N)
  bind_ms=$((t1 - t0))
  blocked="no"; [ -z "${node}" ] && blocked="yes"
  placement_verified="no"
  if decision_has_token "${NS}" "${pod}"; then
    placement_verified="yes"
  fi
  echo "${ENV_NAME},run-${r},B5,A7,0,${bind_ms},0,${placement_verified},${blocked},,,,prebind-tokenized-placement node=${node:-none}" >> "${CSV}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  printf '  B5 run %2d/%d blocked=%s token=%s bind=%sms\n' "${r}" "${N_RUNS}" "${blocked}" "${placement_verified}" "${bind_ms}"
done

for r in $(seq 1 "${N_RUNS}"); do
  pod="b4-${r}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  t0=$(date +%s%3N)
  kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: bench }
  annotations:
    ai.sovereign.io/model-digest: "sha256:b4-${r}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulingGates:
    - name: ai.sovereign.io/attestation
  runtimeClassName: ${RUNTIME_CLASS}
  nodeSelector: { ai.sovereign.io/tee: "${REQUIRED_TEE}" }
${TOLERATIONS_YAML}
  containers: [{ name: app, image: ${IMAGE} }]
EOF

  checked_node="$(verified_evidence_node || true)"
  t_check=$(date +%s%3N)
  gate_check_ms=$((t_check - t0))
  if [ -z "${checked_node}" ]; then
    echo "${ENV_NAME},run-${r},B4,A7,${gate_check_ms},,,no,yes,,,,no-verified-${REQUIRED_TEE}-evidence" >> "${CSV}"
    printf '  B4 run %2d/%d blocked=yes reason=no-evidence\n' "${r}" "${N_RUNS}"
    continue
  fi

  kubectl -n "${NS}" patch pod "${pod}" --type=json \
    -p='[{"op":"remove","path":"/spec/schedulingGates"}]' >/dev/null
  t_gate=$(date +%s%3N)
  node="$(wait_bound "${NS}" "${pod}" 30 || true)"
  t_bind=$(date +%s%3N)
  bind_ms=$((t_bind - t0))
  window_ms=$((t_bind - t_gate))
  blocked="no"; [ -z "${node}" ] && blocked="yes"
  echo "${ENV_NAME},run-${r},B4,A7,${gate_check_ms},${bind_ms},${window_ms},no,${blocked},,,,schedulingGate-external-verifier checked_node=${checked_node} bound_node=${node:-none}" >> "${CSV}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  printf '  B4 run %2d/%d blocked=%s window=%sms checked=%s bound=%s\n' "${r}" "${N_RUNS}" "${blocked}" "${window_ms}" "${checked_node}" "${node:-none}"
done

echo "== b4_vs_b5 done (CSV: ${CSV})"
