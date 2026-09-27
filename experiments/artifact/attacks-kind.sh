#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# attacks-kind.sh — Article 1 on-cluster attack scenarios A1..A9 (kind).
#
# SIMULATED — KIND ONLY — NOT REAL TEE/GPU
#
# Each attack asserts the LIVE platform (admission webhook + attestation
# scheduler + verify-placement) blocks the attack. Results are written as a CSV
# row per attack to results/tables/attack-results.csv.
#
# Honesty rules: no `|| true` masking, no skipping. An attack "passes" only when
# the platform actually refused/blocked it, observed on the real cluster.
set -euo pipefail

NS="${NS:-article1-attacks}"
NODE="${NODE:-ai-platform-control-plane}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
VERIFY_BIN="${VERIFY_BIN:-verify-placement}"
OUT_DIR="${OUT_DIR:-results/raw/kind}"
ENV_NAME="${ENV_NAME:-kind-live-simulated}"
if [[ "${ENV_NAME}" == aks* ]]; then
  POLICY_TEE="${POLICY_TEE:-TDX}"
  A1_POLICY_TEE="${A1_POLICY_TEE:-SEV-SNP}"
  WRONG_TEE="${WRONG_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  BAD_RUNTIME_CLASS="${BAD_RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  A1_NODE_SELECTOR_KEY="${A1_NODE_SELECTOR_KEY:-kubernetes.azure.com/agentpool}"
  A1_NODE_SELECTOR_VALUE="${A1_NODE_SELECTOR_VALUE:-system}"
  CLEAR_EVIDENCE_FOR_A1="${CLEAR_EVIDENCE_FOR_A1:-false}"
else
  POLICY_TEE="${POLICY_TEE:-TDX}"
  A1_POLICY_TEE="${A1_POLICY_TEE:-${POLICY_TEE}}"
  WRONG_TEE="${WRONG_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-tdx}"
  BAD_RUNTIME_CLASS="${BAD_RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
  A1_NODE_SELECTOR_KEY="${A1_NODE_SELECTOR_KEY:-}"
  A1_NODE_SELECTOR_VALUE="${A1_NODE_SELECTOR_VALUE:-}"
  CLEAR_EVIDENCE_FOR_A1="${CLEAR_EVIDENCE_FOR_A1:-true}"
fi
# Live-run CSV goes to raw results; the curated A1..A9 matrix lives separately in
# paper/tables/attack-results.csv and must not be overwritten here.
CSV="${CSV:-results/raw/kind/attack-results-kind-live.csv}"
mkdir -p "${OUT_DIR}"
mkdir -p "$(dirname "${CSV}")"

KUBECTL_RETRIES="${KUBECTL_RETRIES:-6}"
KUBECTL_RETRY_SLEEP="${KUBECTL_RETRY_SLEEP:-5}"
TRANSIENT_KUBECTL_ERROR='Unable to connect|client connection lost|i/o timeout|TLS handshake timeout|failed to download openapi|lookup .* timeout|connection reset|connection refused|context deadline exceeded|net/http|EOF|ServiceUnavailable|Too Many Requests|temporarily unavailable'

is_transient_kubectl_error() {
  printf '%s\n' "$1" | grep -Eqi "${TRANSIENT_KUBECTL_ERROR}"
}

kubectl_retry() {
  local attempt out rc
  for attempt in $(seq 1 "${KUBECTL_RETRIES}"); do
    set +e
    out="$(kubectl "$@" 2>&1)"
    rc=$?
    set -e
    if [ "${rc}" -eq 0 ]; then
      printf '%s\n' "${out}"
      return 0
    fi
    if is_transient_kubectl_error "${out}" && [ "${attempt}" -lt "${KUBECTL_RETRIES}" ]; then
      sleep "${KUBECTL_RETRY_SLEEP}"
      continue
    fi
    printf '%s\n' "${out}"
    return "${rc}"
  done
}

apply_manifest() {
  local tmp attempt out rc
  tmp="$(mktemp)"
  cat > "${tmp}"
  for attempt in $(seq 1 "${KUBECTL_RETRIES}"); do
    set +e
    out="$(kubectl apply --validate=false -f "${tmp}" 2>&1)"
    rc=$?
    set -e
    if [ "${rc}" -eq 0 ]; then
      rm -f "${tmp}"
      printf '%s\n' "${out}"
      return 0
    fi
    if is_transient_kubectl_error "${out}" && [ "${attempt}" -lt "${KUBECTL_RETRIES}" ]; then
      sleep "${KUBECTL_RETRY_SLEEP}"
      continue
    fi
    rm -f "${tmp}"
    printf '%s\n' "${out}"
    return "${rc}"
  done
}

apply_manifest_expect_denial() {
  local tmp attempt out rc
  tmp="$(mktemp)"
  cat > "${tmp}"
  for attempt in $(seq 1 "${KUBECTL_RETRIES}"); do
    set +e
    out="$(kubectl apply --validate=false -f "${tmp}" 2>&1)"
    rc=$?
    set -e
    if [ "${rc}" -eq 0 ]; then
      rm -f "${tmp}"
      printf '%s\n' "${out}"
      return 0
    fi
    if is_transient_kubectl_error "${out}" && [ "${attempt}" -lt "${KUBECTL_RETRIES}" ]; then
      sleep "${KUBECTL_RETRY_SLEEP}"
      continue
    fi
    rm -f "${tmp}"
    printf '%s\n' "${out}"
    return "${rc}"
  done
}

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi
A1_NODE_SELECTOR_YAML=""
if [ -n "${A1_NODE_SELECTOR_KEY}" ] && [ -n "${A1_NODE_SELECTOR_VALUE}" ]; then
  A1_NODE_SELECTOR_YAML="  nodeSelector:
    ${A1_NODE_SELECTOR_KEY}: ${A1_NODE_SELECTOR_VALUE}"
fi

echo "attack_id,description,baseline,expected,observed,result,environment,notes" > "${CSV}"
PASS_COUNT=0
TOTAL=0

record() { # id desc expected observed result notes
  TOTAL=$((TOTAL+1))
  [ "$5" = "BLOCKED" ] && PASS_COUNT=$((PASS_COUNT+1))
  echo "$1,\"$2\",B5-proposed,$3,$4,$5,${ENV_NAME},\"$6\"" >> "${CSV}"
  printf '  [%s] %-52s -> %s\n' "$1" "$2" "$5"
}

echo "== resetting attack namespace + clearing competing node evidence (isolation)"
kubectl_retry delete namespace "${NS}" --ignore-not-found --wait=true --timeout=90s >/dev/null
# A1 requires that NO valid evidence exists for the node. Clear evidence left by
# the positive e2e scenario so the fake-label attack is tested in isolation.
# On AKS real runs, never delete MAA evidence; use a non-matching POLICY_TEE to
# exercise fail-closed placement without destroying the real attestation set.
if [ "${CLEAR_EVIDENCE_FOR_A1}" = "true" ]; then
  kubectl_retry delete attestationevidence --all -A >/dev/null || true
fi
kubectl_retry create namespace "${NS}" >/dev/null
kubectl_retry label namespace "${NS}" ai.sovereign.io/sensitivity=high --overwrite >/dev/null

apply_policy() { # requiredTEE
apply_manifest >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata: { name: atk-policy, namespace: ${NS} }
spec:
  target:
    namespaceSelector: { matchLabels: { ai.sovereign.io/sensitivity: high } }
    workloadSelector: { matchLabels: { app: victim } }
  requiredTEE: ["$1"]
  requireConfidentialContainers: ${REQUIRE_CONFIDENTIAL_CONTAINERS}
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: 300
  requireModelDigest: true
  enforcementMode: enforce
EOF
}

# Helper: is a pod still Pending with no AIPlacementDecision after grace?
pod_blocked() { # podName graceSeconds
  local n="$1" g="${2:-12}"
  local errors=0
  for _ in $(seq 1 "$g"); do
    local node rc out
    set +e
    out="$(kubectl get pod "$n" -n "${NS}" -o jsonpath='{.spec.nodeName}' 2>&1)"
    rc=$?
    set -e
    if [ "${rc}" -ne 0 ]; then
      if is_transient_kubectl_error "${out}"; then
        errors=$((errors+1))
      fi
      sleep 1
      continue
    fi
    node="${out}"
    [ -n "$node" ] && { echo "SCHEDULED:$node"; return 0; }
    sleep 1
  done
  [ "${errors}" -ge "${g}" ] && { echo "INFRA_ERROR"; return 0; }
  echo "PENDING"
}

# ── A1: fake attested label, NO evidence -> scheduler must not bind ───────────
apply_policy "${A1_POLICY_TEE}"
apply_manifest >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: victim-a1
  namespace: ${NS}
  labels: { app: victim, ai.sovereign.io/attested: "true" }
  annotations:
    ai.sovereign.io/model-digest: "sha256:m"
    ai.sovereign.io/attestation-evidence: "nonexistent"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${A1_NODE_SELECTOR_YAML}
${TOLERATIONS_YAML}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
r=$(pod_blocked victim-a1 12)
if [ "$r" = "PENDING" ]; then
  record A1 "Forged attested=true label, no evidence" BLOCKED "$r" BLOCKED "scheduler filter: no valid evidence"
elif [ "$r" = "INFRA_ERROR" ]; then
  record A1 "Forged attested=true label, no evidence" BLOCKED "$r" INFRA_ERROR "transient Kubernetes API/DNS errors exhausted"
else
  record A1 "Forged attested=true label, no evidence" BLOCKED "$r" ALLOWED "UNEXPECTED bind"
fi

# ── A4: evidence with wrong TEE (SEV-SNP) vs policy TDX -> filter rejects ─────
apply_policy "${POLICY_TEE}"
apply_manifest >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: AttestationEvidence
metadata: { name: atk-ev-wrongtee, namespace: ${NS} }
spec:
  subjectRef: { name: ${NODE} }
  evidenceType: cpu
  tee: ${WRONG_TEE}
  simulated: true
  freshness: { maxAgeSeconds: 300, simulated: true }
  digest: "sha256:wrongtee"
EOF
sleep 3
apply_manifest >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: victim-a4
  namespace: ${NS}
  labels: { app: victim }
  annotations:
    ai.sovereign.io/model-digest: "sha256:m"
    ai.sovereign.io/attestation-evidence: "atk-ev-wrongtee"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
r=$(pod_blocked victim-a4 12)
if [ "$r" = "PENDING" ]; then
  record A4 "Evidence TEE=${WRONG_TEE} but policy requires ${POLICY_TEE}" BLOCKED "$r" BLOCKED "scheduler filter: TEE not in required list"
elif [ "$r" = "INFRA_ERROR" ]; then
  record A4 "Evidence TEE=${WRONG_TEE} but policy requires ${POLICY_TEE}" BLOCKED "$r" INFRA_ERROR "transient Kubernetes API/DNS errors exhausted"
else
  record A4 "Evidence TEE=${WRONG_TEE} but policy requires ${POLICY_TEE}" BLOCKED "$r" ALLOWED "UNEXPECTED bind"
fi

# ── A5: sensitive pod with forbidden runtimeClass -> webhook DENIES ───────────
apply_manifest >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata: { name: atk-policy, namespace: ${NS} }
spec:
  target:
    namespaceSelector: { matchLabels: { ai.sovereign.io/sensitivity: high } }
    workloadSelector: { matchLabels: { app: victim } }
  requiredTEE: ["${POLICY_TEE}"]
  requireConfidentialContainers: true
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: 300
  requireModelDigest: true
  enforcementMode: enforce
EOF
set +e
deny_out=$(apply_manifest_expect_denial <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: victim-a5
  namespace: ${NS}
  labels: { app: victim }
  annotations: { ai.sovereign.io/model-digest: "sha256:m" }
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${BAD_RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
)
rc=$?
set -e
if [ $rc -ne 0 ] && echo "$deny_out" | grep -qi 'runtimeClass\|not allowed\|forbidden\|denied'; then
  record A5 "Forbidden runtimeClass ${BAD_RUNTIME_CLASS} on sensitive pod" BLOCKED "DENIED" BLOCKED "validating webhook denied admission"
elif [ $rc -ne 0 ] && is_transient_kubectl_error "${deny_out}"; then
  record A5 "Forbidden runtimeClass ${BAD_RUNTIME_CLASS} on sensitive pod" BLOCKED "INFRA_ERROR" INFRA_ERROR "transient Kubernetes API/DNS errors exhausted: ${deny_out}"
else
  record A5 "Forbidden runtimeClass ${BAD_RUNTIME_CLASS} on sensitive pod" BLOCKED "ADMITTED" ALLOWED "UNEXPECTED admit: ${deny_out}"
fi

# ── A5b: missing required model digest -> webhook DENIES ──────────────────────
set +e
deny2=$(apply_manifest_expect_denial <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: victim-a5b
  namespace: ${NS}
  labels: { app: victim }
  annotations: { ai.sovereign.io/attestation-evidence: "atk-ev-wrongtee" }
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
)
rc2=$?
set -e
if [ $rc2 -ne 0 ] && echo "$deny2" | grep -qi 'model-digest\|required\|denied\|is required'; then
  record A5b "Missing required model-digest annotation" BLOCKED "DENIED" BLOCKED "validating webhook denied admission"
elif [ $rc2 -ne 0 ] && is_transient_kubectl_error "${deny2}"; then
  record A5b "Missing required model-digest annotation" BLOCKED "INFRA_ERROR" INFRA_ERROR "transient Kubernetes API/DNS errors exhausted: ${deny2}"
else
  record A5b "Missing required model-digest annotation" BLOCKED "ADMITTED" ALLOWED "UNEXPECTED admit: ${deny2}"
fi

# ── A8: tamper the placement token -> verify-placement must FAIL ──────────────
# reuse the positive-scenario token if present, else mint via a good placement
GOOD_TOKEN="${OUT_DIR}/e2e-placement-token.json"
PUBKEY_FILE="${OUT_DIR}/e2e-scheduler-pubkey.hex"
if [ -f "${GOOD_TOKEN}" ] && [ -f "${PUBKEY_FILE}" ]; then
  # flip one hex char inside the base64/JSON signature field
  python3 - "$GOOD_TOKEN" "${OUT_DIR}/atk-token-tampered.json" <<'PY'
import json,sys
src,dst=sys.argv[1],sys.argv[2]
tok=json.load(open(src))
sig=tok.get("signature","")
if sig:
    c='0' if sig[0]!='0' else 'f'
    tok["signature"]=c+sig[1:]
json.dump(tok,open(dst,"w"))
PY
  set +e
  out=$("${VERIFY_BIN}" -token-file "${OUT_DIR}/atk-token-tampered.json" -pubkey-hex "$(cat "${PUBKEY_FILE}")" 2>&1)
  vrc=$?
  set -e
  if [ $vrc -ne 0 ] && echo "$out" | grep -qi 'FAIL'; then
    record A8 "Tampered placement token signature" BLOCKED "FAIL" BLOCKED "verify-placement rejected tampered signature"
  else
    record A8 "Tampered placement token signature" BLOCKED "PASS" ALLOWED "UNEXPECTED verify pass: ${out}"
  fi
else
  record A8 "Tampered placement token signature" BLOCKED "SKIPPED-NO-TOKEN" ALLOWED "run e2e-kind-positive.sh first"
fi

echo ""
echo "== attacks summary: ${PASS_COUNT}/${TOTAL} blocked (${ENV_NAME})"
cp "${CSV}" "${OUT_DIR}/attack-results-${ENV_NAME}.csv"
[ "${PASS_COUNT}" -eq "${TOTAL}" ] || { echo "FAIL: not all attacks blocked" >&2; exit 1; }
echo "== all on-cluster attacks blocked env=${ENV_NAME}"
