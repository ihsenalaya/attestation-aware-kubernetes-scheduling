#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# A10 — compromised node-agent attempts to forge AttestationEvidence.
#
# The expected security property is RBAC-enforced trust-chain separation:
# node-attestation-agent can create RawAttestationReport only; it cannot create,
# update or patch AttestationEvidence. The central verifier is the sole writer.
set -euo pipefail

PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
ATTACK_NS="${ATTACK_NS:-article1-a10}"
ENV_NAME="${ENV_NAME:-kind-live-simulated}"
OUT_DIR="${OUT_DIR:-results/raw/kind/a10}"
CSV="${CSV:-${OUT_DIR}/a10-rbac-results.csv}"
NODE_NAME="${NODE_NAME:-ai-platform-control-plane}"
VERIFY_BIN="${VERIFY_BIN:-verify-placement}"
GOOD_TOKEN="${GOOD_TOKEN:-results/raw/kind/e2e-placement-token.json}"
PUBKEY_FILE="${PUBKEY_FILE:-results/raw/kind/e2e-scheduler-pubkey.hex}"
if [[ "${ENV_NAME}" == aks* ]]; then
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
else
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-tdx}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
fi
A10_POLICY_TEE="${A10_POLICY_TEE:-TDX}"
NODE_SA="system:serviceaccount:${PLATFORM_NS}:node-attestation-agent"
SCHEDULER_SA="system:serviceaccount:${PLATFORM_NS}:attestation-scheduler"
VERIFIER_SA="system:serviceaccount:${PLATFORM_NS}:central-verifier"

mkdir -p "${OUT_DIR}"
echo "timestamp,env,attack_id,adversary,service_account,attempted_action,expected_result,actual_result,blocked,reason,raw_log_path" > "${CSV}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
  - key: ai.sovereign.io/confidential
    operator: Equal
    value: "true"
    effect: NoSchedule'
fi

failures=0

record() {
  local attack_id="$1" adversary="$2" service_account="$3" action="$4" expected="$5" actual="$6" blocked="$7" reason="$8" raw_log="$9"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "${ENV_NAME}" \
    "${attack_id}" \
    "${adversary}" \
    "${service_account}" \
    "${action}" \
    "${expected}" \
    "${actual}" \
    "${blocked}" \
    "${reason}" \
    "${raw_log}" >> "${CSV}"
}

set +e
kubectl delete namespace "${ATTACK_NS}" --ignore-not-found --wait=true --timeout=90s > "${OUT_DIR}/A10-cleanup-attack-namespace.log" 2>&1
cleanup_rc=$?
set -e
if [ "${cleanup_rc}" -ne 0 ]; then
  record "A10-cleanup-attack-namespace" "test-harness" "local-admin" "delete_attack_namespace" "success" "error" "false" "cleanup_failed" "${OUT_DIR}/A10-cleanup-attack-namespace.log"
  echo "A10 FAIL: could not clean namespace ${ATTACK_NS}" >&2
  exit 1
fi

can_i() {
  local attack_id="$1" adversary="$2" service_account="$3" verb="$4" resource="$5" expected="$6"
  local raw_log="${OUT_DIR}/${attack_id}.log"
  local actual rc blocked reason

  set +e
  actual="$(kubectl auth can-i "${verb}" "${resource}" --namespace "${PLATFORM_NS}" --as="${service_account}" 2>&1)"
  rc=$?
  set -e

  printf 'kubectl auth can-i %s %s --namespace %s --as=%s\n%s\nrc=%s\n' \
    "${verb}" "${resource}" "${PLATFORM_NS}" "${service_account}" "${actual}" "${rc}" > "${raw_log}"

  if [ "${actual}" != "yes" ] && [ "${actual}" != "no" ]; then
    actual="error"
  fi
  blocked="false"
  if [ "${actual}" = "no" ]; then
    blocked="true"
  fi
  if [ "${actual}" = "${expected}" ]; then
    reason="expected_${expected}"
  else
    reason="unexpected_${actual}_expected_${expected}"
    failures=$((failures + 1))
  fi

  record "${attack_id}" "${adversary}" "${service_account}" "${verb}_${resource}" "${expected}" "${actual}" "${blocked}" "${reason}" "${raw_log}"
}

can_i "A10-node-create-evidence" "compromised-node-agent" "${NODE_SA}" "create" "attestationevidences.aiops.imperium.io" "no"
can_i "A10-node-update-evidence" "compromised-node-agent" "${NODE_SA}" "update" "attestationevidences.aiops.imperium.io" "no"
can_i "A10-node-patch-evidence-status" "compromised-node-agent" "${NODE_SA}" "patch" "attestationevidences/status.aiops.imperium.io" "no"
can_i "A10-scheduler-create-evidence" "compromised-scheduler" "${SCHEDULER_SA}" "create" "attestationevidences.aiops.imperium.io" "no"
can_i "A10-node-create-raw-report" "compromised-node-agent" "${NODE_SA}" "create" "rawattestationreports.aiops.imperium.io" "yes"
can_i "A10-verifier-create-evidence" "central-verifier-control" "${VERIFIER_SA}" "create" "attestationevidences.aiops.imperium.io" "yes"

report_name="a10-forged-$(date -u +%Y%m%d%H%M%S)"
node_name="a10-forged-node-${report_name}"
evidence_name="evidence-${node_name}"
raw_log="${OUT_DIR}/A10-forged-raw-report.log"

set +e
kubectl create --as="${NODE_SA}" --namespace "${PLATFORM_NS}" -f - > "${raw_log}" 2>&1 <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: RawAttestationReport
metadata:
  name: ${report_name}
spec:
  nodeName: ${node_name}
  nodeUID: forged-node-uid
  provider: maa
  rawToken: forged-not-a-valid-maa-token
  rawTokenHash: forged-token-hash
  nonce: forged-nonce
  collectedAt: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  agentPodUID: forged-agent-pod
  agentServiceAccount: node-attestation-agent
  simulated: false
EOF
create_rc=$?
set -e

if [ "${create_rc}" -ne 0 ]; then
  failures=$((failures + 1))
  record "A10-forged-raw-report" "compromised-node-agent" "${NODE_SA}" "create_rawattestationreports.aiops.imperium.io" "yes" "error" "false" "raw_report_create_failed" "${raw_log}"
else
  record "A10-forged-raw-report" "compromised-node-agent" "${NODE_SA}" "create_rawattestationreports.aiops.imperium.io" "yes" "yes" "false" "raw_report_created_for_verifier_negative_control" "${raw_log}"
fi

mode=""
verified=""
verify_log="${OUT_DIR}/A10-forged-verifier-outcome.log"
: > "${verify_log}"
for _ in $(seq 1 30); do
  set +e
  mode="$(kubectl get attestationevidence "${evidence_name}" --namespace "${PLATFORM_NS}" -o jsonpath='{.status.evidenceMode}' 2>>"${verify_log}")"
  mode_rc=$?
  verified="$(kubectl get attestationevidence "${evidence_name}" --namespace "${PLATFORM_NS}" -o jsonpath='{.status.verified}' 2>>"${verify_log}")"
  verified_rc=$?
  set -e
  if [ "${mode_rc}" -eq 0 ] && [ "${verified_rc}" -eq 0 ] && [ -n "${mode}" ]; then
    break
  fi
  sleep 2
done

printf 'evidence=%s\nmode=%s\nverified=%s\n' "${evidence_name}" "${mode}" "${verified}" >> "${verify_log}"
if [ "${mode}" = "unverified" ] && [ "${verified}" != "true" ]; then
  record "A10-forged-verifier-outcome" "compromised-node-agent" "${NODE_SA}" "forge_real_attestation_via_raw_report" "unverified" "${mode}" "true" "central_verifier_refused_bogus_real_token" "${verify_log}"
else
  failures=$((failures + 1))
  record "A10-forged-verifier-outcome" "compromised-node-agent" "${NODE_SA}" "forge_real_attestation_via_raw_report" "unverified" "${mode:-missing}" "false" "unexpected_verifier_outcome" "${verify_log}"
fi

placement_log="${OUT_DIR}/A10-scheduler-refuses-unverified-placement.log"
set +e
{
  echo "Creating isolated attack namespace ${ATTACK_NS}"
  kubectl create namespace "${ATTACK_NS}" --dry-run=client -o yaml | kubectl apply --validate=false -f -
  kubectl label namespace "${ATTACK_NS}" ai.sovereign.io/sensitivity=high --overwrite
  kubectl apply --validate=false -f - <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata:
  name: a10-reject-unverified
  namespace: ${ATTACK_NS}
spec:
  target:
    namespaceSelector:
      matchLabels:
        ai.sovereign.io/sensitivity: high
    workloadSelector:
      matchLabels:
        app: a10-victim
  requiredTEE: ["${A10_POLICY_TEE}"]
  requireConfidentialContainers: ${REQUIRE_CONFIDENTIAL_CONTAINERS}
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: 300
  requireModelDigest: true
  enforcementMode: enforce
EOF
  kubectl apply --validate=false -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: a10-victim
  namespace: ${ATTACK_NS}
  labels:
    app: a10-victim
  annotations:
    ai.sovereign.io/model-digest: "sha256:a10"
    ai.sovereign.io/attestation-evidence: "${evidence_name}"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
EOF
} > "${placement_log}" 2>&1
placement_setup_rc=$?
set -e

bound_node=""
if [ "${placement_setup_rc}" -ne 0 ]; then
  failures=$((failures + 1))
  record "A10-scheduler-refuses-placement" "compromised-node-agent" "${SCHEDULER_SA}" "schedule_pod_with_unverified_or_incompatible_evidence" "pending" "error" "false" "placement_setup_failed" "${placement_log}"
else
  for _ in $(seq 1 20); do
    set +e
    bound_node="$(kubectl get pod a10-victim --namespace "${ATTACK_NS}" -o jsonpath='{.spec.nodeName}' 2>>"${placement_log}")"
    pod_rc=$?
    set -e
    if [ "${pod_rc}" -eq 0 ] && [ -n "${bound_node}" ]; then
      break
    fi
    sleep 1
  done

  printf 'expected_bound_node=<empty>\nactual_bound_node=%s\nnode_under_test=%s\n' "${bound_node}" "${NODE_NAME}" >> "${placement_log}"
  if [ -z "${bound_node}" ]; then
    record "A10-scheduler-refuses-placement" "compromised-node-agent" "${SCHEDULER_SA}" "schedule_pod_with_unverified_or_incompatible_evidence" "pending" "pending" "true" "scheduler_did_not_bind_pod_without_valid_matching_evidence" "${placement_log}"
  else
    failures=$((failures + 1))
    record "A10-scheduler-refuses-placement" "compromised-node-agent" "${SCHEDULER_SA}" "schedule_pod_with_unverified_or_incompatible_evidence" "pending" "bound:${bound_node}" "false" "unexpected_scheduler_bind" "${placement_log}"
  fi
fi

token_log="${OUT_DIR}/A10-verify-placement-tampered-token.log"
if [ ! -f "${GOOD_TOKEN}" ] || [ ! -f "${PUBKEY_FILE}" ]; then
  failures=$((failures + 1))
  printf 'missing prerequisite token=%s pubkey=%s\n' "${GOOD_TOKEN}" "${PUBKEY_FILE}" > "${token_log}"
  record "A10-verify-placement-tampered-token" "token-tamper" "offline-verifier" "verify_tampered_placement_token" "fail" "prerequisite_missing" "false" "run_positive_e2e_first_to_generate_token_and_pubkey" "${token_log}"
else
  tampered_token="${OUT_DIR}/a10-placement-token-tampered.json"
  set +e
  python3 - "${GOOD_TOKEN}" "${tampered_token}" > "${token_log}" 2>&1 <<'PY'
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
tok = json.load(open(src, encoding="utf-8"))
sig = tok.get("signature", "")
if not sig:
    raise SystemExit("token has no signature field")
tok["signature"] = ("0" if sig[0] != "0" else "f") + sig[1:]
json.dump(tok, open(dst, "w", encoding="utf-8"))
PY
  tamper_rc=$?
  set -e
  if [ "${tamper_rc}" -ne 0 ]; then
    failures=$((failures + 1))
    record "A10-verify-placement-tampered-token" "token-tamper" "offline-verifier" "verify_tampered_placement_token" "fail" "tamper_error" "false" "could_not_tamper_reference_token" "${token_log}"
  else
  set +e
  verify_out="$("${VERIFY_BIN}" -token-file "${tampered_token}" -pubkey-hex "$(cat "${PUBKEY_FILE}")" 2>&1)"
  verify_rc=$?
  set -e
  printf '%s\nrc=%s\n' "${verify_out}" "${verify_rc}" >> "${token_log}"
  if [ "${verify_rc}" -ne 0 ] && printf '%s\n' "${verify_out}" | grep -q "FAIL"; then
    record "A10-verify-placement-tampered-token" "token-tamper" "offline-verifier" "verify_tampered_placement_token" "fail" "fail" "true" "verify-placement_rejected_tampered_signature" "${token_log}"
  else
    failures=$((failures + 1))
    record "A10-verify-placement-tampered-token" "token-tamper" "offline-verifier" "verify_tampered_placement_token" "fail" "pass" "false" "unexpected_verify_placement_pass" "${token_log}"
  fi
  fi
fi

if [ "${failures}" -ne 0 ]; then
  echo "A10 FAIL: ${failures} unexpected outcomes. See ${CSV}" >&2
  exit 1
fi

echo "A10 PASS: trust-chain RBAC and verifier outcome match expected controls. CSV=${CSV}"
