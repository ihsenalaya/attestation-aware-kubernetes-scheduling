#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# A10 full matrix: compromised node/agent and forged placement material.
set -uo pipefail

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
A10_DIR="${A10_DIR:-${OUT_DIR}/a10}"
CSV="${CSV:-${OUT_DIR}/a10_compromised_node_full.csv}"
VERIFY_BIN="${VERIFY_BIN:-${PWD}/dist/verify-placement}"
GOOD_TOKEN="${GOOD_TOKEN:-${OUT_DIR}/e2e-placement-token.json}"
PUBKEY_FILE="${PUBKEY_FILE:-${OUT_DIR}/e2e-scheduler-pubkey.hex}"
mkdir -p "${OUT_DIR}" "${A10_DIR}"

echo "timestamp,env,attack_id,variant,adversary,expected,actual,blocked,status,reason,raw_log_path" > "${CSV}"

record() {
  printf '%s,%s,A10,%s,%s,BLOCKED,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "${ENV_NAME}" "$1" "$2" "$3" "$4" "$5" "$6" "$7" >> "${CSV}"
}

if [ -x "${VERIFY_BIN}" ]; then
  verify_cmd="${VERIFY_BIN}"
elif command -v verify-placement >/dev/null 2>&1; then
  verify_cmd="verify-placement"
else
  verify_cmd=""
fi

if kubectl get nodes >/dev/null 2>&1; then
  set +e
  VERIFY_BIN="${verify_cmd:-verify-placement}" PLATFORM_NS="${PLATFORM_NS}" ENV_NAME="${ENV_NAME}" \
    OUT_DIR="${A10_DIR}" GOOD_TOKEN="${GOOD_TOKEN}" PUBKEY_FILE="${PUBKEY_FILE}" \
    bash experiments/attacks/a10_compromised_node_forge_evidence.sh >/dev/null 2>&1
  a10_rc=$?
  set -e
  if [ "${a10_rc}" -eq 0 ]; then
    record "rbac-and-forged-raw-report" "compromised-node-agent" "BLOCKED" "yes" "EXECUTED" "base_a10_script_passed" "${A10_DIR}/a10-rbac-results.csv"
  else
    record "rbac-and-forged-raw-report" "compromised-node-agent" "FAILED" "no" "FAIL" "base_a10_script_failed" "${A10_DIR}/a10-rbac-results.csv"
  fi
else
  record "rbac-and-forged-raw-report" "compromised-node-agent" "NOT_EXECUTED" "no" "NOT_EXECUTED" "cluster_unreachable" ""
fi

offline_log="${A10_DIR}/A10-offline-token-negative.log"
: > "${offline_log}"

if [ -z "${verify_cmd}" ] || [ ! -f "${PUBKEY_FILE}" ]; then
  record "absent-token" "offline-verifier" "NOT_EXECUTED" "no" "NOT_EXECUTED" "verify_placement_or_pubkey_missing" "${offline_log}"
  record "invalid-token" "offline-verifier" "NOT_EXECUTED" "no" "NOT_EXECUTED" "verify_placement_or_pubkey_missing" "${offline_log}"
  record "poduid-mismatch" "offline-verifier" "NOT_EXECUTED" "no" "NOT_EXECUTED" "verify_placement_or_pubkey_missing" "${offline_log}"
  exit 0
fi

set +e
absent_out="$("${verify_cmd}" -pubkey-hex "$(cat "${PUBKEY_FILE}")" 2>&1)"
absent_rc=$?
set -e
printf 'absent-token rc=%s\n%s\n' "${absent_rc}" "${absent_out}" >> "${offline_log}"
if [ "${absent_rc}" -ne 0 ] && printf '%s\n' "${absent_out}" | grep -q "FAIL"; then
  record "absent-token" "offline-verifier" "FAIL" "yes" "EXECUTED" "token_required" "${offline_log}"
else
  record "absent-token" "offline-verifier" "PASS" "no" "FAIL" "missing_token_was_not_rejected" "${offline_log}"
fi

bad_token="${A10_DIR}/a10-invalid-token.json"
printf '{"payload":{"pod_uid":"x"},"signature":"not-hex"}\n' > "${bad_token}"
set +e
invalid_out="$("${verify_cmd}" -token-file "${bad_token}" -pubkey-hex "$(cat "${PUBKEY_FILE}")" 2>&1)"
invalid_rc=$?
set -e
printf 'invalid-token rc=%s\n%s\n' "${invalid_rc}" "${invalid_out}" >> "${offline_log}"
if [ "${invalid_rc}" -ne 0 ] && printf '%s\n' "${invalid_out}" | grep -q "FAIL"; then
  record "invalid-token" "offline-verifier" "FAIL" "yes" "EXECUTED" "invalid_signature_or_payload_rejected" "${offline_log}"
else
  record "invalid-token" "offline-verifier" "PASS" "no" "FAIL" "invalid_token_was_not_rejected" "${offline_log}"
fi

if [ ! -f "${GOOD_TOKEN}" ]; then
  record "poduid-mismatch" "offline-verifier" "NOT_EXECUTED" "no" "NOT_EXECUTED" "good_token_missing" "${offline_log}"
else
  set +e
  mismatch_out="$("${verify_cmd}" -token-file "${GOOD_TOKEN}" -pubkey-hex "$(cat "${PUBKEY_FILE}")" -pod-uid "definitely-not-the-pod-uid" 2>&1)"
  mismatch_rc=$?
  set -e
  printf 'poduid-mismatch rc=%s\n%s\n' "${mismatch_rc}" "${mismatch_out}" >> "${offline_log}"
  if [ "${mismatch_rc}" -ne 0 ] && printf '%s\n' "${mismatch_out}" | grep -q "FAIL"; then
    record "poduid-mismatch" "offline-verifier" "FAIL" "yes" "EXECUTED" "verify_for_pod_rejected_mismatch" "${offline_log}"
  else
    record "poduid-mismatch" "offline-verifier" "PASS" "no" "FAIL" "poduid_mismatch_was_not_rejected" "${offline_log}"
  fi
fi
