#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# AKS-only completion harness for A2/A3/A6/A7/A9.
#
# These attacks are meant to be re-run on the real SEV-SNP AKS environment.
# Outside ENV_NAME=aks-real-sevsnp the script writes NOT_EXECUTED rows instead
# of converting kind/unit evidence into fake AKS evidence.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_LIB="${SCRIPT_DIR}/../harness/lib.sh"
[ -f "${HARNESS_LIB}" ] && source "${HARNESS_LIB}"

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
CSV="${CSV:-${OUT_DIR}/security_attacks_missing_A2_A3_A6_A7_A9.csv}"
N_RUNS="${N_RUNS_SECURITY:-30}"
ALLOW_NON_AKS="${ALLOW_NON_AKS:-false}"
mkdir -p "${OUT_DIR}"

echo "timestamp,env,run_id,attack_id,adversary,scenario,expected,actual,blocked,status,reason,raw_log_path" > "${CSV}"

record() {
  printf '%s,%s,%s,%s,%s,%s,BLOCKED,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "${ENV_NAME}" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" >> "${CSV}"
}

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

if [[ "${ENV_NAME}" != aks* && "${ALLOW_NON_AKS}" != "true" ]]; then
  for attack in A2 A3 A6 A7 A9; do
    record "not-executed" "${attack}" "adversarial-principal" \
      "AKS-required attack not run on current environment" \
      "NOT_EXECUTED" "no" "NOT_EXECUTED" "requires_fresh_aks_real_sevsnp_cluster" ""
  done
  echo "AKS missing attacks not executed outside AKS (CSV: ${CSV})"
  exit 0
fi

if ! kubectl_retry get nodes >/dev/null; then
  for attack in A2 A3 A6 A7 A9; do
    record "not-executed" "${attack}" "adversarial-principal" \
      "AKS cluster unreachable" "NOT_EXECUTED" "no" "NOT_EXECUTED" "kubectl_cluster_unreachable" ""
  done
  exit 0
fi

ATTACK_NS="${ATTACK_NS:-article1-missing-attacks}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
NODE="${NODE:-}"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
if [[ "${ENV_NAME}" == aks* ]]; then
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
else
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
fi
RAW_DIR="${OUT_DIR}/missing-attacks"
mkdir -p "${RAW_DIR}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
  - key: ai.sovereign.io/confidential
    operator: Equal
    value: "true"
    effect: NoSchedule'
fi

detect_node() {
  if [ -n "${NODE}" ]; then
    printf '%s\n' "${NODE}"
    return 0
  fi
  if declare -F detect_verified_node >/dev/null 2>&1; then
    detect_verified_node "${REQUIRED_TEE}" real
    return $?
  fi
  kubectl get attestationevidences -A -o json | REQUIRED_TEE="${REQUIRED_TEE}" MAX_AGE_SECONDS="${MAX_EVIDENCE_AGE_SECONDS:-240}" python3 -c '
import datetime as dt, json, os, sys
data=json.load(sys.stdin)
required=os.environ["REQUIRED_TEE"].upper()
max_age=int(os.environ.get("MAX_AGE_SECONDS", "240"))
now=dt.datetime.now(dt.timezone.utc)
best=None
for item in data.get("items", []):
    spec=item.get("spec", {})
    status=item.get("status", {})
    ts=status.get("lastVerifiedTime")
    if not ts:
        continue
    try:
        last=dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        continue
    age=(now-last).total_seconds()
    if age > max_age:
        continue
    if str(spec.get("tee", "")).upper() == required and status.get("verified") and not status.get("revoked"):
        node=spec.get("subjectRef", {}).get("name", "")
        if node:
            if best is None or last > best[0]:
                best=(last, node)
if best:
    print(best[1])
    raise SystemExit(0)
raise SystemExit(1)
'
}

set +e
TARGET_NODE="$(detect_node)"
node_rc=$?
set -e
if [ "${node_rc}" -ne 0 ] || [ -z "${TARGET_NODE}" ]; then
  for attack in A2 A3 A6 A7 A9; do
    record "not-executed" "${attack}" "adversarial-principal" \
      "no verified target node/evidence found" "NOT_EXECUTED" "no" "NOT_EXECUTED" "no_verified_${REQUIRED_TEE}_evidence" ""
  done
  exit 0
fi

reset_ns() {
  local raw="${RAW_DIR}/reset.log"
  kubectl_retry delete namespace "${ATTACK_NS}" --ignore-not-found --wait=true > "${raw}" 2>&1
  kubectl_retry create namespace "${ATTACK_NS}" >> "${raw}" 2>&1
  kubectl_retry label namespace "${ATTACK_NS}" ai.sovereign.io/sensitivity=high --overwrite >> "${raw}" 2>&1
}

apply_policy() {
  local max_age="$1"
  apply_manifest >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata:
  name: missing-attack-policy
  namespace: ${ATTACK_NS}
spec:
  target:
    namespaceSelector:
      matchLabels:
        ai.sovereign.io/sensitivity: high
    workloadSelector:
      matchLabels:
        app: missing-victim
  requiredTEE: ["${REQUIRED_TEE}"]
  requireConfidentialContainers: ${REQUIRE_CONFIDENTIAL_CONTAINERS}
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: ${max_age}
  requireModelDigest: true
  enforcementMode: enforce
EOF
}

pod_blocked() {
  local name="$1"
  local errors=0
  for _ in $(seq 1 20); do
    local node rc out
    set +e
    out="$(kubectl -n "${ATTACK_NS}" get pod "${name}" -o jsonpath='{.spec.nodeName}' 2>&1)"
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
    [ -n "${node}" ] && { printf 'BOUND:%s\n' "${node}"; return 0; }
    sleep 1
  done
  [ "${errors}" -ge 20 ] && { printf 'INFRA_ERROR\n'; return 0; }
  printf 'PENDING\n'
}

submit_victim() {
  local name="$1" evidence="$2" extra_annotations="${3:-}"
  apply_manifest >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${ATTACK_NS}
  labels:
    app: missing-victim
  annotations:
    ai.sovereign.io/model-digest: "sha256:${name}"
    ai.sovereign.io/attestation-evidence: "${evidence}"
${extra_annotations}
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
EOF
}

mark_evidence_status() {
  local ns="$1" name="$2" patch="$3"
  kubectl_retry patch attestationevidence "${name}" -n "${ns}" --subresource=status --type=merge -p "{\"status\":${patch}}" >/dev/null
}

backup_evidence_status() {
  local ns="$1" name="$2" dst="$3"
  kubectl_retry get attestationevidence "${name}" -n "${ns}" -o json | python3 -c '
import json, sys
data = json.load(sys.stdin)
status = data.get("status", {})
# JSON merge-patch does not remove absent fields. Preserve the healthy
# "not revoked" state explicitly so A3/A7 restore cannot leave revoked=true.
if "revoked" not in status:
    status["revoked"] = False
json.dump(status, open(sys.argv[1], "w"))
' "${dst}"
}

restore_evidence_status() {
  local ns="$1" name="$2" src="$3"
  [ -f "${src}" ] || return 0
  kubectl_retry patch attestationevidence "${name}" -n "${ns}" --subresource=status --type=merge -p "{\"status\":$(cat "${src}")}" >/dev/null
}

first_evidence_name() {
  kubectl_retry get attestationevidences -A -o json | TARGET_NODE="${TARGET_NODE}" REQUIRED_TEE="${REQUIRED_TEE}" MAX_AGE_SECONDS="${MAX_EVIDENCE_AGE_SECONDS:-240}" python3 -c '
import datetime as dt, json, os, sys
data=json.load(sys.stdin)
target=os.environ["TARGET_NODE"]
required=os.environ["REQUIRED_TEE"].upper()
max_age=int(os.environ.get("MAX_AGE_SECONDS", "240"))
now=dt.datetime.now(dt.timezone.utc)
best=None
for item in data.get("items", []):
    spec=item.get("spec", {})
    status=item.get("status", {})
    if spec.get("subjectRef", {}).get("name") != target:
        continue
    if str(spec.get("tee", "")).upper() != required:
        continue
    if not status.get("verified") or status.get("revoked"):
        continue
    ts=status.get("lastVerifiedTime")
    if not ts:
        continue
    try:
        last=dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        continue
    if (now-last).total_seconds() > max_age:
        continue
    if best is None or last > best[0]:
        best=(last, item["metadata"]["namespace"] + "/" + item["metadata"]["name"])
if best:
    print(best[1])
    raise SystemExit(0)
raise SystemExit(1)
'
}

set +e
EVIDENCE_REF="$(first_evidence_name)"
evidence_rc=$?
set -e
if [ "${evidence_rc}" -ne 0 ]; then
  for attack in A2 A3 A6 A7 A9; do
    record "not-executed" "${attack}" "adversarial-principal" \
      "target evidence missing" "NOT_EXECUTED" "no" "NOT_EXECUTED" "target_evidence_missing" ""
  done
  exit 0
fi
EVIDENCE_NS="${EVIDENCE_REF%%/*}"
EVIDENCE_NAME="${EVIDENCE_REF#*/}"

SCHEDULER_DEPLOYMENT="${SCHEDULER_DEPLOYMENT:-attestation-scheduler}"
PREBIND_DELAY_MS="${PREBIND_DELAY_MS:-10000}"
PREBIND_PATCH_AFTER_SECONDS="${PREBIND_PATCH_AFTER_SECONDS:-1}"
FRESH_POLICY_MAX_AGE_SECONDS="${FRESH_POLICY_MAX_AGE_SECONDS:-7200}"
RACE_POLICY_MAX_AGE_SECONDS="${RACE_POLICY_MAX_AGE_SECONDS:-$((FRESH_POLICY_MAX_AGE_SECONDS - 1))}"
if [ "${RACE_POLICY_MAX_AGE_SECONDS}" -lt 1 ]; then
  RACE_POLICY_MAX_AGE_SECONDS=1
fi

set_prebind_delay() {
  local value="$1"
  kubectl_retry -n "${PLATFORM_NS}" set env "deployment/${SCHEDULER_DEPLOYMENT}" "AIOPS_PREBIND_TEST_DELAY_MS=${value}" >/dev/null
  kubectl_retry -n "${PLATFORM_NS}" rollout status "deployment/${SCHEDULER_DEPLOYMENT}" --timeout=240s >/dev/null
}

clear_prebind_delay() {
  set +e
  kubectl_retry -n "${PLATFORM_NS}" set env "deployment/${SCHEDULER_DEPLOYMENT}" AIOPS_PREBIND_TEST_DELAY_MS- >/dev/null
  kubectl_retry -n "${PLATFORM_NS}" rollout status "deployment/${SCHEDULER_DEPLOYMENT}" --timeout=240s >/dev/null
  set -e
}

ACTIVE_EVIDENCE_NS=""
ACTIVE_EVIDENCE_NAME=""
ACTIVE_EVIDENCE_BACKUP=""
ACTIVE_RUNTIME_CLASS=""

restore_active_evidence() {
  if [ -n "${ACTIVE_EVIDENCE_NS}" ] && [ -n "${ACTIVE_EVIDENCE_NAME}" ] && [ -f "${ACTIVE_EVIDENCE_BACKUP}" ]; then
    set +e
    restore_evidence_status "${ACTIVE_EVIDENCE_NS}" "${ACTIVE_EVIDENCE_NAME}" "${ACTIVE_EVIDENCE_BACKUP}" >/dev/null 2>&1
    set -e
  fi
  ACTIVE_EVIDENCE_NS=""
  ACTIVE_EVIDENCE_NAME=""
  ACTIVE_EVIDENCE_BACKUP=""
}

cleanup() {
  restore_active_evidence
  if [ -n "${ACTIVE_RUNTIME_CLASS}" ]; then
    set +e
    kubectl_retry delete runtimeclass "${ACTIVE_RUNTIME_CLASS}" --ignore-not-found >/dev/null 2>&1
    set -e
  fi
  clear_prebind_delay
}

ensure_adversarial_simulated_runtime_class() {
  local name="$1"
  apply_manifest >/dev/null <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${name}
  labels:
    ai.sovereign.io/simulated: "true"
handler: runc
EOF
}

delete_adversarial_simulated_runtime_class() {
  local name="$1"
  set +e
  kubectl_retry delete runtimeclass "${name}" --ignore-not-found >/dev/null 2>&1
  set -e
}

placement_decision_name() {
  local pod_name="$1"
  local decision="${pod_name}-${ATTACK_NS}"
  printf '%s\n' "${decision:0:63}"
}

wait_placement_decision_exists() {
  local pod_name="$1" raw="$2"
  local decision
  decision="$(placement_decision_name "${pod_name}")"
  for _ in $(seq 1 40); do
    if kubectl_retry -n "${ATTACK_NS}" get aiplacementdecision "${decision}" >> "${raw}" 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

set_prebind_delay "${PREBIND_DELAY_MS}"
trap cleanup EXIT

for run in $(seq 1 "${N_RUNS}"); do
  reset_ns
  apply_policy 1
  raw="${RAW_DIR}/A2-run-${run}.log"
  backup="${RAW_DIR}/A2-run-${run}-evidence-status.json"
  ACTIVE_EVIDENCE_NS="${EVIDENCE_NS}"
  ACTIVE_EVIDENCE_NAME="${EVIDENCE_NAME}"
  ACTIVE_EVIDENCE_BACKUP="${backup}"
  backup_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" > "${raw}" 2>&1
  stale_time="$(date -u -d '10 minutes ago' '+%Y-%m-%dT%H:%M:%SZ')"
  mark_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "{\"lastVerifiedTime\":\"${stale_time}\"}" >> "${raw}" 2>&1
  submit_victim "a2-${run}" "${EVIDENCE_NAME}" >> "${raw}" 2>&1
  actual="$(pod_blocked "a2-${run}")"
  restore_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" >> "${raw}" 2>&1
  ACTIVE_EVIDENCE_NS=""
  ACTIVE_EVIDENCE_NAME=""
  ACTIVE_EVIDENCE_BACKUP=""
  [ "${actual}" = "PENDING" ] && record "run-${run}" A2 "adversarial-principal" "expired evidence rejected" "${actual}" yes EXECUTED expired_evidence_blocked "${raw}" || record "run-${run}" A2 "adversarial-principal" "expired evidence rejected" "${actual}" no FAIL expired_evidence_bound "${raw}"

  reset_ns
  apply_policy "${FRESH_POLICY_MAX_AGE_SECONDS}"
  raw="${RAW_DIR}/A3-run-${run}.log"
  backup="${RAW_DIR}/A3-run-${run}-evidence-status.json"
  ACTIVE_EVIDENCE_NS="${EVIDENCE_NS}"
  ACTIVE_EVIDENCE_NAME="${EVIDENCE_NAME}"
  ACTIVE_EVIDENCE_BACKUP="${backup}"
  backup_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" > "${raw}" 2>&1
  mark_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" '{"revoked":true}' >> "${raw}" 2>&1
  submit_victim "a3-${run}" "${EVIDENCE_NAME}" >> "${raw}" 2>&1
  actual="$(pod_blocked "a3-${run}")"
  restore_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" >> "${raw}" 2>&1
  ACTIVE_EVIDENCE_NS=""
  ACTIVE_EVIDENCE_NAME=""
  ACTIVE_EVIDENCE_BACKUP=""
  [ "${actual}" = "PENDING" ] && record "run-${run}" A3 "adversarial-principal" "revoked evidence rejected" "${actual}" yes EXECUTED revoked_evidence_blocked "${raw}" || record "run-${run}" A3 "adversarial-principal" "revoked evidence rejected" "${actual}" no FAIL revoked_evidence_bound "${raw}"

  reset_ns
  apply_policy "${FRESH_POLICY_MAX_AGE_SECONDS}"
  raw="${RAW_DIR}/A6-run-${run}.log"
  submit_victim "a6-${run}" "${EVIDENCE_NAME}" > "${raw}" 2>&1
  if wait_placement_decision_exists "a6-${run}" "${raw}"; then
    sleep "${PREBIND_PATCH_AFTER_SECONDS}"
    kubectl_retry patch confidentialinferencepolicy missing-attack-policy -n "${ATTACK_NS}" --type=merge -p "{\"spec\":{\"maxEvidenceAgeSeconds\":${RACE_POLICY_MAX_AGE_SECONDS}}}" >> "${raw}" 2>&1
    actual="$(pod_blocked "a6-${run}")"
    [ "${actual}" = "PENDING" ] && record "run-${run}" A6 "adversarial-principal" "policy modified between Filter and PreBind" "${actual}" yes EXECUTED policy_hash_mismatch_prebind_blocked "${raw}" || record "run-${run}" A6 "adversarial-principal" "policy modified between Filter and PreBind" "${actual}" no FAIL policy_race_bound_or_infra "${raw}"
  else
    record "run-${run}" A6 "adversarial-principal" "policy modified between Filter and PreBind" "INFRA_ERROR" no FAIL placement_decision_not_observed "${raw}"
  fi

  reset_ns
  apply_policy "${FRESH_POLICY_MAX_AGE_SECONDS}"
  raw="${RAW_DIR}/A7-run-${run}.log"
  backup="${RAW_DIR}/A7-run-${run}-evidence-status.json"
  ACTIVE_EVIDENCE_NS="${EVIDENCE_NS}"
  ACTIVE_EVIDENCE_NAME="${EVIDENCE_NAME}"
  ACTIVE_EVIDENCE_BACKUP="${backup}"
  backup_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" > "${raw}" 2>&1
  submit_victim "a7-${run}" "${EVIDENCE_NAME}" >> "${raw}" 2>&1
  if wait_placement_decision_exists "a7-${run}" "${raw}"; then
    sleep "${PREBIND_PATCH_AFTER_SECONDS}"
    mark_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" '{"revoked":true}' >> "${raw}" 2>&1
    actual="$(pod_blocked "a7-${run}")"
    restore_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" >> "${raw}" 2>&1
    ACTIVE_EVIDENCE_NS=""
    ACTIVE_EVIDENCE_NAME=""
    ACTIVE_EVIDENCE_BACKUP=""
    [ "${actual}" = "PENDING" ] && record "run-${run}" A7 "adversarial-principal" "evidence revoked between Filter and PreBind" "${actual}" yes EXECUTED revoked_evidence_prebind_blocked "${raw}" || record "run-${run}" A7 "adversarial-principal" "evidence revoked between Filter and PreBind" "${actual}" no FAIL revoked_race_bound_or_infra "${raw}"
  else
    restore_evidence_status "${EVIDENCE_NS}" "${EVIDENCE_NAME}" "${backup}" >> "${raw}" 2>&1 || true
    ACTIVE_EVIDENCE_NS=""
    ACTIVE_EVIDENCE_NAME=""
    ACTIVE_EVIDENCE_BACKUP=""
    record "run-${run}" A7 "adversarial-principal" "evidence revoked between Filter and PreBind" "INFRA_ERROR" no FAIL placement_decision_not_observed "${raw}"
  fi

  reset_ns
  raw="${RAW_DIR}/A9-run-${run}.log"
  saved_require_cc="${REQUIRE_CONFIDENTIAL_CONTAINERS}"
  policy_runtime="${RUNTIME_CLASS}"
  attack_runtime="${SIMULATED_RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  ACTIVE_RUNTIME_CLASS="${attack_runtime}"
  ensure_adversarial_simulated_runtime_class "${attack_runtime}" > "${raw}" 2>&1
  REQUIRE_CONFIDENTIAL_CONTAINERS=true
  apply_policy "${FRESH_POLICY_MAX_AGE_SECONDS}" >> "${raw}" 2>&1
  REQUIRE_CONFIDENTIAL_CONTAINERS="${saved_require_cc}"
  set +e
  out="$(apply_manifest_expect_denial <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: a9-${run}
  namespace: ${ATTACK_NS}
  labels:
    app: missing-victim
  annotations:
    ai.sovereign.io/model-digest: "sha256:a9-${run}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_NAME}"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${attack_runtime}
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
EOF
)"
  rc=$?
  set -e
  printf 'policy_runtime=%s\nattack_runtime=%s\n%s\nrc=%s\n' "${policy_runtime}" "${attack_runtime}" "${out}" "${rc}" >> "${raw}"
  if [ "${rc}" -ne 0 ] && printf '%s\n' "${out}" | grep -qi 'simulated runtimeClass\|forbidden in production\|denied'; then
    record "run-${run}" A9 "adversarial-principal" "simulated RuntimeClass forbidden in production" "DENIED" yes EXECUTED simulated_runtime_denied "${raw}"
  else
    record "run-${run}" A9 "adversarial-principal" "simulated RuntimeClass forbidden in production" "ADMITTED_OR_WRONG_DENIAL" no FAIL simulated_runtime_not_denied "${raw}"
  fi
  delete_adversarial_simulated_runtime_class "${attack_runtime}"
  ACTIVE_RUNTIME_CLASS=""
done

failures="$(python3 - "${CSV}" <<'PY'
import csv, sys
rows=list(csv.DictReader(open(sys.argv[1], newline="", encoding="utf-8")))
print(sum(1 for row in rows if row.get("status") == "FAIL"))
PY
)"
if [ "${failures}" != "0" ]; then
  echo "FAIL: ${failures} missing attack rows failed (CSV: ${CSV})" >&2
  exit 1
fi
echo "AKS missing attacks harness completed (CSV: ${CSV})"
