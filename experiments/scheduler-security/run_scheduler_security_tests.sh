#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# run_scheduler_security_tests.sh — G8 Scheduler Self-Security Gate (S1-S30).
#
# Uses `kubectl auth can-i --as=<scheduler SA>` to PROVE the scheduler's RBAC is
# minimal and fail-closed: it can do its job (read policy/evidence, write
# AIPlacementDecision, bind) but CANNOT forge evidence, tamper policies, patch
# node labels, or read arbitrary secrets. Env-parameterised (kind|aks).
#
# Honest: each check records expected vs actual; the gate PASSES only if every
# row's actual == expected. No || true masking.
set -uo pipefail
ENV_NAME="${ENV_NAME:-kind-live-simulated}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
SCHED_SA="${SCHED_SA:-system:serviceaccount:${PLATFORM_NS}:attestation-scheduler}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-paper/tables/scheduler_security_tests.csv}"
RAW="${OUT_DIR}/scheduler-security"
mkdir -p "${OUT_DIR}" "${RAW}" "$(dirname "${CSV}")"

echo "timestamp,env,test_id,service_account,verb,resource,namespace,expected,actual,pass,raw_log_path" > "${CSV}"
PASS=0; TOTAL=0

check() { # test_id verb resource expected(yes|no) [namespace]
  local id="$1" verb="$2" res="$3" exp="$4" ns="${5:-${PLATFORM_NS}}"
  TOTAL=$((TOTAL+1))
  local raw="${RAW}/${id}.log"
  local actual
  actual="$(kubectl auth can-i "${verb}" "${res}" --as="${SCHED_SA}" -n "${ns}" 2>"${raw}" | tr -d '\r' | tr '[:upper:]' '[:lower:]')"
  [ -z "${actual}" ] && actual="no"
  echo "kubectl auth can-i ${verb} ${res} --as=${SCHED_SA} -n ${ns} => ${actual}" >> "${raw}"
  local ok="FAIL"
  if [ "${actual}" = "${exp}" ]; then ok="PASS"; PASS=$((PASS+1)); fi
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},${id},${SCHED_SA},${verb},${res},${ns},${exp},${actual},${ok},${raw}" >> "${CSV}"
  printf '  %-4s %-6s %-45s exp=%-3s act=%-3s %s\n' "${id}" "${verb}" "${res}" "${exp}" "${actual}" "${ok}"
}

check_pod_binding() { # test_id expected(yes|no) [namespace]
  local id="$1" exp="$2" ns="${3:-${PLATFORM_NS}}"
  TOTAL=$((TOTAL+1))
  local raw="${RAW}/${id}.log"
  local actual
  # Kubernetes auth can-i is more reliable with explicit --subresource here;
  # some API servers answer "no" for the shorthand "pods/binding".
  actual="$(kubectl auth can-i create pods --subresource=binding --as="${SCHED_SA}" -n "${ns}" 2>"${raw}" | tr -d '\r' | tr '[:upper:]' '[:lower:]')"
  [ -z "${actual}" ] && actual="no"
  echo "kubectl auth can-i create pods --subresource=binding --as=${SCHED_SA} -n ${ns} => ${actual}" >> "${raw}"
  local ok="FAIL"
  if [ "${actual}" = "${exp}" ]; then ok="PASS"; PASS=$((PASS+1)); fi
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},${id},${SCHED_SA},create,pods/binding,${ns},${exp},${actual},${ok},${raw}" >> "${CSV}"
  printf '  %-4s %-6s %-45s exp=%-3s act=%-3s %s\n' "${id}" create pods/binding "${exp}" "${actual}" "${ok}"
}

echo "== Scheduler self-security (G8) env=${ENV_NAME} SA=${SCHED_SA}"
# FORBIDDEN capabilities (expected = no)
check S1  create attestationevidences.aiops.imperium.io           no
check S2  update attestationevidences.aiops.imperium.io           no
check S3  create rawattestationreports.aiops.imperium.io          no
check S4  update confidentialinferencepolicies.aiops.imperium.io  no
check S5  patch  nodes                                            no
check S6  get    secrets                                          no
check S10 delete attestationevidences.aiops.imperium.io           no
check S11 create mutatingwebhookconfigurations.admissionregistration.k8s.io no
check S12 "*"    "*"                                              no
check S22 get    rawattestationreports.aiops.imperium.io          no
check S23 list   rawattestationreports.aiops.imperium.io          no
check S24 patch  rawattestationreports/status.aiops.imperium.io   no
check S25 create clusterroles.rbac.authorization.k8s.io           no
check S26 create rolebindings.rbac.authorization.k8s.io           no
check S27 impersonate serviceaccounts                             no
check S28 create pods/exec                                        no
check S29 delete pods                                             no
check S30 update pods/status                                      no
# ALLOWED capabilities (expected = yes)
check S7  get    attestationevidences.aiops.imperium.io           yes
check S8  create aiplacementdecisions.aiops.imperium.io           yes
check_pod_binding S9 yes
check S13 list   pods                                             yes
check S14 watch  nodes                                            yes
check S15 list   namespaces                                       yes
check S16 create events                                           yes
check S17 patch  events                                           yes
check S18 get    confidentialinferencepolicies.aiops.imperium.io  yes
check S19 list   aiplacementdecisions.aiops.imperium.io           yes
check S20 update aiplacementdecisions.aiops.imperium.io           yes
check S21 patch  aiplacementdecisions/status.aiops.imperium.io    yes

echo "== scheduler-security summary: ${PASS}/${TOTAL} PASS (CSV: ${CSV})"
[ "${PASS}" -eq "${TOTAL}" ] || { echo "FAIL: scheduler RBAC is not minimal/fail-closed" >&2; exit 1; }
echo "== G8 scheduler RBAC minimal + fail-closed VERIFIED (S1-S30)"
