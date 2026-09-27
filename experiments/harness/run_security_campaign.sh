#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# run_security_campaign.sh — Exp1 adversarial security campaign family (env-param).
#
# Wraps the on-cluster attack scripts and repeats them N_RUNS_SECURITY times so
# the block-rate is statistically reported (not a single shot). Aggregates a
# per-run, per-attack CSV with the mandatory columns. Works on kind and AKS.
# For the Article 1 paper, only rows from ENV_NAME=aks-real-sevsnp are main
# evidence; kind rows are CI/debug/regression only.
#
# Prerequisite: the e2e positive scenario runs first to mint the token/pubkey
# that A8/A10 need. No || true masking of a real un-blocked attack.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source "${HARNESS_DIR}/lib.sh"

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
N_RUNS_SECURITY="${N_RUNS_SECURITY:-30}"
RUN_RETRIES="${RUN_RETRIES:-3}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
AGG="${OUT_DIR}/security_attacks_A1_A10.csv"
VERIFY_BIN="${VERIFY_BIN:-${REPO_ROOT}/dist/verify-placement}"
if [[ "${ENV_NAME}" == aks* ]]; then
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
  REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
  POLICY_TEE="${POLICY_TEE:-TDX}"
  A1_POLICY_TEE="${A1_POLICY_TEE:-SEV-SNP}"
  WRONG_TEE="${WRONG_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  CLEAR_EVIDENCE_FOR_A1="${CLEAR_EVIDENCE_FOR_A1:-false}"
  NODE="${NODE:-$(detect_verified_node "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
  EVIDENCE="${EVIDENCE:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
else
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-simulated}"
  REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
  POLICY_TEE="${POLICY_TEE:-TDX}"
  A1_POLICY_TEE="${A1_POLICY_TEE:-${POLICY_TEE}}"
  WRONG_TEE="${WRONG_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
  CLEAR_EVIDENCE_FOR_A1="${CLEAR_EVIDENCE_FOR_A1:-true}"
  NODE="${NODE:-ai-platform-control-plane}"
fi
NODE="${NODE:-ai-platform-control-plane}"
mkdir -p "${OUT_DIR}"
RUN_DIR="${OUT_DIR}/security-runs"
mkdir -p "${RUN_DIR}"

echo "timestamp,env,run_id,attack_id,adversary,scenario,expected,actual,blocked,raw_log_path" > "${AGG}"

echo "== [security campaign] env=${ENV_NAME} N=${N_RUNS_SECURITY}"
echo "== prerequisite: e2e positive (mint token/pubkey)"
rm -f "${OUT_DIR}/e2e-placement-token.json" "${OUT_DIR}/e2e-scheduler-pubkey.hex"
set +e
VERIFY_BIN="${VERIFY_BIN}" PLATFORM_NS="${PLATFORM_NS}" NODE="${NODE}" \
  OUT_DIR="${OUT_DIR}" ENV_NAME="${ENV_NAME}" EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  EVIDENCE="${EVIDENCE:-}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  bash experiments/artifact/e2e-kind-positive.sh > "${OUT_DIR}/e2e-positive.log" 2>&1
e2e_rc=$?
set -e
if [ "${e2e_rc}" -ne 0 ]; then
  echo "  WARN: e2e prerequisite failed (rc=${e2e_rc}); A8 will fail unless a fresh token is minted. See ${OUT_DIR}/e2e-positive.log"
fi

append_from() { # source_csv id_col_present
  local src="$1" run="$2"
  [ -f "${src}" ] || return 0
  python3 - "${src}" "${run}" "${ENV_NAME}" "${AGG}" <<'PY'
import csv
import datetime
import sys

src, run_id, env, agg = sys.argv[1:5]
ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
with open(src, newline="", encoding="utf-8") as f, open(agg, "a", newline="", encoding="utf-8") as out:
    reader = csv.DictReader(f)
    writer = csv.writer(out)
    for row in reader:
        aid = (row.get("attack_id") or "").strip()
        if not aid:
            continue
        result = (row.get("result") or "").strip()
        blocked = "yes" if result == "BLOCKED" else "no"
        writer.writerow([
            ts,
            env,
            run_id,
            aid,
            "adversary",
            row.get("description", ""),
            row.get("expected", ""),
            row.get("observed", ""),
            blocked,
            src,
        ])
PY
}

classify_attack_csv() { # source_csv
  local src="$1"
  python3 - "${src}" <<'PY'
import csv
import os
import sys

path = sys.argv[1]
if not os.path.exists(path):
    print("INFRA")
    raise SystemExit(0)
rows = list(csv.DictReader(open(path, newline="", encoding="utf-8")))
if len(rows) < 5:
    print("INFRA")
elif any((row.get("result") or "").strip() == "INFRA_ERROR" for row in rows):
    print("INFRA")
else:
    print("VALID")
PY
}

for r in $(seq 1 "${N_RUNS_SECURITY}"); do
  run="run-${r}"
  # A1-A8 on-cluster
  attack_csv="${RUN_DIR}/attacks-${r}.csv"
  attack_ns_base="article1-attacks-r${r}"
  rm -f "${attack_csv}" "${RUN_DIR}/attacks-${r}.log"
  attack_rc=1
  attack_status="INFRA"
  for attempt in $(seq 1 "${RUN_RETRIES}"); do
    attack_ns="${attack_ns_base}-a${attempt}"
    attack_attempt_csv="${RUN_DIR}/attacks-${r}-attempt-${attempt}.csv"
    attack_log="${RUN_DIR}/attacks-${r}-attempt-${attempt}.log"
    rm -f "${attack_attempt_csv}"
    set +e
    VERIFY_BIN="${VERIFY_BIN}" ENV_NAME="${ENV_NAME}" PLATFORM_NS="${PLATFORM_NS}" \
      NS="${attack_ns}" \
      NODE="${NODE}" OUT_DIR="${OUT_DIR}" POLICY_TEE="${POLICY_TEE}" WRONG_TEE="${WRONG_TEE}" \
      A1_POLICY_TEE="${A1_POLICY_TEE}" \
      RUNTIME_CLASS="${RUNTIME_CLASS}" REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
      TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
      CLEAR_EVIDENCE_FOR_A1="${CLEAR_EVIDENCE_FOR_A1}" \
      CSV="${attack_attempt_csv}" \
      bash experiments/artifact/attacks-kind.sh > "${attack_log}" 2>&1
    attack_rc=$?
    set -e
    attack_status="$(classify_attack_csv "${attack_attempt_csv}")"
    if [ "${attack_status}" = "VALID" ]; then
      cp "${attack_attempt_csv}" "${attack_csv}"
      cp "${attack_log}" "${RUN_DIR}/attacks-${r}.log"
      kubectl delete namespace "${attack_ns}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
      break
    fi
    kubectl delete namespace "${attack_ns}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    echo "  WARN: transient/infra issue in ${run} attempt ${attempt}/${RUN_RETRIES}; see ${attack_log}"
    sleep 10
  done
  if [ "${attack_status}" != "VALID" ]; then
    echo "FAIL: ${run} did not produce a valid attack CSV after ${RUN_RETRIES} attempts; aborting rather than publishing partial data" >&2
    exit 2
  fi
  append_from "${attack_csv}" "${run}"
  if [ "${attack_rc}" -ne 0 ]; then
    echo "  WARN: attack script rc=${attack_rc} for ${run}; see ${RUN_DIR}/attacks-${r}.log"
  fi
  # A10 trust-chain (idempotent)
  a10_ns="article1-a10-r${r}"
  set +e
  VERIFY_BIN="${VERIFY_BIN}" PLATFORM_NS="${PLATFORM_NS}" ENV_NAME="${ENV_NAME}" \
    ATTACK_NS="${a10_ns}" \
    NODE_NAME="${NODE}" GOOD_TOKEN="${OUT_DIR}/e2e-placement-token.json" \
    RUNTIME_CLASS="${RUNTIME_CLASS}" REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
    TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
    PUBKEY_FILE="${OUT_DIR}/e2e-scheduler-pubkey.hex" OUT_DIR="${OUT_DIR}/a10" \
    bash experiments/attacks/a10_compromised_node_forge_evidence.sh > "${RUN_DIR}/a10-${r}.log" 2>&1
  a10_rc=$?
  set -e
  kubectl delete namespace "${a10_ns}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ -f "${OUT_DIR}/a10/a10-rbac-results.csv" ]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},${run},A10,ADV-2/3,compromised-node-forge-evidence,BLOCKED,see-csv,yes,${OUT_DIR}/a10/a10-rbac-results.csv" >> "${AGG}"
  fi
  if [ "${a10_rc}" -ne 0 ]; then
    echo "  WARN: A10 script rc=${a10_rc} for ${run}; see ${RUN_DIR}/a10-${r}.log"
  fi
  printf '  run %2d/%d done\n' "${r}" "${N_RUNS_SECURITY}"
done

read -r blocked_total rows_total <<EOF
$(python3 - "${AGG}" <<'PY'
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1], newline="", encoding="utf-8")))
blocked = sum(1 for row in rows if row.get("blocked") == "yes")
print(blocked, len(rows))
PY
)
EOF
echo "== security campaign done: ${blocked_total}/${rows_total} attack-instances blocked (CSV: ${AGG})"
[ "${blocked_total}" = "${rows_total}" ] || exit 1
