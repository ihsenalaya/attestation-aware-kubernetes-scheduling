#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# measure_identity_binding.sh — Exp6 Identity Binding / Verifiable Placement.
#
# Takes a real minted placement token (from the e2e positive run) and verifies:
#   (a) it PASSES offline with the correct expected values and the public key;
#   (b) it FAILS when ANY bound field is mutated (podUID, podSpecHash, node,
#       evidenceHash, policyHash) — proving the decision is cryptographically
#       bound and offline-verifiable without cluster access.
# Real: uses the verify-placement CLI; no fabricated outcomes.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
VERIFY_BIN="${VERIFY_BIN:-${REPO_ROOT}/dist/verify-placement}"
TOKEN="${TOKEN:-${OUT_DIR}/e2e-placement-token.json}"
PUBKEY_FILE="${PUBKEY_FILE:-${OUT_DIR}/e2e-scheduler-pubkey.hex}"
DECISION="${DECISION:-${OUT_DIR}/e2e-placement-decision.yaml}"
CSV="${CSV:-${OUT_DIR}/identity_binding.csv}"
mkdir -p "${OUT_DIR}"

echo "timestamp,env,case,mutated_field,expected,actual,pass,raw_log_path" > "${CSV}"

if [ ! -f "${TOKEN}" ] || [ ! -f "${PUBKEY_FILE}" ]; then
  echo "PREREQ MISSING: run e2e-kind-positive.sh first to mint ${TOKEN}" >&2
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},prereq,,PASS,MISSING,FAIL,${TOKEN}" >> "${CSV}"
  exit 2
fi
PUB="$(cat "${PUBKEY_FILE}" | tr -d '\r\n')"

# Extract the true bound values from the token itself (the honest ground truth).
read -r POD_UID POD_HASH NODE EV_HASH POL_HASH <<EOF
$(python3 - "${TOKEN}" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))["payload"]
print(p.get("pod_uid",""), p.get("pod_spec_hash",""), p.get("node_identity",""), p.get("evidence_hash",""), p.get("policy_hash",""))
PY
)
EOF

record() { # case field expected actual raw
  local ok="FAIL"; [ "$3" = "$4" ] && ok="PASS"
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},$1,$2,$3,$4,${ok},$5" >> "${CSV}"
  printf '  %-22s field=%-14s exp=%-4s act=%-4s %s\n' "$1" "${2:-none}" "$3" "$4" "${ok}"
}

run_verify() { # extra-args... -> prints PASS/FAIL
  local raw="${OUT_DIR}/.idb.log"
  if "${VERIFY_BIN}" -token-file "${TOKEN}" -pubkey-hex "${PUB}" "$@" >"${raw}" 2>&1; then echo PASS; else echo FAIL; fi
}

echo "== [identity binding] env=${ENV_NAME}"
# (a) correct values -> PASS
a="$(run_verify -pod-uid "${POD_UID}" -pod-spec-hash "${POD_HASH}" -node "${NODE}" -evidence-hash "${EV_HASH}" -policy-hash "${POL_HASH}")"
record correct-binding none PASS "${a}" "${OUT_DIR}/.idb.log"
# (b) each mutation -> FAIL
a="$(run_verify -pod-uid WRONG-uid -pod-spec-hash "${POD_HASH}" -node "${NODE}" -evidence-hash "${EV_HASH}" -policy-hash "${POL_HASH}")"; record mutate-poduid podUID FAIL "${a}" "${OUT_DIR}/.idb.log"
a="$(run_verify -pod-uid "${POD_UID}" -pod-spec-hash WRONGHASH -node "${NODE}" -evidence-hash "${EV_HASH}" -policy-hash "${POL_HASH}")"; record mutate-podspechash podSpecHash FAIL "${a}" "${OUT_DIR}/.idb.log"
a="$(run_verify -pod-uid "${POD_UID}" -pod-spec-hash "${POD_HASH}" -node WRONG-node -evidence-hash "${EV_HASH}" -policy-hash "${POL_HASH}")"; record mutate-node nodeIdentity FAIL "${a}" "${OUT_DIR}/.idb.log"
a="$(run_verify -pod-uid "${POD_UID}" -pod-spec-hash "${POD_HASH}" -node "${NODE}" -evidence-hash WRONGEV -policy-hash "${POL_HASH}")"; record mutate-evidencehash evidenceHash FAIL "${a}" "${OUT_DIR}/.idb.log"
a="$(run_verify -pod-uid "${POD_UID}" -pod-spec-hash "${POD_HASH}" -node "${NODE}" -evidence-hash "${EV_HASH}" -policy-hash WRONGPOL)"; record mutate-policyhash policyHash FAIL "${a}" "${OUT_DIR}/.idb.log"
rm -f "${OUT_DIR}/.idb.log"

fails="$(tail -n +2 "${CSV}" | awk -F, '$7=="FAIL"{c++} END{print c+0}')"
echo "== identity binding done (CSV: ${CSV}); non-conforming rows=${fails}"
[ "${fails}" -eq 0 ] || { echo "FAIL: identity binding not enforced as expected" >&2; exit 1; }
echo "== Verifiable Placement + Identity Binding confirmed"
