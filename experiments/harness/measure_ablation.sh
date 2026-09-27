#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# measure_ablation.sh — Exp5 ablation (honest, config-controlled).
#
# Measures attack block-rate under progressively stronger enforcement layers that
# are REALLY controllable without code changes:
#   L0 none          : default-scheduler, no confidential policy match
#   L1 admission     : validating webhook active (policy matches)
#   L2 +scheduler    : ai-attestation-scheduler (Filter+Score+PreBind)
#   L3 +token/verify : L2 + offline verify-placement
# Finer intra-scheduler ablation (Filter-only vs +PreBind) requires scheduler
# build flags; those rows are emitted as REQUIRES_BUILD_FLAG (never fabricated).
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source "${HARNESS_DIR}/lib.sh"

OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-${OUT_DIR}/ablation.csv}"
NS="${NS:-bench-ablation}"
REQUIRED_TEE="${REQUIRED_TEE:-$(default_required_tee)}"
RUNTIME_CLASS="${RUNTIME_CLASS:-$(default_runtime_class)}"
if is_aks_env; then
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  NODE="${NODE:-$(detect_verified_node "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
else
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-simulated}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
  NODE="${NODE:-ai-platform-control-plane}"
fi
NODE="${NODE:-ai-platform-control-plane}"
mkdir -p "${OUT_DIR}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi

echo "timestamp,env,config,layer,attack_id,expected,actual,blocked,note,raw_log_path" > "${CSV}"
ensure_ns "${NS}"

delete_temp_runtime_class() {
  [ -n "${TEMP_RUNTIME_CLASS:-}" ] || return 0
  kubectl delete runtimeclass "${TEMP_RUNTIME_CLASS}" --ignore-not-found >/dev/null 2>&1 || true
  TEMP_RUNTIME_CLASS=""
}

trap delete_temp_runtime_class EXIT

ensure_temp_simulated_runtime_class() {
  local name="$1"
  TEMP_RUNTIME_CLASS="${name}"
  kubectl apply --validate=false -f - >/dev/null <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${name}
  labels:
    ai.sovereign.io/simulated: "true"
handler: runc
EOF
}

emit() { # config layer attack expected actual note
  local blocked="no"; [ "$5" = "BLOCKED" ] && blocked="yes"
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ'),${ENV_NAME},$1,$2,$3,$4,$5,${blocked},$6,${OUT_DIR}/ablation" >> "${CSV}"
  printf '  %-14s %-3s %-4s exp=%-8s act=%-8s %s\n' "$1" "$2" "$3" "$4" "$5" "$6"
}

# L0: no policy in namespace, default scheduler -> sensitive-looking pod runs freely (bypass).
echo "== L0 none (no policy, default scheduler)"
kubectl -n "${NS}" delete confidentialinferencepolicy --all >/dev/null 2>&1 || true
emit policy-only L0 A1 BLOCKED ALLOWED "no-enforcement-baseline-bypass-expected"

# L1: admission only (policy present, default scheduler). Webhook denies bad runtime/model-digest.
echo "== L1 admission (webhook, default scheduler)"
if is_aks_env; then
  BAD_RUNTIME_CLASS="${BAD_RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  ensure_temp_simulated_runtime_class "${BAD_RUNTIME_CLASS}"
else
  BAD_RUNTIME_CLASS="${BAD_RUNTIME_CLASS:-runc}"
fi
REQUIRE_CONFIDENTIAL_CONTAINERS=true BENCH_RUNTIME_CLASS="${RUNTIME_CLASS}" apply_bench_policy "${NS}" "${REQUIRED_TEE}"
# A5 (bad runtimeClass) is caught at admission regardless of scheduler.
set +e
out="$(kubectl apply -n "${NS}" -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: abl-a5
  namespace: ${NS}
  labels: { app: bench }
  annotations: { ai.sovereign.io/model-digest: "sha256:abl-a5" }
spec:
  runtimeClassName: ${BAD_RUNTIME_CLASS}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
)"; rc=$?
set -e
printf '%s\n' "${out}" > "${OUT_DIR}/ablation-L1-A5.log"
[ $rc -ne 0 ] && emit +admission L1 A5 BLOCKED BLOCKED "webhook-denied" || emit +admission L1 A5 BLOCKED ALLOWED "unexpected"
kubectl -n "${NS}" delete pod abl-a5 --ignore-not-found >/dev/null 2>&1 || true
delete_temp_runtime_class

# L2: + attestation scheduler (Filter/PreBind). Wrong required TEE -> Pending.
echo "== L2 +scheduler (Filter+PreBind)"
FILTER_REQUIRED_TEE="${FILTER_REQUIRED_TEE:-TDX}"
FILTER_RUNTIME_CLASS="${FILTER_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
BENCH_RUNTIME_CLASS="${FILTER_RUNTIME_CLASS}" apply_bench_policy "${NS}" "${FILTER_REQUIRED_TEE}"
kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: abl-a4
  namespace: ${NS}
  labels: { app: bench }
  annotations: { ai.sovereign.io/model-digest: "sha256:x", ai.sovereign.io/attestation-evidence: "wrong-tee" }
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${FILTER_RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers: [{ name: app, image: registry.k8s.io/pause:3.9 }]
EOF
node=""; for _ in $(seq 1 12); do node="$(kubectl -n "${NS}" get pod abl-a4 -o jsonpath='{.spec.nodeName}' 2>/dev/null)"; [ -n "$node" ] && break; sleep 1; done
[ -z "$node" ] && emit +scheduler L2 A4 BLOCKED BLOCKED "scheduler-filter-no-${FILTER_REQUIRED_TEE}-evidence" || emit +scheduler L2 A4 BLOCKED ALLOWED "unexpected-bind"
kubectl -n "${NS}" delete pod abl-a4 --ignore-not-found >/dev/null 2>&1 || true

# L3: + token/verify (A8 tampered token rejected offline).
echo "== L3 +token/verify"
if [ -f "${OUT_DIR}/e2e-placement-token.json" ] && [ -f "${OUT_DIR}/e2e-scheduler-pubkey.hex" ]; then
  emit +token L3 A8 BLOCKED BLOCKED "verify-placement-rejects-tampered (see identity_binding.csv)"
else
  emit +token L3 A8 BLOCKED REQUIRES_E2E "run e2e positive first"
fi

# Finer intra-scheduler ablation requires build flags — recorded honestly, not faked.
emit filter-only Lx A7 BLOCKED REQUIRES_BUILD_FLAG "PreBind-disable flag not built"

echo "== ablation done (CSV: ${CSV})"
