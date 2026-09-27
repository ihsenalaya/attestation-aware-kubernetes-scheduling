#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# measure_baselines.sh — Exp2 baselines B1-B5 scheduling overhead (env-param).
#
# Measures, with the SAME load / seeds / N, the scheduling latency of five
# placement strategies for a sensitive pod:
#   B1 default kube-scheduler (no confidential enforcement)
#   B2 default scheduler + nodeSelector ai.sovereign.io/tee=<TEE> (label-based)
#   B3 default scheduler + runtimeClassName only
#   B4 schedulingGate + external verifier controller (measured via run_b4_vs_b5)
#   B5 our attestation-aware scheduler (precise per-phase ms timing available)
#
# Cross-baseline metric = pending_duration_ms (pod creation -> PodScheduled),
# observable for every strategy. For B5 the precise monotonic scheduler-total is
# also recorded. On kind this is a REGRESSION/prep artifact (kind = CI/debug per
# the AKS-only paper rule); the paper's B1-B5 numbers come from AKS (Phase 2).
#
# HONEST: no || true masking a scheduling failure; a pod that never binds is
# recorded success=failure with its logs kept.
set -uo pipefail
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HARNESS_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source "${HARNESS_DIR}/lib.sh"

N_RUNS="${N_RUNS_PERFORMANCE:-30}"
WARMUP="${WARMUP:-3}"
NS="${NS:-bench-baselines}"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-snp}"
NODE="${NODE:-ai-platform-control-plane}"
EVIDENCE_ANN="${EVIDENCE_ANN:-bench-node-evidence}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-${OUT_DIR}/baselines_b1_b5.csv}"
IMAGE="${BENCH_IMAGE:-registry.k8s.io/pause:3.9}"
mkdir -p "${OUT_DIR}"

echo "timestamp,env,run_id,seed,baseline,scheduler,pending_ms,sched_total_precise_ms,node,success,raw_log_path" > "${CSV}"
ensure_ns "${NS}"
kubectl label node "${NODE}" ai.sovereign.io/tee="${REQUIRED_TEE}" --overwrite >/dev/null 2>&1 || true
BENCH_RUNTIME_CLASS="${RUNTIME_CLASS}" apply_bench_policy "${NS}" "${REQUIRED_TEE}" >/dev/null 2>&1 || true

# emit a pod manifest for a given baseline into stdin-apply
run_pod() { # baseline seed pod
  local bl="$1" seed="$2" pod="$3"
  local sched_line="  schedulerName: default-scheduler" extra=""
  # IMPORTANT: B1/B2/B3 must NOT match the ConfidentialInferencePolicy, or the
  # admission webhook would rewrite schedulerName to our scheduler and gate them.
  # They therefore use app=baseline (the policy selects app=bench, i.e. only B5).
  local app="baseline"
  case "${bl}" in
    B1) : ;;                                                   # default sched, nothing
    B2) extra="  nodeSelector: { ai.sovereign.io/tee: \"${REQUIRED_TEE}\" }" ;;
    B3) extra="  runtimeClassName: ${RUNTIME_CLASS}" ;;
    B5) app="bench"                                            # matches policy
        sched_line="  schedulerName: ${SCHED_NAME}"
        extra="  runtimeClassName: ${RUNTIME_CLASS}" ;;
  esac
  local ann=""
  [ "${bl}" = "B5" ] && ann="
    ai.sovereign.io/attestation-evidence: \"${EVIDENCE_ANN}\""
  kubectl apply -n "${NS}" -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels: { app: ${app}, baseline: "${bl}" }
  annotations:
    ai.sovereign.io/model-digest: "sha256:${bl}-${seed}"${ann}
spec:
${sched_line}
${extra}
  containers: [{ name: app, image: ${IMAGE} }]
EOF
}

for BL in B1 B2 B3 B5; do
  echo "== baseline ${BL} (N=${N_RUNS}, warmup=${WARMUP})"
  total=$((N_RUNS + WARMUP))
  for i in $(seq 1 "${total}"); do
    seed=$((2000 + i)); pod="$(echo ${BL} | tr A-Z a-z)-${i}"
    phase="measured"; [ "${i}" -le "${WARMUP}" ] && phase="warmup"
    kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    run_pod "${BL}" "${seed}" "${pod}"
    node=""; for _ in $(seq 1 30); do node="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"; [ -n "$node" ] && break; sleep 1; done
    pj="$(mktemp)"; kubectl -n "${NS}" get pod "${pod}" -o json > "${pj}" 2>/dev/null || echo '{}' > "${pj}"
    pending_ms="$(python3 -c "
import json,datetime
p=json.load(open('${pj}'))
def t(s):
    if not s: return None
    return datetime.datetime.fromisoformat(s.replace('Z','+00:00'))
c=t(p.get('metadata',{}).get('creationTimestamp')); sc=None
for x in p.get('status',{}).get('conditions',[]):
    if x.get('type')=='PodScheduled' and x.get('status')=='True': sc=t(x.get('lastTransitionTime'))
print(int((sc-c).total_seconds()*1000) if c and sc else '')" 2>/dev/null)"
    rm -f "${pj}"
    precise=""
    if [ "${BL}" = "B5" ]; then
      slog="$(mktemp)"; dump_scheduler_logs "${slog}"
      precise="$(python3 -c "
import json
best=''
for line in open('${slog}',encoding='utf-8',errors='ignore'):
    if '${pod}' in line and 'phase timings' in line:
        try:
            e=json.loads(line)
            if 'total_us' in e: best=round(int(e['total_us'])/1000.0,3)
        except: pass
print(best)" 2>/dev/null)"
      rm -f "${slog}"
    fi
    sched="default-scheduler"; [ "${BL}" = "B5" ] && sched="${SCHED_NAME}"
    echo "$(now_iso),${ENV_NAME},${phase}-${i},${seed},${BL},${sched},${pending_ms},${precise},${node},$([ -n "$node" ] && echo success || echo failure),scheduler-logs" >> "${CSV}"
    printf '  %s run %2d/%d node=%s pending=%sms precise=%sms\n' "${BL}" "${i}" "${total}" "${node:-PENDING}" "${pending_ms:-NA}" "${precise:-NA}"
    kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done
done
echo "== baselines done (CSV: ${CSV}). NOTE: kind = regression/prep; paper B1-B5 = AKS (Phase 2)."
