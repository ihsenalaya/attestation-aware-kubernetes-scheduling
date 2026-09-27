#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# Multi-node AKS qualification for Article 1.
#
# The script runs on real AKS SEV-SNP nodes only. It validates that governed pods
# can be placed on multiple real confidential nodes, and exercises rejection when
# the selected node evidence is temporarily revoked, expired, or absent. Evidence
# status is backed up and restored before exit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source experiments/harness/lib.sh

ENV_NAME="${ENV_NAME:-aks-real-sevsnp}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
NS="${NS:-article1-multinode}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
TABLE_DIR="${TABLE_DIR:-results/tables}"
CSV="${CSV:-${OUT_DIR}/multinode_node_selection.csv}"
TABLE_CSV="${TABLE_CSV:-${TABLE_DIR}/multinode_node_selection.csv}"
RAW_DIR="${OUT_DIR}/multinode-node-selection"
VERIFY_BIN="${VERIFY_BIN:-${REPO_ROOT}/dist/verify-placement}"
CENTRAL_VERIFIER_DEPLOY="${CENTRAL_VERIFIER_DEPLOY:-central-verifier}"
IMAGE="${MULTINODE_IMAGE:-mcr.microsoft.com/oss/busybox/busybox:1.36}"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
MAX_EVIDENCE_AGE_SECONDS="${MAX_EVIDENCE_AGE_SECONDS:-300}"
NEGATIVE_WAIT_SECONDS="${NEGATIVE_WAIT_SECONDS:-25}"
POSITIVE_WAIT_SECONDS="${POSITIVE_WAIT_SECONDS:-180}"

mkdir -p "${OUT_DIR}" "${TABLE_DIR}" "${RAW_DIR}"

header="timestamp,env,run_id,node_name,node_type,evidence_status,tee_type,eligible,score,selected,pod_bound,reason,raw_log_path"
printf '%s\n' "${header}" > "${CSV}"

append_row() {
  ROW_TIMESTAMP="$1" ROW_RUN_ID="$2" ROW_NODE="$3" ROW_NODE_TYPE="$4" ROW_EVIDENCE_STATUS="$5" \
  ROW_TEE="$6" ROW_ELIGIBLE="$7" ROW_SCORE="$8" ROW_SELECTED="$9" ROW_BOUND="${10}" \
  ROW_REASON="${11}" ROW_RAW="${12}" ROW_ENV="${ENV_NAME}" ROW_CSV="${CSV}" python3 - <<'PY'
import csv, os
row = [
    os.environ["ROW_TIMESTAMP"],
    os.environ["ROW_ENV"],
    os.environ["ROW_RUN_ID"],
    os.environ["ROW_NODE"],
    os.environ["ROW_NODE_TYPE"],
    os.environ["ROW_EVIDENCE_STATUS"],
    os.environ["ROW_TEE"],
    os.environ["ROW_ELIGIBLE"],
    os.environ["ROW_SCORE"],
    os.environ["ROW_SELECTED"],
    os.environ["ROW_BOUND"],
    os.environ["ROW_REASON"],
    os.environ["ROW_RAW"],
]
with open(os.environ["ROW_CSV"], "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerow(row)
PY
}

decision_name() {
  local pod="$1"
  local decision="${pod}-${NS}"
  printf '%s\n' "${decision:0:63}"
}

status_file_for() {
  local ns="$1" name="$2"
  printf '%s/%s__%s.status.json\n' "${RAW_DIR}" "${ns}" "${name}"
}

patched_ns=()
patched_name=()
patched_file=()
central_replicas=""

restore_evidence_statuses() {
  local i ns name status_file patch
  for i in "${!patched_name[@]}"; do
    ns="${patched_ns[$i]}"
    name="${patched_name[$i]}"
    status_file="${patched_file[$i]}"
    if [ -s "${status_file}" ]; then
      patch="$(python3 - "${status_file}" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    status = json.load(f)
status.setdefault("revoked", False)
print(json.dumps({"status": status}, separators=(",", ":")))
PY
)"
      kubectl -n "${ns}" patch attestationevidence "${name}" \
        --subresource=status --type=merge -p "${patch}" >/dev/null 2>&1 || true
    fi
  done
}

restore_central_verifier() {
  if [ -n "${central_replicas}" ]; then
    kubectl -n "${PLATFORM_NS}" scale deployment "${CENTRAL_VERIFIER_DEPLOY}" \
      --replicas="${central_replicas}" >/dev/null 2>&1 || true
    if [ "${central_replicas}" != "0" ]; then
      kubectl -n "${PLATFORM_NS}" rollout status "deployment/${CENTRAL_VERIFIER_DEPLOY}" \
        --timeout=180s >/dev/null 2>&1 || true
    fi
  fi
}

cleanup() {
  set +e
  restore_evidence_statuses
  restore_central_verifier
}
trap cleanup EXIT

score_from_status() {
  local status_file="$1"
  python3 - "${status_file}" "${MAX_EVIDENCE_AGE_SECONDS}" <<'PY'
import datetime as dt, json, sys
path, max_age_raw = sys.argv[1:3]
max_age = int(max_age_raw)
with open(path, encoding="utf-8") as f:
    status = json.load(f)
score = 100
last = status.get("lastVerifiedTime")
if last and max_age > 0:
    try:
        ts = dt.datetime.fromisoformat(last.replace("Z", "+00:00"))
        now = dt.datetime.now(dt.timezone.utc)
        age = max(0.0, (now - ts).total_seconds())
        freshness = max(0.0, 1.0 - age / max_age)
        score += int(freshness * 50)
    except Exception:
        pass
if status.get("evidenceMode") == "real":
    score += 20
score += 5
print(score)
PY
}

backup_status() {
  local ns="$1" name="$2" status_file
  status_file="$(status_file_for "${ns}" "${name}")"
  kubectl -n "${ns}" get attestationevidence "${name}" -o json \
    | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin).get("status", {})))' > "${status_file}"
  patched_ns+=("${ns}")
  patched_name+=("${name}")
  patched_file+=("${status_file}")
  printf '%s\n' "${status_file}"
}

scale_central_verifier_down() {
  central_replicas="$(kubectl -n "${PLATFORM_NS}" get deployment "${CENTRAL_VERIFIER_DEPLOY}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [ -n "${central_replicas}" ] || central_replicas="1"
  echo "== [multinode] pausing ${CENTRAL_VERIFIER_DEPLOY} replicas=${central_replicas}"
  kubectl -n "${PLATFORM_NS}" scale deployment "${CENTRAL_VERIFIER_DEPLOY}" --replicas=0 >/dev/null
  for _ in $(seq 1 90); do
    ready="$(kubectl -n "${PLATFORM_NS}" get deployment "${CENTRAL_VERIFIER_DEPLOY}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [ -z "${ready}" ] || [ "${ready}" = "0" ] && return 0
    sleep 1
  done
  return 1
}

apply_policy() {
  ensure_ns "${NS}"
  kubectl apply -f - >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata:
  name: multinode-policy
  namespace: ${NS}
spec:
  target:
    namespaceSelector:
      matchLabels:
        ai.sovereign.io/sensitivity: high
    workloadSelector:
      matchLabels:
        app: bench
  requiredTEE: ["${REQUIRED_TEE}"]
  requireConfidentialContainers: false
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: ${MAX_EVIDENCE_AGE_SECONDS}
  requireModelDigest: true
  enforcementMode: enforce
EOF
}

patch_revoked() {
  local ns="$1" name="$2"
  backup_status "${ns}" "${name}" >/dev/null
  kubectl -n "${ns}" patch attestationevidence "${name}" \
    --subresource=status --type=merge \
    -p '{"status":{"revoked":true,"verificationStatus":"Failed"}}' >/dev/null
}

patch_expired() {
  local ns="$1" name="$2" old_ts old_exp
  backup_status "${ns}" "${name}" >/dev/null
  old_ts="$(python3 - <<'PY'
import datetime as dt
print((dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"
  old_exp="$(python3 - <<'PY'
import datetime as dt
print((dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"
  kubectl -n "${ns}" patch attestationevidence "${name}" \
    --subresource=status --type=merge \
    -p "{\"status\":{\"revoked\":false,\"verified\":true,\"verificationStatus\":\"Verified\",\"lastVerifiedTime\":\"${old_ts}\",\"expiresAt\":\"${old_exp}\"}}" >/dev/null
}

verify_decision_token() {
  local pod="$1" raw="$2" decision token pub token_file
  decision="$(decision_name "${pod}")"
  token="$(kubectl -n "${NS}" get aiplacementdecision "${decision}" -o jsonpath='{.metadata.annotations.ai\.sovereign\.io/placement-token}' 2>/dev/null || true)"
  pub="$(scheduler_pubkey)"
  if [ -z "${token}" ] || [ -z "${pub}" ]; then
    printf 'NO_TOKEN\n'
    return 1
  fi
  token_file="${RAW_DIR}/${pod}-placement-token.json"
  printf '%s\n' "${token}" > "${token_file}"
  if "${VERIFY_BIN}" -token-file "${token_file}" -pubkey-hex "${pub}" >> "${raw}" 2>&1; then
    printf 'TOKEN_PASS\n'
    return 0
  fi
  printf 'TOKEN_FAIL\n'
  return 1
}

submit_pod() {
  local pod="$1" node="$2" raw="$3" evidence_ann="$4"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl apply -f - > "${raw}" 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels:
    app: bench
    ai.sovereign.io/experiment: multinode
  annotations:
    ai.sovereign.io/model-digest: "sha256:multinode-node-selection"
    ai.sovereign.io/attestation-evidence: "${evidence_ann}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: "${node}"
  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - name: app
      image: ${IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["sh", "-c", "echo multinode-ok"]
EOF
}

wait_for_node_binding() {
  local pod="$1" timeout="$2" node
  for _ in $(seq 1 "${timeout}"); do
    node="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    if [ -n "${node}" ]; then
      printf '%s\n' "${node}"
      return 0
    fi
    sleep 1
  done
  printf '\n'
  return 1
}

run_case() {
  local run_id="$1" node="$2" node_type="$3" evidence_status="$4" tee="$5" eligible="$6" score="$7" expected_bound="$8" expected_reason="$9" evidence_ann="${10}"
  local pod raw bound_node decision_status token_status pod_bound selected reason
  pod="mn-${run_id}"
  raw="${RAW_DIR}/${pod}.log"
  echo "== [multinode] ${run_id}: node=${node} evidence=${evidence_status}"
  submit_pod "${pod}" "${node}" "${raw}" "${evidence_ann}"
  if [ "${expected_bound}" = "true" ]; then
    bound_node="$(wait_for_node_binding "${pod}" "${POSITIVE_WAIT_SECONDS}" || true)"
  else
    bound_node="$(wait_for_node_binding "${pod}" "${NEGATIVE_WAIT_SECONDS}" || true)"
  fi
  kubectl -n "${NS}" get pod "${pod}" -o yaml >> "${raw}" 2>&1 || true
  kubectl -n "${NS}" describe pod "${pod}" >> "${raw}" 2>&1 || true
  dump_scheduler_logs "${RAW_DIR}/${pod}-scheduler.log"
  cat "${RAW_DIR}/${pod}-scheduler.log" >> "${raw}" 2>/dev/null || true

  if [ -n "${bound_node}" ]; then
    pod_bound="true"
  else
    pod_bound="false"
  fi
  if [ "${bound_node}" = "${node}" ]; then
    selected="true"
  else
    selected="false"
  fi
  decision_status="$(kubectl -n "${NS}" get aiplacementdecision "$(decision_name "${pod}")" -o jsonpath='{.status.decision}' 2>/dev/null || true)"
  if [ "${expected_bound}" = "true" ]; then
    token_status="$(verify_decision_token "${pod}" "${raw}" || true)"
    if [ "${pod_bound}" = "true" ] && [ "${selected}" = "true" ] && [ "${decision_status}" = "allow" ] && [ "${token_status}" = "TOKEN_PASS" ]; then
      reason="${expected_reason};decision=allow;${token_status}"
    else
      reason="UNEXPECTED_RESULT;bound=${pod_bound};selected=${selected};decision=${decision_status:-missing};token=${token_status:-missing}"
    fi
  else
    if [ "${pod_bound}" = "false" ]; then
      reason="${expected_reason};decision=${decision_status:-missing}"
    else
      reason="UNEXPECTED_BOUND;bound_node=${bound_node};decision=${decision_status:-missing}"
    fi
  fi
  append_row "$(now_iso)" "${run_id}" "${node}" "${node_type}" "${evidence_status}" "${tee}" \
    "${eligible}" "${score}" "${selected}" "${pod_bound}" "${reason}" "${raw}"
  printf '  %-12s node=%-34s bound=%-5s selected=%-5s reason=%s\n' \
    "${run_id}" "${node}" "${pod_bound}" "${selected}" "${reason}"
}

if [[ "${ENV_NAME}" != aks* ]]; then
  append_row "$(now_iso)" "not-executed" "" "" "NOT_EXECUTED" "" "false" "" "false" "false" "requires_aks_real_sevsnp" ""
  cp "${CSV}" "${TABLE_CSV}"
  exit 1
fi

echo "== [multinode] collecting AKS nodes and real evidence"
kubectl get nodes -o json > "${RAW_DIR}/nodes.json"
kubectl get attestationevidences -A -o json > "${RAW_DIR}/evidence-initial.json"
python3 - "${RAW_DIR}/nodes.json" "${RAW_DIR}/evidence-initial.json" "${REQUIRED_TEE}" "${MAX_EVIDENCE_AGE_SECONDS}" > "${RAW_DIR}/candidates.tsv" <<'PY'
import datetime as dt, json, sys
nodes_path, evidence_path, required_tee, max_age_raw = sys.argv[1:5]
required_tee = required_tee.upper()
max_age = int(max_age_raw)
now = dt.datetime.now(dt.timezone.utc)
nodes = json.load(open(nodes_path, encoding="utf-8")).get("items", [])
evidence = json.load(open(evidence_path, encoding="utf-8")).get("items", [])
by_node = {}
for item in evidence:
    spec = item.get("spec", {})
    status = item.get("status", {})
    node = spec.get("subjectRef", {}).get("name", "")
    if not node or str(spec.get("tee", "")).upper() != required_tee:
        continue
    if status.get("evidenceMode") != "real" or status.get("revoked") or not status.get("verified"):
        continue
    last = status.get("lastVerifiedTime")
    if not last:
        continue
    try:
        ts = dt.datetime.fromisoformat(last.replace("Z", "+00:00"))
    except ValueError:
        continue
    age = max(0.0, (now - ts).total_seconds())
    score = 100 + int(max(0.0, 1.0 - age / max_age) * 50) + 20 + 5
    prev = by_node.get(node)
    if prev is None or ts > prev["ts"]:
        by_node[node] = {
            "ts": ts,
            "namespace": item["metadata"]["namespace"],
            "name": item["metadata"]["name"],
            "score": score,
        }
for node in nodes:
    meta = node.get("metadata", {})
    name = meta.get("name", "")
    labels = meta.get("labels", {})
    if name in by_node and labels.get("kubernetes.azure.com/security-type") == "ConfidentialVM":
        ev = by_node[name]
        print("\t".join(["conf", name, ev["namespace"], ev["name"], str(ev["score"])]))
for node in nodes:
    meta = node.get("metadata", {})
    name = meta.get("name", "")
    labels = meta.get("labels", {})
    if name not in by_node and labels.get("kubernetes.azure.com/security-type") != "ConfidentialVM":
        print("\t".join(["system", name, "", "", "0"]))
PY

mapfile -t conf_lines < <(awk -F'\t' '$1=="conf"{print}' "${RAW_DIR}/candidates.tsv")
mapfile -t system_lines < <(awk -F'\t' '$1=="system"{print}' "${RAW_DIR}/candidates.tsv")

if [ "${#conf_lines[@]}" -lt 3 ] || [ "${#system_lines[@]}" -lt 1 ]; then
  append_row "$(now_iso)" "not-executed" "" "" "NOT_EXECUTED" "${REQUIRED_TEE}" "false" "" "false" "false" "need_at_least_3_confidential_nodes_and_1_system_node" "${RAW_DIR}/candidates.tsv"
  cp "${CSV}" "${TABLE_CSV}"
  echo "FAIL: not enough nodes for multinode experiment" >&2
  exit 1
fi

IFS=$'\t' read -r _ valid_node valid_ev_ns valid_ev_name valid_score <<< "${conf_lines[0]}"
IFS=$'\t' read -r _ revoked_node revoked_ev_ns revoked_ev_name _ <<< "${conf_lines[1]}"
IFS=$'\t' read -r _ expired_node expired_ev_ns expired_ev_name _ <<< "${conf_lines[2]}"
IFS=$'\t' read -r _ system_node _ _ _ <<< "${system_lines[0]}"

apply_policy
scale_central_verifier_down
patch_revoked "${revoked_ev_ns}" "${revoked_ev_name}"
patch_expired "${expired_ev_ns}" "${expired_ev_name}"

run_case "valid" "${valid_node}" "confidential" "fresh_verified_real" "${REQUIRED_TEE}" "true" "${valid_score}" "true" "fresh_verified_real_evidence" "${valid_ev_name}"
run_case "revoked" "${revoked_node}" "confidential" "revoked_real" "${REQUIRED_TEE}" "false" "0" "false" "revoked_evidence_rejected" "${revoked_ev_name}"
run_case "expired" "${expired_node}" "confidential" "expired_real" "${REQUIRED_TEE}" "false" "0" "false" "expired_evidence_rejected" "${expired_ev_name}"
run_case "noevidence" "${system_node}" "non_confidential_system" "no_evidence" "none" "false" "0" "false" "labels_or_nodeSelector_without_evidence_rejected" "missing-evidence"

restore_evidence_statuses
restore_central_verifier
trap - EXIT

kubectl get attestationevidences -A -o json > "${RAW_DIR}/evidence-restored.json"
cp "${CSV}" "${TABLE_CSV}"

failures="$(tail -n +2 "${CSV}" | awk -F, '($2!="aks-real-sevsnp"){next} ($10=="true" && $11!="true"){c++} ($10=="false" && $11!="false"){c++} /UNEXPECTED/{c++} END{print c+0}')"
echo "== [multinode] wrote ${CSV} and ${TABLE_CSV}; failures=${failures}"
if [ "${failures}" -ne 0 ]; then
  exit 1
fi
