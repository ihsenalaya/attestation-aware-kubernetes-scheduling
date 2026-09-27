#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# Run minimal real AI workloads from ConfidentialInferencePolicy-governed pods on
# AKS. Secrets are mounted from Kubernetes Secret objects and are never printed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
source experiments/harness/lib.sh

ENV_NAME="${ENV_NAME:-aks-real-sevsnp}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
NS="${NS:-article1-ai-workloads}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
TABLE_DIR="${TABLE_DIR:-results/tables}"
CSV="${CSV:-${OUT_DIR}/ai_workloads.csv}"
TABLE_CSV="${TABLE_CSV:-${TABLE_DIR}/ai_workloads.csv}"
OPENAI_KEY_FILE="${OPENAI_KEY_FILE:-ai_key.txt}"
VERIFY_BIN="${VERIFY_BIN:-${REPO_ROOT}/dist/verify-placement}"
IMAGE="${AI_WORKLOAD_IMAGE:-python:3.11-slim}"
REQUEST_COUNT="${REQUEST_COUNT:-3}"
CHAT_MODEL="${OPENAI_CHAT_MODEL:-gpt-5.5,gpt-5.4,gpt-4.1-mini,gpt-4o-mini}"
EMBED_MODEL="${OPENAI_EMBED_MODEL:-text-embedding-3-small}"
LOCAL_MODEL_NAME="${LOCAL_MODEL_NAME:-local-linear-risk-v1}"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"

mkdir -p "${OUT_DIR}" "${TABLE_DIR}" "${OUT_DIR}/ai-workloads"

header="timestamp,env,run_id,workload_id,workload_type,model_name,model_digest,image_digest,namespace,pod,placement_decision,verify_result,request_count,success_count,latency_ms_median,status,raw_log_path"
printf '%s\n' "${header}" > "${CSV}"

append_row() {
  ROW_TIMESTAMP="$1" ROW_RUN_ID="$2" ROW_WORKLOAD_ID="$3" ROW_WORKLOAD_TYPE="$4" \
  ROW_MODEL_NAME="$5" ROW_MODEL_DIGEST="$6" ROW_IMAGE_DIGEST="$7" ROW_NAMESPACE="$8" \
  ROW_POD="$9" ROW_PLACEMENT="${10}" ROW_VERIFY="${11}" ROW_REQ="${12}" ROW_SUCCESS="${13}" \
  ROW_LAT="${14}" ROW_STATUS="${15}" ROW_RAW="${16}" ROW_ENV="${ENV_NAME}" ROW_CSV="${CSV}" \
  python3 - <<'PY'
import csv, os
row = [
    os.environ["ROW_TIMESTAMP"],
    os.environ["ROW_ENV"],
    os.environ["ROW_RUN_ID"],
    os.environ["ROW_WORKLOAD_ID"],
    os.environ["ROW_WORKLOAD_TYPE"],
    os.environ["ROW_MODEL_NAME"],
    os.environ["ROW_MODEL_DIGEST"],
    os.environ["ROW_IMAGE_DIGEST"],
    os.environ["ROW_NAMESPACE"],
    os.environ["ROW_POD"],
    os.environ["ROW_PLACEMENT"],
    os.environ["ROW_VERIFY"],
    os.environ["ROW_REQ"],
    os.environ["ROW_SUCCESS"],
    os.environ["ROW_LAT"],
    os.environ["ROW_STATUS"],
    os.environ["ROW_RAW"],
]
with open(os.environ["ROW_CSV"], "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerow(row)
PY
}

sha256_text() {
  printf '%s' "$1" | sha256sum | awk '{print "sha256:" $1}'
}

wait_for_pod_done() {
  local pod="$1" timeout="${2:-240}"
  local phase=""
  for _ in $(seq 1 "${timeout}"); do
    phase="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.status.phase}' 2>/dev/null)"
    case "${phase}" in
      Succeeded|Failed) printf '%s\n' "${phase}"; return 0 ;;
    esac
    sleep 1
  done
  printf 'Timeout\n'
  return 1
}

placement_decision_name() {
  local pod="$1"
  local decision="${pod}-${NS}"
  printf '%s\n' "${decision:0:63}"
}

verify_decision_token() {
  local pod="$1" raw="$2"
  local decision token pub token_file
  decision="$(placement_decision_name "${pod}")"
  for _ in $(seq 1 60); do
    if kubectl -n "${NS}" get aiplacementdecision "${decision}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  token="$(kubectl -n "${NS}" get aiplacementdecision "${decision}" -o jsonpath='{.metadata.annotations.ai\.sovereign\.io/placement-token}' 2>/dev/null)"
  pub="$(scheduler_pubkey)"
  if [ -z "${token}" ] || [ -z "${pub}" ]; then
    printf 'FAIL\n'
    return 1
  fi
  token_file="${OUT_DIR}/ai-workloads/${pod}-placement-token.json"
  printf '%s\n' "${token}" > "${token_file}"
  if "${VERIFY_BIN}" -token-file "${token_file}" -pubkey-hex "${pub}" >> "${raw}" 2>&1; then
    printf 'PASS\n'
    return 0
  fi
  printf 'FAIL\n'
  return 1
}

extract_result_field() {
  local raw="$1" field="$2"
  python3 - "$raw" "$field" <<'PY'
import json, sys
path, field = sys.argv[1:3]
result = {}
for line in open(path, encoding="utf-8", errors="replace"):
    if line.startswith("AI_WORKLOAD_RESULT "):
        result = json.loads(line.split(" ", 1)[1])
if not result:
    raise SystemExit(1)
print(result.get(field, ""))
PY
}

echo "== [ai-workloads] preparing namespace ${NS}"
kubectl get namespace "${NS}" >/dev/null 2>&1
if [ $? -ne 0 ]; then
  kubectl create namespace "${NS}" >/dev/null
fi
kubectl label namespace "${NS}" ai.sovereign.io/sensitivity=high --overwrite >/dev/null

echo "== [ai-workloads] applying policy"
apply_bench_policy "${NS}" "${REQUIRED_TEE}"

echo "== [ai-workloads] applying OpenAI secret without printing key"
if [ -s "${OPENAI_KEY_FILE}" ]; then
  kubectl -n "${NS}" create secret generic openai-api \
    --from-file=OPENAI_API_KEY="${OPENAI_KEY_FILE}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
else
  echo "FAIL: ${OPENAI_KEY_FILE} missing or empty" >&2
  append_row "$(now_iso)" "prereq" "openai-secret" "secret" "n/a" "" "" "${NS}" "" "" "" 0 0 "" "FAILED" "${OPENAI_KEY_FILE}"
  exit 2
fi

local_model_json='{"name":"local-linear-risk-v1","classes":["deny","allow"],"features":["risk_score","attested"],"weights":[-1.75,2.25],"bias":-0.2,"threshold":0.0,"samples":[[0.9,0],[0.1,1],[0.4,1],[0.8,1],[0.2,0]]}'
local_model_digest="$(sha256_text "${local_model_json}")"
kubectl -n "${NS}" create configmap ai-local-model \
  --from-literal=model.json="${local_model_json}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

runner_py="$(mktemp)"
cat > "${runner_py}" <<'PY'
import json, os, statistics, time, urllib.error, urllib.request

def percentile(xs):
    return statistics.median(xs) if xs else ""

def post_json(path, payload, key):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        "https://api.openai.com/v1/" + path,
        data=body,
        headers={
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())

workload_id = os.environ["WORKLOAD_ID"]
workload_type = os.environ["WORKLOAD_TYPE"]
model_candidates = [m.strip() for m in os.environ["MODEL_NAME"].split(",") if m.strip()]
model = model_candidates[0] if model_candidates else ""
request_count = int(os.environ.get("REQUEST_COUNT", "3"))
latencies = []
success = 0
error_type = ""

try:
    if workload_type.startswith("openai-"):
        key_file = os.environ.get("OPENAI_KEY_FILE", "/var/run/secrets/openai/OPENAI_API_KEY")
        key = open(key_file, encoding="utf-8").read().strip()
        if not key:
            raise RuntimeError("missing_api_key")
        last_error = ""
        for candidate in model_candidates:
            trial_latencies = []
            trial_success = 0
            try:
                for i in range(request_count):
                    t0 = time.perf_counter()
                    if workload_type == "openai-chat":
                        data = post_json("responses", {
                            "model": candidate,
                            "input": "Return exactly the word PASS for an Article 1 AI workload smoke test."
                        }, key)
                        ok = bool(data.get("id"))
                    elif workload_type == "openai-embedding":
                        data = post_json("embeddings", {
                            "model": candidate,
                            "input": "Article 1 attestation-aware scheduling AI workload smoke test."
                        }, key)
                        ok = bool(data.get("data"))
                    else:
                        raise RuntimeError("unknown_openai_workload")
                    trial_latencies.append((time.perf_counter() - t0) * 1000.0)
                    trial_success += 1 if ok else 0
                model = candidate
                latencies = trial_latencies
                success = trial_success
                break
            except urllib.error.HTTPError as exc:
                last_error = f"HTTPError_{exc.code}"
                if exc.code in (400, 404):
                    continue
                raise
            except Exception as exc:
                last_error = type(exc).__name__
                continue
        if success != request_count:
            error_type = last_error or "all_model_candidates_failed"
    elif workload_type == "local-cpu":
        model_doc = json.load(open("/models/model.json", encoding="utf-8"))
        weights = model_doc["weights"]
        bias = model_doc["bias"]
        threshold = model_doc["threshold"]
        samples = model_doc["samples"]
        request_count = len(samples)
        for sample in samples:
            t0 = time.perf_counter()
            score = bias + sum(w * x for w, x in zip(weights, sample))
            _prediction = "allow" if score >= threshold else "deny"
            latencies.append((time.perf_counter() - t0) * 1000.0)
            success += 1
    else:
        raise RuntimeError("unknown_workload_type")
except urllib.error.HTTPError as exc:
    error_type = f"HTTPError_{exc.code}"
except Exception as exc:
    error_type = type(exc).__name__

status = "PASS" if success == request_count and request_count > 0 else "FAIL"
print("AI_WORKLOAD_RESULT " + json.dumps({
    "workload_id": workload_id,
    "workload_type": workload_type,
    "model_name": model,
    "request_count": request_count,
    "success_count": success,
    "latency_ms_median": round(float(percentile(latencies)), 3) if latencies else "",
    "status": status,
    "error_type": error_type,
}, sort_keys=True))
raise SystemExit(0 if status == "PASS" else 1)
PY

kubectl -n "${NS}" create configmap ai-workload-runner \
  --from-file=runner.py="${runner_py}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
rm -f "${runner_py}"

EVIDENCE_ANN="${EVIDENCE_ANN:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null)}"
if [ -z "${EVIDENCE_ANN}" ]; then
  echo "FAIL: no verified ${EXPECTED_EVIDENCE_MODE} ${REQUIRED_TEE} evidence found" >&2
  append_row "$(now_iso)" "prereq" "verified-evidence" "attestation" "n/a" "" "" "${NS}" "" "" "" 0 0 "" "FAILED" "${OUT_DIR}/attestation-real-summary.json"
  exit 2
fi

tolerations_yaml=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  tolerations_yaml='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi

run_workload() {
  local run_id="$1" workload_id="$2" workload_type="$3" model_name="$4" model_digest="$5"
  local pod="ai-${workload_id}"
  local raw="${OUT_DIR}/ai-workloads/${pod}.log"
  local ts phase image_digest placement verify_result req success latency workload_status decision actual_model
  ts="$(now_iso)"
  rm -f "${raw}"
  kubectl -n "${NS}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NS}
  labels:
    app: bench
    ai.sovereign.io/workload-id: ${workload_id}
  annotations:
    ai.sovereign.io/model-digest: "${model_digest}"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE_ANN}"
spec:
  schedulerName: ${SCHED_NAME}
  runtimeClassName: ${RUNTIME_CLASS}
${tolerations_yaml}
  restartPolicy: Never
  containers:
    - name: app
      image: ${IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["python", "/workload/runner.py"]
      env:
        - name: WORKLOAD_ID
          value: "${workload_id}"
        - name: WORKLOAD_TYPE
          value: "${workload_type}"
        - name: MODEL_NAME
          value: "${model_name}"
        - name: REQUEST_COUNT
          value: "${REQUEST_COUNT}"
        - name: OPENAI_KEY_FILE
          value: "/var/run/secrets/openai/OPENAI_API_KEY"
      volumeMounts:
        - name: runner
          mountPath: /workload
          readOnly: true
        - name: openai
          mountPath: /var/run/secrets/openai
          readOnly: true
        - name: local-model
          mountPath: /models
          readOnly: true
  volumes:
    - name: runner
      configMap:
        name: ai-workload-runner
    - name: openai
      secret:
        secretName: openai-api
    - name: local-model
      configMap:
        name: ai-local-model
EOF

  phase="$(wait_for_pod_done "${pod}" 300)"
  kubectl -n "${NS}" logs "${pod}" > "${raw}" 2>&1
  kubectl -n "${NS}" get pod "${pod}" -o yaml > "${OUT_DIR}/ai-workloads/${pod}.yaml" 2>>"${raw}"
  decision="$(placement_decision_name "${pod}")"
  placement="$(kubectl -n "${NS}" get aiplacementdecision "${decision}" -o jsonpath='{.status.decision}' 2>/dev/null)"
  [ -n "${placement}" ] || placement="missing"
  image_digest="$(kubectl -n "${NS}" get pod "${pod}" -o jsonpath='{.status.containerStatuses[0].imageID}' 2>/dev/null)"
  verify_result="$(verify_decision_token "${pod}" "${raw}")"

  req="$(extract_result_field "${raw}" request_count 2>/dev/null)"
  success="$(extract_result_field "${raw}" success_count 2>/dev/null)"
  latency="$(extract_result_field "${raw}" latency_ms_median 2>/dev/null)"
  workload_status="$(extract_result_field "${raw}" status 2>/dev/null)"
  actual_model="$(extract_result_field "${raw}" model_name 2>/dev/null)"
  if [ -n "${actual_model}" ]; then
    model_name="${actual_model}"
    if [ "${workload_type}" != "local-cpu" ]; then
      model_digest="$(sha256_text "${workload_type}:${model_name}:observed-v1")"
    fi
  fi
  [ -n "${req}" ] || req="0"
  [ -n "${success}" ] || success="0"
  [ -n "${workload_status}" ] || workload_status="FAIL"

  if [ "${phase}" != "Succeeded" ]; then
    workload_status="FAILED"
  fi
  if [ "${workload_status}" = "PASS" ] && [ "${verify_result}" = "PASS" ] && [ "${placement}" = "allow" ]; then
    status="PASS"
  else
    status="FAILED"
  fi
  append_row "${ts}" "${run_id}" "${workload_id}" "${workload_type}" "${model_name}" \
    "${model_digest}" "${image_digest}" "${NS}" "${pod}" "${placement}" "${verify_result}" \
    "${req}" "${success}" "${latency}" "${status}" "${raw}"
  printf '  %-24s workload=%-16s phase=%-9s placement=%-7s verify=%-4s status=%s\n' \
    "${pod}" "${workload_type}" "${phase}" "${placement}" "${verify_result}" "${status}"
}

echo "== [ai-workloads] running governed AI pods"
failures=0
run_workload "run-1" "openai-chat-minimal" "openai-chat" "${CHAT_MODEL}" "$(sha256_text "openai-chat:${CHAT_MODEL}:prompt-v1")"
run_workload "run-2" "openai-embedding-minimal" "openai-embedding" "${EMBED_MODEL}" "$(sha256_text "openai-embedding:${EMBED_MODEL}:input-v1")"
run_workload "run-3" "local-cpu-minimal" "local-cpu" "${LOCAL_MODEL_NAME}" "${local_model_digest}"

cp "${CSV}" "${TABLE_CSV}"
failures="$(tail -n +2 "${CSV}" | awk -F, '$16!="PASS"{c++} END{print c+0}')"
echo "== [ai-workloads] wrote ${CSV} and ${TABLE_CSV}; failures=${failures}"
if [ "${failures}" -ne 0 ]; then
  exit 1
fi
