#!/usr/bin/env bash
# lib.sh — shared helpers for the article-1 experiment harnesses.
# Sourced by measure_*.sh. Env-parameterised so the SAME harness runs on kind and
# on AKS: set ENV_NAME (kind-live-simulated | aks-real-sevsnp), PLATFORM_NS,
# NODE (target node), and (for AKS) KUBECONFIG.
set -uo pipefail

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
SCHED_DEPLOY="${SCHED_DEPLOY:-attestation-scheduler}"
SCHED_NAME="${SCHED_NAME:-ai-attestation-scheduler}"

now_iso() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

# ms between two RFC3339/K8s timestamps ($1 start, $2 end), or empty on error.
delta_ms() {
  python3 - "$1" "$2" <<'PY' 2>/dev/null
import sys,datetime
def p(s):
    s=s.strip().replace('Z','+00:00')
    # tolerate fractional seconds
    try: return datetime.datetime.fromisoformat(s)
    except Exception:
        return datetime.datetime.strptime(s.split('+')[0],'%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc)
a=p(sys.argv[1]); b=p(sys.argv[2])
print(int((b-a).total_seconds()*1000))
PY
}

# Namespaced ensure with sensitivity label.
ensure_ns() { # ns
  kubectl get ns "$1" >/dev/null 2>&1 || kubectl create ns "$1" >/dev/null
  kubectl label ns "$1" ai.sovereign.io/sensitivity=high --overwrite >/dev/null
}

# Apply a ConfidentialInferencePolicy that matches app=bench pods.
apply_bench_policy() { # ns requiredTEE
  local runtime_class="${BENCH_RUNTIME_CLASS:-$(default_runtime_class)}"
  local require_cc="${REQUIRE_CONFIDENTIAL_CONTAINERS:-$(default_require_confidential_containers)}"
  kubectl apply -f - >/dev/null <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata: { name: bench-policy, namespace: $1 }
spec:
  target:
    namespaceSelector: { matchLabels: { ai.sovereign.io/sensitivity: high } }
    workloadSelector: { matchLabels: { app: bench } }
  requiredTEE: ["${2:-TDX}"]
  requireConfidentialContainers: ${require_cc}
  allowedRuntimeClasses: ["${runtime_class}"]
  maxEvidenceAgeSeconds: 300
  requireModelDigest: true
  enforcementMode: enforce
EOF
}

# Dump scheduler logs since a marker for later parsing.
dump_scheduler_logs() { # outfile
  kubectl logs -n "${PLATFORM_NS}" "deploy/${SCHED_DEPLOY}" --tail=100000 > "$1" 2>/dev/null || true
}

# Scheduler public key (hex) from logs, for verify-placement.
scheduler_pubkey() {
  kubectl logs -n "${PLATFORM_NS}" "deploy/${SCHED_DEPLOY}" 2>/dev/null \
    | grep 'signing key ready' | tail -1 | sed -E 's/.*"pubKey":"([0-9a-f]+)".*/\1/'
}

is_aks_env() {
  [[ "${ENV_NAME}" == aks* ]]
}

default_required_tee() {
  if is_aks_env; then
    printf 'SEV-SNP\n'
  else
    printf 'TDX\n'
  fi
}

default_runtime_class() {
  if is_aks_env; then
    printf 'runc\n'
  else
    printf 'simulated-kata-qemu-tdx\n'
  fi
}

default_require_confidential_containers() {
  if is_aks_env; then
    # DCasv6 provides node-level SEV-SNP CVM isolation. AKS Pod Sandboxing
    # requires nested virtualization and is rejected on this SKU.
    printf 'false\n'
  else
    printf 'true\n'
  fi
}

detect_verified_evidence_json() {
  local required_tee="${1:-SEV-SNP}"
  local expected_mode="${2:-}"
  local node_names
  node_names="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  kubectl get attestationevidences -A -o json 2>/dev/null | \
    REQUIRED_TEE="${required_tee}" EXPECTED_MODE="${expected_mode}" NODE_NAMES="${node_names}" python3 -c '
import json, os, sys
required = os.environ.get("REQUIRED_TEE", "").upper()
expected_mode = os.environ.get("EXPECTED_MODE", "")
node_names = {line.strip() for line in os.environ.get("NODE_NAMES", "").splitlines() if line.strip()}
try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)
items = sorted(data.get("items", []), key=lambda i: i.get("metadata", {}).get("creationTimestamp", ""), reverse=True)
for item in items:
    spec = item.get("spec", {})
    status = item.get("status", {})
    if required and str(spec.get("tee", "")).upper() != required:
        continue
    if expected_mode and status.get("evidenceMode") != expected_mode:
        continue
    if not status.get("verified") or status.get("revoked"):
        continue
    node = spec.get("subjectRef", {}).get("name", "")
    name = item.get("metadata", {}).get("name", "")
    namespace = item.get("metadata", {}).get("namespace", "")
    if node_names and node not in node_names:
        continue
    if node and name:
        print(json.dumps({"node": node, "name": name, "namespace": namespace}))
        raise SystemExit(0)
raise SystemExit(1)
'
}

detect_verified_node() {
  detect_verified_evidence_json "${1:-SEV-SNP}" "${2:-}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["node"])'
}

detect_verified_evidence_name() {
  detect_verified_evidence_json "${1:-SEV-SNP}" "${2:-}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])'
}
