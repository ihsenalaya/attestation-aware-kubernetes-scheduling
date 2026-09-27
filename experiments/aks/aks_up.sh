#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# aks_up.sh — bring up the confidential AKS cluster and deploy the platform.
#
# Order (fail-closed):
#   1. preflight.sh  (GO/NO-GO for confidential pool)
#   2. terraform apply (confidential pool only if preflight says GO)
#   3. get kubeconfig (LOCAL file, gitignored — never committed)
#   4. create ghcr-pull + openai secrets (from local creds, never committed)
#   5. apply CRDs, deploy Helm (values-aks-private, GHCR images)
#   6. wait for central-verifier + node-attestation-agent
#
# Secrets are created in-cluster from local files; none is written to Git.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

TF_DIR="automation/terraform/aks-confidential"
RG="${RG:-rg-article1-confidential}"
CLUSTER="${CLUSTER:-aks-article1}"
NS="${NS:-ai-platform}"
KUBECONFIG_FILE="${KUBECONFIG_FILE:-${TF_DIR}/kubeconfig-aks}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
CHART="charts/ai-confidential-governance-platform"
VALUES="${VALUES:-${CHART}/values-aks-private.yaml}"
IMAGE_TAG="${IMAGE_TAG:-0.5.11}"
OPENAI_KEY_FILE="${OPENAI_KEY_FILE:-ai_key.txt}"
mkdir -p "${OUT_DIR}"

docker_config_has_inline_ghcr_auth() {
  local cfg="$1"
  python3 - "$cfg" <<'PY'
import json, sys
try:
    cfg = json.load(open(sys.argv[1]))
except Exception:
    raise SystemExit(1)
auths = cfg.get("auths", {})
entry = auths.get("ghcr.io") or auths.get("https://ghcr.io") or {}
raise SystemExit(0 if entry.get("auth") or entry.get("identitytoken") else 1)
PY
}

docker_config_ghcr_helper() {
  local cfg="$1"
  python3 - "$cfg" <<'PY'
import json, sys
try:
    cfg = json.load(open(sys.argv[1]))
except Exception:
    raise SystemExit(1)
helper = (cfg.get("credHelpers", {}) or {}).get("ghcr.io") or cfg.get("credsStore") or ""
if helper:
    print(helper)
    raise SystemExit(0)
raise SystemExit(1)
PY
}

find_docker_credential_helper() {
  local helper="$1"
  local names=()
  if [[ "${helper}" == docker-credential-* ]]; then
    names+=("${helper}")
  else
    names+=("docker-credential-${helper}" "docker-credential-${helper}.exe")
  fi
  names+=(
    "/mnt/c/Program Files/Docker/Docker/resources/bin/docker-credential-${helper}.exe"
    "/mnt/c/Program Files/Docker/Docker/resources/bin/docker-credential-desktop.exe"
  )
  for candidate in "${names[@]}"; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      command -v "${candidate}"
      return 0
    fi
    if [ -x "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

write_ghcr_dockerconfig_from_helper() {
  local docker_cfg="$1" out="$2"
  local helper helper_bin creds_tmp
  helper="$(docker_config_ghcr_helper "${docker_cfg}")" || return 1
  helper_bin="$(find_docker_credential_helper "${helper}")" || return 1
  creds_tmp="$(mktemp)"
  if ! "${helper_bin}" get <<<'ghcr.io' > "${creds_tmp}"; then
    rm -f "${creds_tmp}"
    return 1
  fi
  python3 - "${creds_tmp}" "${out}" <<'PY'
import base64, json, sys
src, out = sys.argv[1:3]
creds = json.load(open(src))
user = creds.get("Username", "")
secret = creds.get("Secret", "")
server = creds.get("ServerURL") or "ghcr.io"
if not secret:
    raise SystemExit(1)
auth = base64.b64encode(f"{user}:{secret}".encode()).decode()
json.dump({"auths": {server: {"auth": auth}, "ghcr.io": {"auth": auth}}}, open(out, "w"))
PY
  rm -f "${creds_tmp}"
}

create_ghcr_pull_secret() {
  local docker_cfg="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
  local secret_cfg="" tmp_cfg=""
  if [ ! -f "${docker_cfg}" ]; then
    echo "   WARN: no docker config at ${docker_cfg}; create ghcr-pull manually"
    return 0
  fi
  if docker_config_has_inline_ghcr_auth "${docker_cfg}"; then
    secret_cfg="${docker_cfg}"
  else
    tmp_cfg="$(mktemp)"
    if write_ghcr_dockerconfig_from_helper "${docker_cfg}" "${tmp_cfg}"; then
      secret_cfg="${tmp_cfg}"
    else
      rm -f "${tmp_cfg}"
      echo "   WARN: could not extract GHCR credentials from Docker credential helper; create ghcr-pull manually"
      return 0
    fi
  fi
  kubectl -n "${NS}" create secret generic ghcr-pull \
    --from-file=.dockerconfigjson="${secret_cfg}" \
    --type=kubernetes.io/dockerconfigjson \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -f "${tmp_cfg}"
  echo "   ghcr-pull secret applied from local GHCR credentials"
}

echo "== [1/6] preflight"
set +e
AZURE_REGION="${AZURE_REGION:-westus2}" \
  CONF_SKU="${CONF_SKU:-Standard_DC8as_v6}" \
  CONF_FAMILY="${CONF_FAMILY:-Standard DCasv6 Family vCPUs}" \
  REQUIRED_CONF_VCPU="${REQUIRED_CONF_VCPU:-32}" \
  OUT_DIR="${OUT_DIR}" bash experiments/aks/preflight.sh
pf=$?
set -e
enable_conf=true
if [ $pf -ne 0 ]; then
  echo "   preflight is NO-GO for confidential pool (exit ${pf}); deploying SYSTEM-ONLY."
  echo "   real SEV-SNP (G1) will remain BLOCKED until a confidential SKU has quota + exposure."
  enable_conf=false
fi

echo "== [2/6] terraform apply (enable_confidential_pool=${enable_conf})"
export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv | tr -d '\r\n')"
pushd "${TF_DIR}" >/dev/null
terraform init -input=false >/dev/null
terraform apply -input=false -auto-approve \
  -var "enable_confidential_pool=${enable_conf}" \
  | tee "${REPO_ROOT}/${OUT_DIR}/terraform-apply.log"
popd >/dev/null

echo "== [3/6] kubeconfig (local, gitignored)"
az aks get-credentials -g "${RG}" -n "${CLUSTER}" -f "${KUBECONFIG_FILE}" --overwrite-existing
export KUBECONFIG="${REPO_ROOT}/${KUBECONFIG_FILE}"
kubectl get nodes -o wide | tee "${OUT_DIR}/aks-nodes.txt"

echo "== [4/6] namespace + secrets (never committed)"
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -
# GHCR pull secret from local docker config (Docker Desktop credential helper).
create_ghcr_pull_secret
# OpenAI key secret for the realistic confidential-inference workload.
if [ -f "${OPENAI_KEY_FILE}" ]; then
  kubectl -n "${NS}" create secret generic openai-api \
    --from-file=api-key="${OPENAI_KEY_FILE}" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "   openai-api secret created from ${OPENAI_KEY_FILE} (not committed)"
else
  echo "   NOTE: ${OPENAI_KEY_FILE} not found; inference workload will run in echo-mode"
fi

echo "== [5/6] CRDs + Helm deploy"
kubectl apply -f "${CHART}/crds/"
helm upgrade --install ai-platform "${CHART}" -n "${NS}" \
  -f "${VALUES}" --set "images.tag=${IMAGE_TAG}" \
  | tail -5

echo "== [6/6] wait for trust-chain workloads"
kubectl -n "${NS}" rollout status deploy/central-verifier --timeout=180s || true
kubectl -n "${NS}" get ds,deploy -o wide | tee "${OUT_DIR}/aks-workloads.txt"
kubectl -n "${NS}" get rawattestationreports,attestationevidences -o wide 2>/dev/null \
  | tee "${OUT_DIR}/aks-attestation-objects.txt" || true

echo "== AKS UP complete. confidential_pool_enabled=${enable_conf}"
echo "   KUBECONFIG=${REPO_ROOT}/${KUBECONFIG_FILE}"
[ "${enable_conf}" = "true" ] || echo "   (system-only: real SEV-SNP experiments will be skipped honestly)"
