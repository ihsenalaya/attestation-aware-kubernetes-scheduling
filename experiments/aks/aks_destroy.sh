#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# aks_destroy.sh — destroy ALL AKS resources and verify. Writes a cleanup report.
# Also removes local sensitive files (kubeconfig, tfstate) after teardown.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"
TF_DIR="automation/terraform/aks-confidential"
RG="${RG:-rg-article1-confidential}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
mkdir -p "${OUT_DIR}"
REPORT="${OUT_DIR}/cost_cleanup_summary.txt"

echo "== terraform destroy"
export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv | tr -d '\r\n')"
pushd "${TF_DIR}" >/dev/null
terraform destroy -input=false -auto-approve | tee "${REPO_ROOT}/${OUT_DIR}/terraform-destroy.log"
state_after="$(terraform state list 2>/dev/null | wc -l)"
popd >/dev/null

echo "== verify resource group is gone"
exists="$(az group exists -n "${RG}" 2>/dev/null | tr -d '\r' || echo unknown)"

{
  echo "AKS cleanup summary — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "resource_group=${RG}"
  echo "az_group_exists_after_destroy=${exists}"
  echo "terraform_state_entries_after=${state_after}"
} > "${REPORT}"

if [ "${exists}" != "false" ] || [ "${state_after}" -ne 0 ]; then
  echo "ERROR: cleanup not verified; preserving local state for recovery" >&2
  exit 1
fi

echo "== removing local sensitive files (not committed anyway)"
rm -f "${TF_DIR}/kubeconfig-aks" "${TF_DIR}/terraform.tfstate" "${TF_DIR}/terraform.tfstate.backup" "${TF_DIR}"/*.tfplan 2>/dev/null || true

echo "== cleanup report:"; cat "${REPORT}"
[ "${exists}" = "false" ] && echo "== DESTROY VERIFIED (RG gone)" || echo "== WARNING: RG may still exist — check manually"
