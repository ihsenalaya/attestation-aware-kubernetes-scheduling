#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# preflight.sh — Article 1 AKS campaign preflight (authoritative GO/NO-GO gate).
#
# Verifies, BEFORE any billable resource, that:
#   1. an Azure account is logged in (subscription ID never persisted),
#   2. region is westus2 by default,
#   3. Total Regional vCPU quota is sufficient,
#   4. the CONFIDENTIAL family quota (DCasv6 by default) is >= required,
#   5. ARM actually EXPOSES the target confidential SKU with SEV-SNP and no
#      blocking restriction (this was the real blocker on 2026-07-04).
#
# Exit 0 => GO for confidential pool. Exit 3 => system-only possible but
# confidential NOT available (caller must set enable_confidential_pool=false).
# Exit 1 => hard failure. Creates NO resources. Writes evidence to OUT_DIR.
set -uo pipefail

AZURE_REGION="${AZURE_REGION:-westus2}"
CONF_SKU="${CONF_SKU:-Standard_DC8as_v6}"
CONF_FAMILY="${CONF_FAMILY:-Standard DCasv6 Family vCPUs}"
REQUIRED_CONF_VCPU="${REQUIRED_CONF_VCPU:-32}"
REQUIRED_TOTAL="${REQUIRED_TOTAL:-40}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
mkdir -p "${OUT_DIR}"
SUMMARY="${OUT_DIR}/preflight-summary.txt"

log() { echo "$@"; }
fail() { echo "PREFLIGHT FAIL: $*" >&2; exit 1; }

# az retry wrapper for the WSL vsock flakiness.
azq() { local n=6 i; for i in $(seq 1 "$n"); do if out="$("$@" 2>/dev/null)"; then printf '%s' "$out" | tr -d '\r'; return 0; fi; sleep 4; done; return 1; }

log "== AKS campaign preflight — region=${AZURE_REGION}, conf_sku=${CONF_SKU} (no resources)"

sub_name="$(azq az account show --query name -o tsv)"
[ -n "${sub_name}" ] || fail "not logged in (run 'az login')"
log "  [1] azure login OK — subscription: ${sub_name}"

# The Article 1 evidence campaign is pinned to westus2/DCasv6. Older eastus2
# DCASv5 attempts are historical and must not be revived for new paper results.
case "${AZURE_REGION}" in
  westus2) : ;;
  *) fail "region must be westus2 for the Article 1 DCasv6 campaign (got ${AZURE_REGION})" ;;
esac
log "  [2] region ${AZURE_REGION} OK"

usage="$(azq az vm list-usage --location "${AZURE_REGION}" -o json)" || fail "cannot read vm usage"
echo "${usage}" > "${OUT_DIR}/vm-usage-${AZURE_REGION}.json"

read_quota() { # family-localized-substring -> "current limit"
  echo "${usage}" | python3 -c "
import json,sys
key=sys.argv[1]
for u in json.load(sys.stdin):
    name=u.get('name',{}).get('localizedValue','')
    if key.lower() in name.lower():
        print(int(u['currentValue']), int(u['limit'])); break
" "$1"
}

total="$(read_quota 'Total Regional vCPUs')"
[ -n "${total}" ] || fail "Total Regional vCPUs quota not found"
tcur="${total% *}"; tlim="${total#* }"
log "  [3] Total Regional vCPUs: current=${tcur} limit=${tlim} (need >= ${REQUIRED_TOTAL})"
[ "${tlim}" -ge "${REQUIRED_TOTAL}" ] || fail "Total Regional vCPU limit ${tlim} < ${REQUIRED_TOTAL}"

conf="$(read_quota "${CONF_FAMILY}")"
conf_cur="0"; conf_lim="0"
if [ -n "${conf}" ]; then conf_cur="${conf% *}"; conf_lim="${conf#* }"; fi
log "  [4] ${CONF_FAMILY}: current=${conf_cur} limit=${conf_lim} (need >= ${REQUIRED_CONF_VCPU})"

# ARM SKU exposure check (the decisive gate).
sub="$(azq az account show --query id -o tsv)"
token="$(azq az account get-access-token --resource https://management.azure.com --query accessToken -o tsv)"
sku_json=""
if [ -n "${token}" ] && [ -n "${sub}" ]; then
  sku_json="$(curl -fsS -H "Authorization: Bearer ${token}" \
    "https://management.azure.com/subscriptions/${sub}/providers/Microsoft.Compute/skus?api-version=2021-07-01&%24filter=location%20eq%20%27${AZURE_REGION}%27" 2>/dev/null || true)"
fi
unset token
exposure="UNKNOWN"; cc=""
if [ -n "${sku_json}" ]; then
  echo "${sku_json}" | python3 -c "
import json,sys
sku=sys.argv[1]
data=json.load(sys.stdin)
for x in data.get('value',[]):
    if x.get('name')==sku:
        r=x.get('restrictions',[])
        cc=[c.get('value') for c in x.get('capabilities',[]) if c.get('name')=='ConfidentialComputingType']
        blocked=any(rr.get('reasonCode') for rr in r)
        print('EXPOSED' if not blocked else 'RESTRICTED', ','.join(cc) if cc else '-')
        break
else:
    print('NOT-EXPOSED','-')
" "${CONF_SKU}" > "${OUT_DIR}/.exposure" 2>/dev/null || true
  read -r exposure cc < "${OUT_DIR}/.exposure" 2>/dev/null || true
  rm -f "${OUT_DIR}/.exposure"
fi
log "  [5] ARM SKU ${CONF_SKU}: exposure=${exposure} confidential=${cc}"

# Decide.
result="NO-GO"
verdict_exit=3
if [ "${exposure}" = "EXPOSED" ] && [ "${conf_lim}" -ge "${REQUIRED_CONF_VCPU}" ]; then
  result="GO-CONFIDENTIAL"; verdict_exit=0
elif [ "${exposure}" = "EXPOSED" ]; then
  result="NO-GO-NEED-QUOTA (SKU exposed but ${CONF_FAMILY} limit=${conf_lim})"; verdict_exit=3
else
  result="NO-GO-SKU-NOT-EXPOSED (${CONF_SKU})"; verdict_exit=3
fi

cat > "${SUMMARY}" <<EOF
AKS campaign preflight — $(date -u '+%Y-%m-%dT%H:%M:%SZ')
region=${AZURE_REGION}
subscription_name=${sub_name}
confidential_sku=${CONF_SKU}
confidential_family=${CONF_FAMILY}
confidential_quota_current=${conf_cur} confidential_quota_limit=${conf_lim} required=${REQUIRED_CONF_VCPU}
total_regional_current=${tcur} total_regional_limit=${tlim} required=${REQUIRED_TOTAL}
arm_sku_exposure=${exposure} confidential_type=${cc}
result=${result}
EOF
log "== PREFLIGHT ${result} (evidence: ${SUMMARY})"
exit ${verdict_exit}
