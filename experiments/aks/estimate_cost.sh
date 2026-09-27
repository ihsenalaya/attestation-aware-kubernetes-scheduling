#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# estimate_cost.sh — real hourly estimate for the planned AKS topology.
# Queries the PUBLIC Azure Retail Prices API (no auth, no resources). Never
# invents a price: prints UNAVAILABLE if the API is unreachable.
set -uo pipefail

CONF_SKU="${CONF_SKU:-Standard_DC8as_v6}"
SYS_SKU="${SYS_SKU:-Standard_D2s_v3}"
CONF_NODES="${CONF_NODES:-4}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
mkdir -p "${OUT_DIR}"
arm_region="${AZURE_REGION:-westus2}"

price_for() {
  local sku="$1"
  local url="https://prices.azure.com/api/retail/prices?\$filter=armRegionName eq '${arm_region}' and armSkuName eq '${sku}' and priceType eq 'Consumption'"
  curl -fsS --max-time 25 "${url}" 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
best=None
for it in d.get('Items',[]):
    name=(it.get('productName','')+' '+it.get('skuName','')+' '+it.get('meterName','')).lower()
    if 'windows' in name or 'spot' in name or 'low priority' in name: continue
    p=it.get('retailPrice')
    if p and (best is None or p<best): best=p
if best is not None: print(best)
"
}

: > "${OUT_DIR}/cost-estimate.txt"
emit() { echo "$1" | tee -a "${OUT_DIR}/cost-estimate.txt"; }
emit "AKS cost estimate — $(date -u '+%Y-%m-%dT%H:%M:%SZ') region=${arm_region}"
cp="$(price_for "${CONF_SKU}")"; sp="$(price_for "${SYS_SKU}")"
[ -n "${cp}" ] && emit "confidential ${CONF_SKU}: \$${cp}/hr x ${CONF_NODES} nodes" || emit "confidential ${CONF_SKU}: PRICE UNAVAILABLE (do not guess)"
[ -n "${sp}" ] && emit "system ${SYS_SKU}: \$${sp}/hr x 1 node" || emit "system ${SYS_SKU}: PRICE UNAVAILABLE (do not guess)"
if [ -n "${cp}" ] && [ -n "${sp}" ]; then
  python3 - "$cp" "$sp" "$CONF_NODES" <<'PY' | tee -a "${OUT_DIR}/cost-estimate.txt"
import sys
cp=float(sys.argv[1]); sp=float(sys.argv[2]); n=int(sys.argv[3])
h=cp*n+sp
print(f"combined: ${h:.4f}/hr  ~ ${h*4:.2f} per 4h campaign window  ~ ${h*24:.2f}/day")
print("Control plane Free tier=$0; egress/disks/LB extra, not included.")
PY
else
  emit "combined: UNAVAILABLE"
fi
echo "== cost estimate written (no resources created)"
