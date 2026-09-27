#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# check_real_attestation.sh — verify REAL SEV-SNP MAA attestation on AKS (G1).
#
# Reads the AttestationEvidence produced by the central-verifier from the
# node-attestation-agent's RawAttestationReport on a real confidential node, and
# records whether evidenceMode=real was legitimately reached. It NEVER forces
# real: it only reports what the verifier actually wrote.
#
# Exit 0 => at least one node has evidenceMode=real (G1 PASS material).
# Exit 3 => no real evidence (G1 stays FAIL/BLOCKED, documented honestly).
set -uo pipefail
NS="${NS:-ai-platform}"
OUT_DIR="${OUT_DIR:-results/raw/aks}"
mkdir -p "${OUT_DIR}"
SUMMARY="${OUT_DIR}/attestation-real-summary.json"
RAW_REPORTS="${OUT_DIR}/aks-rawattestationreports.json"
RAW_REPORTS_FULL="${OUT_DIR}/.sensitive-aks-rawattestationreports-full.json"
CURRENT_NODES_JSON="${OUT_DIR}/aks-current-confidential-nodes.json"

echo "== collecting confidential nodes + attestation objects"
kubectl get nodes -l ai.sovereign.io/tee=SEV-SNP -o wide 2>/dev/null > "${OUT_DIR}/aks-nodes-confidential.txt" || true
kubectl get nodes -l ai.sovereign.io/tee=SEV-SNP -o json 2>/dev/null > "${CURRENT_NODES_JSON}" || echo '{"items":[]}' > "${CURRENT_NODES_JSON}"
kubectl -n "${NS}" get rawattestationreports -o json 2>/dev/null > "${RAW_REPORTS_FULL}" || echo '{}' > "${RAW_REPORTS_FULL}"
python3 - "${RAW_REPORTS_FULL}" "${RAW_REPORTS}" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
data = json.load(open(src))
for item in data.get("items", []):
    spec = item.get("spec", {})
    if spec.get("rawToken"):
        spec["rawToken"] = "<redacted: see spec.rawTokenHash>"
json.dump(data, open(dst, "w"), indent=2)
PY
rm -f "${RAW_REPORTS_FULL}"
kubectl -n "${NS}" get attestationevidences -o json 2>/dev/null > "${OUT_DIR}/aks-nodepools-confidential.json" || echo '{}' > "${OUT_DIR}/aks-nodepools-confidential.json"

python3 - "${CURRENT_NODES_JSON}" "${OUT_DIR}/aks-nodepools-confidential.json" "${SUMMARY}" <<'PY'
import json, sys, datetime
nodes = json.load(open(sys.argv[1])).get("items", [])
current_nodes = {
    n.get("metadata", {}).get("name", "")
    for n in nodes
    if n.get("metadata", {}).get("name")
}
evs_all = json.load(open(sys.argv[2])).get("items", [])
evs = [
    e for e in evs_all
    if e.get("spec", {}).get("subjectRef", {}).get("name", "") in current_nodes
]
real, simulated, unverified, failed = [], [], [], []
for e in evs:
    st = e.get("status", {})
    mode = st.get("evidenceMode", "")
    name = e.get("metadata", {}).get("name", "")
    subject = e.get("spec", {}).get("subjectRef", {}).get("name", "")
    rec = {
        "name": name,
        "subjectNode": subject,
        "evidenceMode": mode,
        "verificationStatus": st.get("verificationStatus", ""),
        "verifiedBy": st.get("verifiedBy", ""),
        "maaTokenHash": st.get("maaTokenHash", ""),
        "claimsDigest": st.get("claimsDigest", ""),
        "nodeUID": st.get("nodeUID", ""),
        "attestationType": st.get("attestationType", ""),
    }
    {"real": real, "simulated": simulated, "unverified": unverified}.get(mode, failed).append(rec)
out = {
    "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "current_confidential_nodes": sorted(current_nodes),
    "total_evidence_all_namespaces_current_cluster": len(evs_all),
    "total_evidence": len(evs),
    "real_count": len(real),
    "simulated_count": len(simulated),
    "unverified_count": len(unverified),
    "other_count": len(failed),
    "real": real,
    "note": "evidenceMode=real is written ONLY by the central-verifier after a genuine MAA signature+claims check.",
}
json.dump(out, open(sys.argv[3], "w"), indent=2)
print(f"current_nodes={len(current_nodes)} current_evidence={len(evs)} real={len(real)} simulated={len(simulated)} unverified={len(unverified)} other={len(failed)}")
sys.exit(0 if real else 3)
PY
rc=$?
echo "== attestation summary: ${SUMMARY}"
if [ $rc -eq 0 ]; then
  echo "== G1 material PRESENT: at least one node has evidenceMode=real"
else
  echo "== G1 NOT satisfied: no evidenceMode=real. Documented honestly; do NOT fake."
fi
exit $rc
