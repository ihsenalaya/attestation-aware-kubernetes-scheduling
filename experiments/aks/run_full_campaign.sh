#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# run_full_campaign.sh — end-to-end AKS Q1 campaign orchestrator.
#
# Order (each step writes raw files under results/raw/aks; fail-closed):
#   0. preflight (GO/NO-GO)                     -> preflight-summary.txt
#   1. aks_up (terraform + platform + secrets)  -> aks-nodes.txt, aks-workloads.txt
#   2. real MAA attestation check               -> attestation-real-summary.json
#   3. scheduler self-security (S1-S12)         -> scheduler_security_tests.csv
#   4. AKS core security campaign                -> security_attacks_A1_A10.csv
#      Current AKS evidence covers A1,A4,A5,A5b,A8,A10. A2,A3,A6,A7,A9 must
#      stay NOT_EXECUTED/PENDING_AKS_RERUN until an AKS rerun exists.
#   5. scheduling latency (legacy B5 path)       -> scheduling_latency.csv
#   6. ablation                                  -> ablation.csv
#   7. identity binding / verifiable placement  -> identity_binding.csv
#   8. B4 vs B5 baseline comparison              -> b4_vs_b5.csv
#   9. real AI workloads                         -> ai_workloads.csv
#   10. force 4-node conf pool if requested       -> Azure nodepool update
#   11. multi-node qualification                  -> multinode_node_selection.csv
#   12. B1-B5 AKS performance                     -> performance_b1_b5_high_resolution.csv
#   13. collect + stats                           -> results/tables/*.csv
#   14. aks_stop + (if DESTROY_AKS_AFTER_RUN) aks_destroy -> cost_cleanup_summary.txt
#
# Env: DESTROY_AKS_AFTER_RUN (default true), KEEP_AKS_FOR_DEBUG (default false),
# N_RUNS_PERFORMANCE (30), etc. See feuille-de-route-Q1.md §6.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"
AKS_DIR="experiments/aks"
HARNESS="experiments/harness"
SCHEDSEC="experiments/scheduler-security"
OUT_DIR="results/raw/aks"
ENV_NAME="${ENV_NAME:-aks-real-sevsnp}"
NS="${NS:-ai-platform}"
RG="${RG:-rg-article1-confidential}"
CLUSTER="${CLUSTER:-aks-article1}"
CONF_NODEPOOL="${CONF_NODEPOOL:-conf}"
AKS_FORCE_CONF_MIN_NODES="${AKS_FORCE_CONF_MIN_NODES:-4}"
DESTROY_AKS_AFTER_RUN="${DESTROY_AKS_AFTER_RUN:-true}"
KEEP_AKS_FOR_DEBUG="${KEEP_AKS_FOR_DEBUG:-false}"
N_RUNS_PERFORMANCE="${N_RUNS_PERFORMANCE:-30}"
KUBECONFIG_FILE="automation/terraform/aks-confidential/kubeconfig-aks"
REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS:-${RUNTIME_CLASS}}"
REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
FRESH_POLICY_MAX_AGE_SECONDS="${FRESH_POLICY_MAX_AGE_SECONDS:-7200}"
mkdir -p "${OUT_DIR}"
step() { echo; echo "########## $* ##########"; }

step "0. preflight"
set +e; bash "${AKS_DIR}/preflight.sh"; pf=$?; set -e
if [ $pf -ne 0 ]; then
  echo "preflight NO-GO for confidential (exit ${pf}). Aborting real campaign."
  echo "Set the DCasv6 quota first, then re-run. No resources created."
  exit ${pf}
fi

step "1. aks_up"
bash "${AKS_DIR}/aks_up.sh"
export KUBECONFIG="${REPO_ROOT}/${KUBECONFIG_FILE}"

step "2. real MAA attestation"
bash "${AKS_DIR}/check_real_attestation.sh" || echo "  (attestation check reported non-real; G1 stays honest)"

step "3. scheduler self-security (G8 S1-S12)"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  bash "${SCHEDSEC}/run_scheduler_security_tests.sh" || echo "  (some S-tests failed; see CSV)"

step "4. AKS core security campaign"
echo "AKS evidence rule: main-paper security rows must come from real AKS SEV-SNP, not kind."
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  bash "${HARNESS}/run_security_campaign.sh" || echo "  (see security CSV for any failure)"

step "4b. AKS missing attacks A2/A3/A6/A7/A9"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  FRESH_POLICY_MAX_AGE_SECONDS="${FRESH_POLICY_MAX_AGE_SECONDS}" \
  bash experiments/attacks/run_aks_missing_attacks_A2_A3_A6_A7_A9.sh || echo "  (missing-attack harness note)"

step "4c. A11 GPU-required fail-closed"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  RUNTIME_CLASS="${RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  bash experiments/attacks/a11_confidential_gpu_required_no_evidence.sh || echo "  (A11 note)"

python3 scripts/merge_aks_security_a1_a11.py || echo "  (security merge note)"

step "5. scheduling latency N=${N_RUNS_PERFORMANCE}"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" N_RUNS="${N_RUNS_PERFORMANCE}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  OUT_DIR="${OUT_DIR}" OUT_CSV="${OUT_DIR}/scheduling_latency.csv" \
  bash "${HARNESS}/measure_scheduling.sh" || echo "  (latency harness reported failures)"

step "6. ablation"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  bash "${HARNESS}/measure_ablation.sh" || echo "  (ablation harness note)"

step "7. identity binding / verifiable placement"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  bash "${HARNESS}/measure_identity_binding.sh" || echo "  (identity harness note)"

step "8. B4 vs B5 strong baseline"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  BENCH_RUNTIME_CLASS="${BENCH_RUNTIME_CLASS}" \
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  bash experiments/baselines/run_b4_vs_b5.sh || echo "  (B4/B5 harness note)"

step "9. governed real AI workloads"
if ! ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES}" \
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE}" \
  bash experiments/ai-workload/run_ai_workloads_aks.sh; then
  echo "  (AI workload harness failed; see ai_workloads.csv and raw logs)"
fi

step "10. force confidential pool size for multi-node/performance"
if command -v az >/dev/null 2>&1 && [ "${AKS_FORCE_CONF_MIN_NODES}" -gt 0 ]; then
  az aks nodepool update --resource-group "${RG}" --cluster-name "${CLUSTER}" \
    --name "${CONF_NODEPOOL}" --update-cluster-autoscaler \
    --min-count "${AKS_FORCE_CONF_MIN_NODES}" --max-count "${AKS_FORCE_CONF_MIN_NODES}" \
    -o json > "${OUT_DIR}/aks-nodepool-conf-forced-${AKS_FORCE_CONF_MIN_NODES}.json" \
    || echo "  (nodepool scale note; multi-node may fail if fewer nodes are active)"
fi

step "11. multi-node node qualification"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  bash experiments/aks/run_multinode_node_selection.sh || echo "  (multi-node harness note)"

step "12. B1-B5 AKS performance"
ENV_NAME="${ENV_NAME}" PLATFORM_NS="${NS}" OUT_DIR="${OUT_DIR}" \
  REQUIRED_TEE="${REQUIRED_TEE}" RUNTIME_CLASS="${RUNTIME_CLASS}" \
  N_RUNS_PERFORMANCE="${N_RUNS_PERFORMANCE}" WARMUP="${WARMUP:-2}" \
  bash experiments/performance/run_aks_b1_b5_latency.sh || echo "  (B1-B5 performance harness note)"

step "13. stats + figures"
python3 scripts/analyze_all.py --raw "${OUT_DIR}" \
  --tables results/tables --figures paper/figures \
  --env "${ENV_NAME}" || echo "  (analysis note)"
python3 scripts/update_q1_final_artifacts.py || echo "  (Q1 final artifact update note)"

step "14. teardown"
bash "${AKS_DIR}/aks_stop.sh"
if [ "${KEEP_AKS_FOR_DEBUG}" = "true" ]; then
  echo "KEEP_AKS_FOR_DEBUG=true -> NOT destroying. Active resources remain."
  echo "Destroy later with: bash ${AKS_DIR}/aks_destroy.sh"
elif [ "${DESTROY_AKS_AFTER_RUN}" = "true" ]; then
  bash "${AKS_DIR}/aks_destroy.sh"
else
  echo "DESTROY_AKS_AFTER_RUN=false -> cluster left running (stopped pool)."
fi

step "CAMPAIGN COMPLETE"
echo "Raw results: ${OUT_DIR}"
echo "Tables: results/tables ; Figures: paper/figures"
