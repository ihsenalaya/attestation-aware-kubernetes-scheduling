# AKS Confidential Campaign — Article 1 (SEV-SNP, DCasv6)

One-command Q1 experimental campaign on real AMD SEV-SNP confidential AKS nodes.
The main paper may use only AKS real SEV-SNP outputs. `kind` and `kwok` remain
useful for CI, debug, and regression, but they must not be cited as Article 1
security or performance evidence. **Nothing is fabricated**; missing inputs are
recorded as `NOT_EXECUTED`/`PENDING_AKS_RERUN`, and any unblocked attack is a
failure.

Scope note: the DCasv6 campaign is **node-level SEV-SNP**. AKS rejects
`workloadRuntime=KataMshvVmIsolation` on `Standard_DC8as_v6` because Pod
Sandboxing/Kata requires nested virtualization. Therefore Article 1 AKS runs use
`runtimeClassName=runc` on tainted SEV-SNP nodes and must not be described as
pod-level confidential-container or `kata-vm-isolation` evidence.

## Prerequisites
- `az login` (subscription with a confidential-VM SKU that ARM exposes in the
  target region and has quota). The current Article 1 target is **westus2,
  DCasv6, Standard_DC8as_v6, 32 vCPU**.
- `terraform`, `kubectl`, `helm`, `python3` (numpy/scipy/matplotlib).
- GHCR images pushed (Makefile targets); a local docker config for the pull
  secret. OpenAI key at `ai_key.txt` (gitignored) for the realistic
  inference workload (optional; echo-mode otherwise).

## The one command
```bash
cd <repo-root>
bash experiments/aks/run_full_campaign.sh
```
This runs, in order (each writes raw files under `results/raw/aks/`):
1. `preflight.sh` — GO/NO-GO (quota + ARM SKU exposure). Aborts if NO-GO.
2. `aks_up.sh` — terraform apply (4× DC8as_v6 = 32 vCPU SEV-SNP + D2s_v3 system),
   kubeconfig (local, gitignored), ghcr-pull + openai secrets, Helm deploy,
   central-verifier + node-attestation-agent.
3. `check_real_attestation.sh` — records whether `evidenceMode=real` was legitimately
   reached (→ `attestation-real-summary.json`). Honest: no real → G1 stays FAIL.
4. scheduler self-security (G8, S1–S12) → `scheduler_security_tests.csv`.
5. AKS core security campaign (N≥30) → `security_attacks_A1_A10.csv`;
   currently A1,A4,A5,A5b,A8,A10 are executed on AKS, while A2,A3,A6,A7,A9
   remain `PENDING_AKS_RERUN` unless rerun by a later batch.
6. scheduling latency (N≥30) → `scheduling_latency.csv`; microsecond phase
   fields require the GHCR `0.5.11` scheduler image.
7. ablation → `ablation.csv`.
8. identity binding / verifiable placement → `identity_binding.csv`.
9. stats + figures → `results/tables/*.csv`, `paper/figures/*.pdf`.
10. `aks_stop.sh` then (if `DESTROY_AKS_AFTER_RUN=true`) `aks_destroy.sh`.

## Individual steps (for debugging / partial runs)
```bash
# Gate only (no resources):
CONF_SKU=Standard_DC8as_v6 bash experiments/aks/preflight.sh
# Cost estimate (no resources):
bash experiments/aks/estimate_cost.sh
# Bring up / stop / destroy:
bash experiments/aks/aks_up.sh
bash experiments/aks/aks_stop.sh      # scale conf pool to 0
bash experiments/aks/aks_destroy.sh   # terraform destroy + verify
```

## Environment variables (feuille-de-route-Q1.md §6)
```
DESTROY_AKS_AFTER_RUN=true   KEEP_AKS_FOR_DEBUG=false
IMAGE_TAG=0.5.11
N_RUNS_SECURITY=30  N_RUNS_PERFORMANCE=30  N_RUNS_RACE=30
N_RUNS_ABLATION=30  N_RUNS_SCALABILITY=10
AZURE_REGION=westus2  CONF_SKU=Standard_DC8as_v6   REQUIRED_CONF_VCPU=32
RUNTIME_CLASS=runc    REQUIRE_CONFIDENTIAL_CONTAINERS=false
```

## Regression on kind first (save AKS time/money)
The same harnesses can be smoke-tested on the local kind cluster, but these
outputs are regression-only and excluded from the paper's main evaluation:
```bash
ENV_NAME=kind-live-simulated PLATFORM_NS=ai-platform \
  OUT_DIR=results/raw/kind \
  bash experiments/scheduler-security/run_scheduler_security_tests.sh
ENV_NAME=kind-live-simulated N_RUNS=5 OUT_DIR=results/raw/kind \
  bash experiments/harness/measure_scheduling.sh
```

## Safety / honesty invariants
- No secret is ever committed (kubeconfig, tfstate, ai_key.txt, ghcr secret all
  gitignored / created in-cluster only).
- `evidenceMode=real` is written ONLY by the central-verifier after a genuine MAA
  signature+claims check. If MAA is unreachable → `Unavailable`/`unverified`.
- DCasv6 rows are node-level SEV-SNP rows. They are not pod-level Kata rows.
- Every result row carries `env` (kind|aks) and a `raw_log_path`.
- Main-paper empirical rows must have `env=aks-real-sevsnp`.
- Teardown is verified (`az group exists` == false) and reported in
  `cost_cleanup_summary.txt`.
