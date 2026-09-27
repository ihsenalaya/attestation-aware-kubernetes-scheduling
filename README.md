# Attestation-Aware Scheduling for Verifiable AI Placement on SEV-SNP Confidential Kubernetes Nodes

Version 1.0.0 research artifact: reference implementation, archived measurements,
analysis scripts and working manuscript. The manuscript in `paper/` is the latest
local review version available when this artifact was prepared; it is anonymous
and is not represented as an accepted or author-approved final publication.

Repository: https://github.com/ihsenalaya/attestation-aware-kubernetes-scheduling

## Contents

| Path | Contents |
| --- | --- |
| `paper/` | LaTeX manuscript, compiled PDF, figures and supporting tables |
| `operator/` | Go module: scheduler, central verifier, node agent, admission components and offline token verifier |
| `charts/ai-confidential-governance-platform/` | Deployment chart |
| `deploy/`, `experiments/`, `automation/` | Local/AKS setup and experimental harnesses |
| `results/raw/` | Archived AKS measurements; kind/KWOK regression data |
| `results/tables/`, `results/figures/` | Archived derived outputs |
| `scripts/`, `requirements-analysis.txt` | Statistical analysis and plotting tools |
| `ARTIFACT_VALIDATION.md`, `PROVENANCE.json` | Checks performed, limitations and provenance |
| `SHA256SUMS` | SHA-256 inventory (all packaged files except this inventory itself) |

The Go module is shared with a wider platform and retains auxiliary FinOps/GovAR
components for build compatibility. These are outside this article's contribution.
The included RBAC template under `operator/charts/` is a test fixture.

## Verify and build

From the extracted artifact root:

```bash
sha256sum -c SHA256SUMS
cd operator
go build ./...
```

The module declares Go 1.25.0 and some dependencies require a newer toolchain;
use Go 1.27.1 (the tested toolchain) for this snapshot. This is the requirement
for the archived source, not a statement of the historical experimental toolchain.
See `ARTIFACT_VALIDATION.md` for unit/integration test commands and results.

## Analyze the archived AKS measurements

Use Python 3.12 and the pinned analysis dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements-analysis.txt
bash scripts/reproduce-analysis.sh reproduced
```

This runs the analysis scripts and regenerates supported outputs into a separate
output directory. It does not assert byte-identical reproduction of every archived
figure/table or recreate unavailable detailed logs. `paper/tables/` and
`results/tables/` preserve the archived evidence; output from this command is
additional derived data. The input measurements are not changed.

## Experimental reruns

Local regression deployment requires Docker, kind, kubectl and Helm:

```bash
bash deploy/kind-deploy.sh
```

For hardware experiments, use a dedicated Azure subscription/resource group with
appropriate confidential-VM quota and review `experiments/aks/README_AKS_CAMPAIGN.md`
and `docs/aks/private-deployment.md` first. These scripts create billable cloud
resources. Required cluster credentials and registry images are supplied by the
person running the experiment. No cloud infrastructure was created to validate
this package. AKS deployment values are in `deploy/aks-private/values.yaml`
(and `charts/ai-confidential-governance-platform/values-aks-private.yaml`).

## Evidence scope and limitations

- The hardware evidence concerns **node-level AMD SEV-SNP attestation on AKS**.
  It does not establish pod-level attestation or confidential GPU execution.
  kind/KWOK data is regression data and is not hardware-security evidence.
- Subscription identifiers and Azure kubelet client identifiers are redacted.
  Original MAA raw tokens are placeholders in the provided historical data:
  their original issuer signatures cannot be reverified from this public archive.
  Public keys and placement-token evidence are retained. See
  `docs/public-artifact-redaction.md`.
- Some CSV `raw_log_path` values refer to detailed logs unavailable in both the
  initial archive and the source workspace. The CSV measurements are retained;
  missing logs have not been reconstructed. Per-run seeds are not present in
  every CSV. See the validation report for the affected files.
- In the B4/B5 comparison, B5's zero external gate-removal window is a value
  assigned by the harness for a path without gate removal. It is not a measured
  claim of a zero-length PreBind-to-Bind interval.
- This package was checked locally. Archived AKS experiments were not rerun,
  and the checks do not independently establish the manuscript's scientific claims.

## Paper, citation and publication

Build instructions are in `paper/README_OVERLEAF.md`. Cite the artifact using
`CITATION.cff`. Zenodo metadata is in `.zenodo.json`; no DOI is invented.
`PUBLISHING.md` describes enabling this dedicated repository in Zenodo and
publishing the prepared GitHub release. Version 1.0.0 is a prepared artifact
version, not evidence that a Zenodo record already exists.

License: Apache-2.0, as supplied with the original artifact; see `LICENSE`.
