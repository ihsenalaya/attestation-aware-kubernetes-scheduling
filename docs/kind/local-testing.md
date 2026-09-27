# Local testing with kind

Run commands from the artifact root. Install Docker, kind, kubectl, Helm, and
the Go version required by `operator/go.mod` before deploying. Local kind runs
use simulated attestation and are regression checks; they do not reproduce the
real AMD SEV-SNP evidence collected on AKS.

## Build and unit checks

```bash
(cd operator && go build ./...)
(cd operator && go test ./pkg/... ./internal/scheduler/... ./internal/placement/...)
mkdir -p dist
(cd operator && go build -o ../dist/verify-placement ./cmd/verify-placement)
```

## Deploy and exercise placement

```bash
bash deploy/kind-deploy.sh
kubectl get pods -n ai-platform
VERIFY_BIN="$PWD/dist/verify-placement" bash experiments/artifact/e2e-kind-positive.sh
```

The deployment script creates the `ai-platform` kind cluster, builds and loads
the four article component images sequentially, installs the CRDs, and deploys
the Helm chart. It changes the active kubectl context to that cluster. The
positive scenario writes its results under `results/raw/kind/` by default.

The UI, platform API, key-release gateway, and thesis-bench modules are disabled
in this article deployment. Additional attack and regression scripts are in
`experiments/`; inspect their documented inputs before running a campaign.

## Diagnose a pending placement

```bash
kubectl get pods -n ai-platform
kubectl logs -n ai-platform deploy/attestation-scheduler
kubectl logs -n ai-platform deploy/central-verifier
kubectl get attestationevidences -A
```

The scheduler requires suitable evidence and a compatible policy/runtime. In
kind, verify that the values enable simulated evidence and the simulated
RuntimeClass described in [runtimeclass-simulation.md](runtimeclass-simulation.md).

The scheduler uses an ephemeral Ed25519 signing key when
`SCHEDULER_SIGNING_KEY_HEX` is absent. Preserve the public key from a run when
verifying its placement tokens. A scheduler restart generates a new key unless
a persistent seed is configured.

## Remove the local cluster

This removes the experiment cluster and its workloads:

```bash
kind delete cluster --name ai-platform
```
