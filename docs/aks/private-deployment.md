# AKS Private Deployment

## Prerequisites

- AKS cluster with:
  - Private cluster networking (no public API server endpoint)
  - Azure CNI or Azure CNI Overlay
  - Node pool with confidential AMD SEV-SNP VM SKU (`Standard_DC8as_v6` for Article 1)
  - Azure Key Vault with CSI driver enabled (for secrets injection)
  - Internal load balancer only (`service.beta.kubernetes.io/azure-load-balancer-internal: "true"`)
- GitHub Container Registry (GHCR) image access configured for the AKS cluster
- Helm 3.14+
- kubectl with cluster access

## Article 1 AKS runtime scope

Article 1 uses AKS `Standard_DC8as_v6` confidential VM nodes as real
node-level AMD SEV-SNP evidence. AKS rejected `workloadRuntime=KataMshvVmIsolation`
on this SKU because Pod Sandboxing/Kata requires nested virtualization. Therefore
the Article 1 AKS harness uses `runtimeClassName: runc` on tainted SEV-SNP nodes
and reports the result as node-level attested placement, not pod-level
confidential-container attestation.

Do not set `kata-vm-isolation` or claim pod-level isolation for the DCasv6
Article 1 results unless a separate AKS pool/SKU with supported Pod Sandboxing is
created and measured as a distinct experiment.

## Image registry

All images must be pushed to GHCR before deployment:

```bash
export REGISTRY=ghcr.io/ihsenalaya/ai-sovereign-finops-operator
export VERSION=0.5.11

docker build -t "$REGISTRY/controller:$VERSION" -f operator/Dockerfile operator
docker build -t "$REGISTRY/attestation-scheduler:$VERSION" -f operator/Dockerfile.scheduler operator
docker build -t "$REGISTRY/central-verifier:$VERSION" -f operator/Dockerfile.central-verifier operator
docker build -t "$REGISTRY/node-attestation-agent:$VERSION" -f operator/Dockerfile.node-attestation-agent operator
for image in controller attestation-scheduler central-verifier node-attestation-agent; do
  docker push "$REGISTRY/$image:$VERSION"
done
```

## Secrets management

**NEVER** put secrets in `values-aks-private.yaml` or any file committed to Git.

Required secrets — inject via Azure Key Vault CSI driver or Sealed Secrets:

| Secret | Description | Reference |
|---|---|---|
| `scheduler-signing-key` | Ed25519 private key hex for placement token signing | `SCHEDULER_SIGNING_KEY_HEX` |
| `token-public-key` | Ed25519 public key hex for gateway verification | `TOKEN_PUBLIC_KEY_HEX` |
| OIDC config | Client ID + tenant ID for platform-api auth | `platformApi.oidc.*` |

The scheduler accepts a 32-byte Ed25519 seed encoded as 64 hex characters
through `SCHEDULER_SIGNING_KEY_HEX`. Without it, the scheduler creates an
ephemeral key at startup. The chart value `attestationScheduler.signingKeyHex`
renders a Kubernetes Secret and supplies that environment variable. Supply a
persistent seed through your deployment's secret management process; never
commit it to this artifact. Preserve the corresponding public key with the
placement tokens from each run. No key-generation CLI is included.

## Deployment

```bash
helm upgrade --install ai-platform \
  charts/ai-confidential-governance-platform \
  -f charts/ai-confidential-governance-platform/values-aks-private.yaml \
  --set global.registry=ghcr.io/ihsenalaya/ai-sovereign-finops-operator \
  --set images.tag=0.5.11 \
  --set modules.keyReleaseGateway.enabled=false \
  --set modules.platformApi.enabled=false \
  --set modules.platformUi.enabled=false \
  --set modules.thesisBench.enabled=false \
  --namespace ai-platform \
  --create-namespace \
  --wait --timeout 10m
```

The article deployment disables the optional API/UI/key-release modules. The
production guidance below describes the larger platform and requires separate
configuration. The chart does not provision private AKS networking or a Key
Vault CSI integration. Its network-policy setting must also be backed by
appropriate policies in the target cluster.

## Validation checklist

For Article 1, only real AKS SEV-SNP outputs under `results/raw/aks/`
may be used as main-paper empirical results. `kind` and `kwok` outputs are
regression/debug artifacts only.

After deployment:
```bash
# No LoadBalancer services
kubectl get svc -n ai-platform | grep LoadBalancer
# Expected: no output

# Simulated evidence not active
kubectl get configmap -n ai-platform -o yaml | grep simulatedEvidence
# Expected: false

# NetworkPolicies applied
kubectl get networkpolicy -n ai-platform
# Expected: at least one policy

# API auth active (should reject unauthenticated requests)
curl -f http://<internal-platform-api>:8083/api/v1/overview
# Expected: 401 Unauthorized

# Scheduler running
kubectl get pod -n ai-platform -l component=attestation-scheduler
```

## Ingress (internal)

The platform UI and API are ClusterIP only. Access via:
- Internal nginx ingress with annotation `service.beta.kubernetes.io/azure-load-balancer-internal: "true"`
- VPN or bastion host for direct access
- No public endpoint should be created

See the commented ingress block in `values-aks-private.yaml`.

## Audit anchoring

The production audit anchoring backend is planned (Azure Blob). In the interim, use the file backend with a persistent volume backed by Azure Disk (ZRS for HA):

```yaml
# In your deployment values overlay:
global:
  audit:
    anchoring:
      mode: file
      filePath: /data/audit/checkpoints.jsonl
# Mount a PVC backed by Azure Disk at /data/audit
```

Switch to `azure-blob` mode when that backend is implemented.
