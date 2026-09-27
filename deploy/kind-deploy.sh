#!/usr/bin/env bash
set -euo pipefail

VERSION="${VERSION:-0.5.11}"
NAMESPACE="${NAMESPACE:-ai-platform}"
CLUSTER="${CLUSTER:-ai-platform}"
SKIP_BUILD="${SKIP_BUILD:-false}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPERATOR="$ROOT/operator"
CHART="$ROOT/charts/ai-confidential-governance-platform"

log() { echo "[kind-deploy] $*"; }

# ── 1. Cluster ────────────────────────────────────────────────────────────────
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER}$"; then
  log "Creating kind cluster '$CLUSTER'..."
  kind create cluster --name "$CLUSTER" --config "$ROOT/deploy/kind-config.yaml" --wait 60s
else
  log "Cluster '$CLUSTER' already exists — skipping creation."
fi

# Point kubectl to this cluster
kubectl config use-context "kind-${CLUSTER}"

# ── 2. Build images ───────────────────────────────────────────────────────────
# Built sequentially to avoid OOM on RAM-constrained hosts (WSL2 ~7 GiB).
if [ "$SKIP_BUILD" != "true" ]; then
  log "Building Docker images (tag: $VERSION) — sequential to avoid OOM..."

  docker build -t "controller:${VERSION}" \
    -f "$OPERATOR/Dockerfile" "$OPERATOR"

  docker build -t "attestation-scheduler:${VERSION}" \
    -f "$OPERATOR/Dockerfile.scheduler" "$OPERATOR"

  docker build -t "central-verifier:${VERSION}" \
    -f "$OPERATOR/Dockerfile.central-verifier" "$OPERATOR"

  docker build -t "node-attestation-agent:${VERSION}" \
    -f "$OPERATOR/Dockerfile.node-attestation-agent" "$OPERATOR"

else
  log "SKIP_BUILD=true — skipping image builds."
fi

# ── 3. Load images into kind ──────────────────────────────────────────────────
log "Loading images into kind cluster '$CLUSTER'..."
for img in controller attestation-scheduler central-verifier node-attestation-agent; do
  kind load docker-image "${img}:${VERSION}" --name "$CLUSTER"
done

# ── 4. Apply CRDs (Helm does not update existing CRDs on upgrade) ─────────────
log "Applying CRDs..."
kubectl apply -f "$CHART/crds/"

# ── 5. Namespace ──────────────────────────────────────────────────────────────
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ── 6. Deploy ─────────────────────────────────────────────────────────────────
log "Deploying Helm chart (namespace: $NAMESPACE)..."
helm upgrade --install ai-platform "$CHART" \
  -f "$CHART/values-kind.yaml" \
  --set images.tag="$VERSION" \
  --set modules.keyReleaseGateway.enabled=false \
  --set modules.platformApi.enabled=false \
  --set modules.platformUi.enabled=false \
  --set modules.thesisBench.enabled=false \
  --namespace "$NAMESPACE" \
  --wait --timeout 5m

# ── 7. Status ─────────────────────────────────────────────────────────────────
log ""
log "=== Deployment status ==="
kubectl get pods -n "$NAMESPACE"

log ""
log "Article components are ready; see experiments/artifact/e2e-kind-positive.sh."
log "Build the offline verifier with: (cd operator && go build -o ../dist/verify-placement ./cmd/verify-placement)"
log "Done."
