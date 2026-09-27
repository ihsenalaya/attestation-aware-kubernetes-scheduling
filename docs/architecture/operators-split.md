# Operators Architecture — Component Split

## Components

The platform is split into multiple binaries to respect the single-responsibility principle and allow independent scaling and RBAC scoping.

### governance-operator

**Binary**: `operator/cmd/main.go`

Responsibility: reconcile all FinOps and AI governance CRDs.

Controllers hosted:
- `AIProviderReconciler` — manages AIProvider lifecycle
- `AISLAReconciler` — manages AISLA policy
- `AIBudgetPolicyReconciler` — manages AIBudgetPolicy
- `AIRoutingPolicyReconciler` — manages AIRoutingPolicy
- `ConfidentialInferencePolicyReconciler` — manages ConfidentialInferencePolicy
- `AttestationEvidenceReconciler` — manages AttestationEvidence lifecycle + expiry
- `AIRevocationPolicyReconciler` — manages AIRevocationPolicy

Admission webhooks hosted:
- `MutatingWebhook` — pod injection (RuntimeClass simulation, scheduling gate, scheduler name)
- `ValidatingWebhook` — policy validation, pod vs policy compliance

### attestation-scheduler

**Binary**: `operator/cmd/attestation-scheduler/main.go`

Responsibility: schedule pods that have `schedulerName: ai-attestation-scheduler`.

Functions:
- Pre-filter: require `AttestationEvidence` reference on pod
- Filter: node must have valid, non-revoked evidence matching pod requirements
- Score: prefer nodes with fresher evidence; prefer nodes with matching GPU identity
- Reserve: mint Ed25519 placement token; create `AIPlacementDecision` CR
- Bind: call Kubernetes Binding API to bind pod to node

Uses controller-runtime for the reconciliation loop watching `Pending` pods.

### key-release-gateway

**Binary**: `operator/cmd/key-release-gateway/main.go`

Responsibility: HTTP service for key material release requests.

Endpoints: `POST /v1/key-release`, `GET /healthz`, `GET /readyz`, `GET /metrics`

Validates:
- Placement token signature and expiry
- Evidence freshness and revocation status
- Pod UID, model digest, image digest, policy hash binding

### platform-api

**Binary**: `operator/cmd/platform-api/main.go`

Responsibility: REST API for the UI and external consumers. Read-only view of the platform state via the Kubernetes API server.

13 endpoints covering: overview stats, policies, evidence, placement decisions, key releases, audit chain, reports.

### platform-ui

**Source**: `platform-ui/`

React 18 + TypeScript SPA. Built with Vite, served as static assets by `nginxinc/nginx-unprivileged:1.27-alpine` (port 8080, no root required). Proxies `/api/` to `platform-api:8083`.

> **Implementation note**: `nginx:alpine` requires `CAP_CHOWN` for its cache directories. Since the pod security context drops all capabilities, the unprivileged nginx variant must be used.

### thesis-bench

**Binary**: `operator/cmd/thesis-bench/main.go`

Standalone binary for running the 14 attack scenarios + 6 baseline scenarios. Produces `results.json`, `results.csv`, `report.md`. Not deployed in production.

## RBAC model

Each component has a dedicated ServiceAccount with minimal permissions:

| Component | Key RBAC permissions |
|---|---|
| governance-operator | CRUD on all `aiops.imperium.io/v1alpha1` CRDs; read pods/nodes |
| attestation-scheduler | Read pods/nodes/attestation evidence; create bindings; create `AIPlacementDecision` |
| key-release-gateway | Read `AttestationEvidence`, `AIRevocationPolicy`, `AIKeyReleasePolicy` |
| platform-api | Read-only on all `aiops.imperium.io/v1alpha1` CRDs and pods |

## Communication paths

```
pods (workloads)
  → admission webhook → governance-operator
  → scheduling gate → attestation-scheduler
  → Kubernetes Binding API (direct)
  → key release request → key-release-gateway

UI → platform-api → Kubernetes API server (read-only)

audit chain → file/memory anchor backend (kind)
           → azure-blob backend (planned, production)
```

## No direct inter-component communication

Components do not call each other directly. All state flows through:
1. Kubernetes CRDs (the shared source of truth)
2. Pod annotations (for placement context)
3. The audit chain (append-only, written by each component independently)

This ensures that failure of any single component does not block reads by other components.
