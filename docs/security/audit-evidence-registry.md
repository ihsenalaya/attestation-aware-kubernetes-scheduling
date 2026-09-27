# Audit Evidence Registry

## Overview

The audit evidence registry is the live set of `AttestationEvidence` CRDs in the cluster, combined with the append-only audit chain written by the platform components. Together they form the evidentiary record for all policy decisions.

## AttestationEvidence CRD

Each pod that requests confidential scheduling must have a corresponding `AttestationEvidence` object in the same namespace.

Key fields:
- `spec.evidenceHash` — SHA-256 hex of the raw TEE report bytes
- `spec.evidenceType` — `tdx-dcap` | `snp-ravl` | `simulated`
- `spec.verified` — boolean set by the attestation verifier controller
- `spec.expiresAt` — RFC 3339 timestamp; evidence rejected after this
- `spec.gpuIdentity` — GPU attestation reference (optional)

The admission webhook reads these fields when validating pods against a `ConfidentialInferencePolicy`.

## How evidence is created

1. In kind/simulated mode: evidence objects are created with `evidenceType: simulated` and `verified: true` by the bootstrap job. This is explicitly flagged in every downstream decision.
2. In production: an external attestation verifier (DaaS, Intel DCAP, AMD SEV) creates and signs the `AttestationEvidence` object before pod submission. The platform does not generate real TEE quotes.

## Evidence lifecycle

```
Created → Verified → [Referenced by scheduler] → Expired
                                              ↓
                                         Revoked (via AIRevocationPolicy)
```

Evidence expiry is enforced at:
- Admission webhook (rejects pods referencing expired evidence)
- Key release gateway (`maxEvidenceAgeSecs` in `AIKeyReleasePolicy`)
- Scheduler filter (rejects nodes without valid evidence on the pod's evidence reference)

## Audit chain events

The following events are appended to the audit chain for evidence:
- `AttestationVerified` — when evidence is accepted
- `AttestationExpired` — when evidence TTL is exceeded
- `RevocationTriggered` — when an `AIRevocationPolicy` is activated

## Querying the registry

```bash
# All evidence in a namespace
kubectl get attestationevidence -n <namespace>

# Evidence for a specific pod
kubectl get attestationevidence -n <namespace> -l pod-uid=<uid>

# Expired evidence
kubectl get attestationevidence -A --field-selector='spec.verified=true'
```

Via the platform API:
```
GET /api/v1/attestation-evidence?namespace=<ns>
GET /api/v1/attestation-evidence/<name>?namespace=<ns>
```
