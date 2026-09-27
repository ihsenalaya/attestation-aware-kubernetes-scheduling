# Key Release Protocol

## Goal

Protect confidential AI workloads by releasing keys only when:

- attestation evidence is present and verified;
- the evidence is not revoked;
- the placement decision is explicitly allowed;
- the placement token is valid;
- the key-release policy TTL window is still valid.

## Current implementation in this repository

- Policy CRD: `AIKeyReleasePolicy`
- Pure evaluation logic: `operator/internal/keyrelease/keyrelease.go`
- Gateway scaffold: `operator/cmd/key-release-gateway/main.go`
- Evidence sources: `AttestationEvidence`, `AIPlacementDecision`, `AIRevocationPolicy`

## Decision flow

1. A workload is selected by a `ConfidentialInferencePolicy`.
2. Attestation evidence is produced or referenced through `AttestationEvidence`.
3. A placement decision is evaluated and recorded in `AIPlacementDecision`.
4. Revocation rules can invalidate evidence or placement through `AIRevocationPolicy`.
5. `AIKeyReleasePolicy` evaluates whether release is allowed.
6. `key-release-gateway` returns `allow` or `deny` with an auditable reason.

## Simulated mode

In local `kind`, attestation/runtime signals may be simulated.

Requirements:

- simulation must stay explicit;
- simulated runtime classes must be labelled `ai.sovereign.io/simulated=true`;
- production mode must reject simulated runtime classes;
- logs, annotations and metrics must expose simulation status.

## Remaining work

- Bind gateway decisions to actual secret delivery or envelope decryption
- Verify signed placement tokens instead of digest-only scaffolding
- Emit append-only `AIEvidenceRecord` entries per key-release decision
- Add external signed checkpoints for audit-chain anchoring
