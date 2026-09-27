# Revocation Protocol

## Overview

Revocation stops key releases for workloads whose attestation evidence is no longer trusted, without requiring pod termination. The `AIRevocationPolicy` CRD is the control plane object for this.

## AIRevocationPolicy fields

- `spec.targetEvidenceRef` — reference to the `AttestationEvidence` to revoke
- `spec.reason` — human-readable reason (e.g. `"TEE firmware vulnerability CVE-2024-XXXX"`)
- `spec.active` — boolean; set to `true` to activate revocation immediately
- `spec.ttlSeconds` — how long the revocation remains enforced (0 = permanent)

## Revocation flow

1. Operator or automated system creates / patches `AIRevocationPolicy` with `active: true`
2. The key-release-gateway checks `RevocationActive` on every request
3. If active and TTL not expired → response: `Denied`, reason `RevocationActive`
4. `RevocationTriggered` event is appended to audit chain
5. Running pods continue running (revocation does not kill pods)
6. New key release requests are denied until revocation expires or is deactivated

## Pod impact

Revocation affects future key releases only. Pods that already have released keys in memory are not automatically affected. To enforce eviction of running pods, use standard Kubernetes mechanisms (cordon + drain, or eviction API) separately.

## Audit trail

Every key release denial due to revocation appends to the audit chain:
- `EventType: KeyRefused`
- `Reason: RevocationActive`
- `PolicyHash`, `PodUID`, `NodeName` recorded for forensics

## Automatic expiry

When `ttlSeconds > 0`, the revocation is treated as expired after `creationTimestamp + ttlSeconds`. The key-release-gateway enforces this in `Evaluate()` — no separate controller required.

## Emergency revocation

For immediate cluster-wide response:
```bash
# Activate revocation for a specific evidence reference
kubectl patch airevocationpolicy <name> -n <ns> \
  --type=merge -p '{"spec":{"active":true}}'

# Check revocation status
kubectl get airevocationpolicy -A

# Deactivate
kubectl patch airevocationpolicy <name> -n <ns> \
  --type=merge -p '{"spec":{"active":false}}'
```

## Limitations

- Revocation is per-`AttestationEvidence` reference, not per-node or per-policy
- If multiple pods share the same `AttestationEvidence`, all are affected
- There is no broadcast mechanism to invalidate in-memory keys already held by running pods
