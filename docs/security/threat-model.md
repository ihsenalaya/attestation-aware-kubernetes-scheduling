# Threat Model

## What the system protects

- **Placement verification**: Only pods with valid, non-expired, non-revoked AttestationEvidence can be scheduled by the ai-attestation-scheduler.
- **Key release gating**: The key-release-gateway refuses requests unless placement token, evidence freshness, pod UID, model digest, image digest, and policy hash all match.
- **Bounded revocation**: AIRevocationPolicy blocks new key releases immediately upon activation with a TTL.
- **Audit chain integrity**: The audit chain's hash linkage detects any modification to historical records.
- **Checkpoint anchoring**: Signed Ed25519 checkpoints bound the falsification window. Rewrites prior to the last anchor are detected.
- **Simulated mode visibility**: In kind, simulated TEE/GPU/RuntimeClass is always explicitly labelled — it cannot be silently treated as real.

## What the system does NOT protect (honest limitations)

- **cluster-admin access**: A cluster-admin can bypass all validation webhooks, delete or modify etcd entries directly, and rewrite audit records before the next checkpoint. The checkpoint mechanism bounds but does not eliminate this risk.
- **etcd direct access**: Direct etcd access bypasses all API server admission controls. Mitigation: restrict etcd access, use etcd encryption at rest, monitor etcd access.
- **Node kernel vulnerabilities**: The system relies on the node OS and containerd for isolation. Kernel exploits can bypass TEE isolation at the host level.
- **GPU side-channel attacks**: Confidential GPU attestation does not cover all GPU side channels. This is a research-stage protection.
- **Real GPU confidential in kind**: GPU confidential computing is NOT validated in kind. All kind GPU tests are explicitly SIMULATED.
- **Supply chain attacks**: The system attests workload images and models via digest pinning but does not audit the build pipeline itself.
- **Distributed key management**: The memory and file key backends are not suitable for production. Production requires Vault, Trustee, or Azure Key Vault (planned).
- **Network-level attacks**: NetworkPolicies are enforced in production/AKS. They do not cover encrypted traffic inspection or BGP hijacking.
- **Metadata leakage**: Pod annotations and CRD status fields may expose policy structure to cluster-level readers with RBAC list access.

## Threat actors modelled

1. **Compromised workload (low-privilege)**: Cannot bypass admission webhook. Cannot obtain a key without a valid placement token. Detected by audit chain.
2. **Malicious operator (no cluster-admin)**: Cannot create ConfidentialInferencePolicy violations that pass the webhook. Cannot forge placement tokens without the scheduler's signing key.
3. **Cluster admin with malicious intent**: Can bypass webhooks and etcd immutability. Bounded by checkpoint anchoring — any rewrite before the last checkpoint is detectable after the fact.
4. **External attacker**: No public surfaces. Platform API and UI are ClusterIP + internal ingress. No LoadBalancer exposed.

## Simulated mode threat boundary

In simulated-kind mode, the TEE is not real. The system is used for:
- Functional testing of the full flow
- Attack scenario validation against non-hardware threats
- Thesis experimental reproducibility

Simulated mode MUST NOT be deployed in production. The admission webhook enforces this: simulated RuntimeClass objects are refused in `AIOPS_PLATFORM_MODE=production`.
