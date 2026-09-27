# GPU Confidential Computing — Future Validation

## Current status

GPU confidential computing is **DISABLED** in all deployments (`global.gpu.mode: disabled`).

This is an explicit, intentional decision documented here so future contributors understand the gap and the validation path.

## What is planned

NVIDIA Confidential Computing (H100 with CC mode enabled) on AKS requires:
- AKS node pool with `Standard_NCC` or `NC` SKU with H100 NVL (CC-capable)
- NVIDIA GPU Operator with confidential computing mode
- NVIDIA attestation service integration (`attestationServiceURL` in GPU operator config)
- The nvidia/gpu-feature-discovery labels: `nvidia.com/cc.capable: "true"`

AMD SEV-SNP + NVIDIA CC attestation can produce a combined TEE+GPU attestation report. The platform's `AttestationEvidence` CRD has a `gpuIdentity` field reserved for this.

## Why it is not validated yet

1. No access to H100 NVL with CC mode in the current test infrastructure
2. NVIDIA CC attestation SDK has evolving APIs (as of 2025-Q4)
3. Kind cannot simulate GPU attestation at the hardware level — any kind GPU test would be functionally identical to the simulated TEE test and add no real coverage
4. Thesis scope is bounded to TEE orchestration logic, not GPU attestation protocol

## How to validate in the future

1. Provision an AKS node pool with H100 NVL (SKU: `Standard_NCC40ads_H100_v5`)
2. Deploy NVIDIA GPU Operator with `confidentialComputingMode: "CC_MODE_DEVTOOLS"` initially
3. Create a test `AttestationEvidence` with `evidenceType: nvidia-cc` and the GPU's attestation report
4. Deploy a test pod with `runtimeClassName: nvidia-gpu-confidential`
5. Verify that the admission webhook accepts it and the scheduler places it on the CC-capable node
6. Verify that the key release gateway accepts the combined TEE+GPU evidence
7. Upgrade to `confidentialComputingMode: "CC_MODE_ON"` for production

## What changes in the code

When real GPU validation is done, update:
- `operator/internal/webhook/podinjector/confidential.go` — add GPU attestation check in `validatePodAgainstPolicy()`
- `operator/internal/keyrelease/keyrelease.go` — add `gpuAttestationVerified` field to `Request`
- `operator/api/v1alpha1/confidential_platform_types.go` — update `AttestationEvidence.spec.gpuIdentity` field with real schema
- `charts/ai-confidential-governance-platform/values-aks-private.yaml` — set `gpu.mode: nvidia-cc`

## GPU in simulated mode (kind)

In kind, GPU is completely disabled:
```yaml
gpu:
  enabled: false
  mode: disabled
```

The thesis scenarios that mention "GPU" (B4, B5, attack05FakeGPULabel) use label-based simulation only. They test that the scheduler correctly reads and validates `nvidia.com/gpu: "1"` node labels, not that a real GPU is present.
