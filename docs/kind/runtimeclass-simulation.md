# RuntimeClass Simulation in kind

## Why simulation is needed

Kind clusters use the `runc` container runtime. They do not have:
- Intel TDX (Trust Domain Extensions) hardware
- AMD SEV-SNP (Secure Encrypted Virtualization) hardware
- `kata-qemu-tdx` or `kata-qemu-snp` container runtime handlers

The platform requires that confidential workloads declare a specific RuntimeClass. To test the full admission + scheduling + key-release flow in kind without TEE hardware, we create **simulated RuntimeClass objects** backed by `runc`.

## How it works

### Bootstrap step

On kind startup, `EnsureSimulatedRuntimeClasses()` in [operator/internal/webhook/bootstrap/runtimeclass.go](../../operator/internal/webhook/bootstrap/runtimeclass.go) creates:

| RuntimeClass name | Handler | Purpose |
|---|---|---|
| `simulated-kata-qemu-tdx` | `runc` | Simulates TDX workloads |
| `simulated-kata-qemu-snp` | `runc` | Simulates SEV-SNP workloads |

Both carry label `ai.sovereign.io/simulated: "true"`.

### Admission webhook mutation

When a pod matches a `ConfidentialInferencePolicy` and the environment is kind (`AIOPS_PLATFORM_MODE != production`):

1. Webhook reads `spec.allowedRuntimeClasses` from the policy
2. Maps real classes → simulated equivalents:
   - `kata-qemu-tdx` → `simulated-kata-qemu-tdx`
   - `kata-qemu-snp` → `simulated-kata-qemu-snp`
3. Sets `pod.spec.runtimeClassName` to the simulated class
4. Adds annotation `ai.sovereign.io/applied-runtime: simulated-kata-qemu-tdx` (or snp)
5. Adds annotation `ai.sovereign.io/simulated-execution: "true"`

### Production enforcement

In `AIOPS_PLATFORM_MODE=production`, the admission webhook **rejects** pods that have a simulated RuntimeClass. This is enforced in `validatePodAgainstPolicy()`.

## Labels and annotations on simulated pods

| Key | Value | Meaning |
|---|---|---|
| `ai.sovereign.io/applied-runtime` | `simulated-kata-qemu-tdx` | Actual RuntimeClass applied |
| `ai.sovereign.io/expected-runtime` | `kata-qemu-tdx` | What production would use |
| `ai.sovereign.io/simulated-execution` | `"true"` | Explicit simulated flag |
| `ai.sovereign.io/policy-hash` | SHA-256 hex | Policy version reference |

## Prometheus metrics

- `ai_simulated_runtimeclass_in_use` (gauge) — set to 1 while any simulated RuntimeClass pod is running
- `ai_simulated_evidence_in_use` (gauge) — set to 1 while any simulated evidence is in use

## Security boundary

Simulated RuntimeClass provides **functional equivalence** of the flow but **zero TEE security**. The runc handler has:
- No hardware memory encryption
- No attestation quotes
- No isolated execution environment

This is intentional and acknowledged. The simulation validates the orchestration logic, not the hardware security boundary.
