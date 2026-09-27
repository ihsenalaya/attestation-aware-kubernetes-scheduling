# Version Matrix

## Effective versions observed in the current repository

| Layer | Source | Value | Notes |
|---|---|---|---|
| API group | `operateur/PROJECT` | `aiops.imperium.io/v1alpha1` | Kept as-is for compatibility. |
| Go module | `operateur/go.mod` | `go 1.21` | Effective compiler target in code. |
| Kubernetes Go deps | `operateur/go.mod` | `k8s.io/* v0.29.0` | Aligns with Kubernetes `1.29`. |
| controller-runtime | `operateur/go.mod` | `v0.17.0` | Kubebuilder/controller runtime stack. |
| Envtest assets | `operateur/Makefile` | `1.31.0` | Test control-plane version. |
| Kind target | `operateur/README.md` | `kind 0.31 / Kubernetes 1.35` | Documentation target, not enforced by code. |
| Runtime minimum | `operateur/README.md` | `Kubernetes >= 1.29` | Matches dependency floor. |
| Kubebuilder layout | `operateur/PROJECT` | `go.kubebuilder.io/v4` | Current scaffold. |

## Compatibility stance

- The repository currently compiles against Kubernetes `1.29` libraries.
- Local test assets already target Kubernetes `1.31`.
- The broader platform target in the roadmap is higher, but the codebase should keep `1.29+` compatibility unless a later phase explicitly raises the floor.
- Future confidential GPU / DRA work should document a separate track because real DRA GA requirements exceed the current dependency floor.

## Gaps to track

- `operateur/README.md` mentions `Go 1.25`, while `operateur/go.mod` is `1.21`.
- `operateur/README.md` mentions a newer kind/Kubernetes pairing than the compile target.
- Before enabling real scheduler-plugin or DRA-dependent features, this matrix must be extended with exact Kubernetes minor requirements and test coverage.
