# Audit Chain Anchoring

## Purpose

The audit chain is an in-process append-only log with SHA-256 hash linkage between batches. Anchoring is the process of periodically writing a signed checkpoint to an external backend, bounding the window in which a privileged actor could rewrite history without detection.

## Chain structure

Each `BatchRecord` contains:
- `index` — monotonically increasing batch number
- `previousHash` — SHA-256 of the previous batch's JSON
- `entries` — slice of `AuditEntry` structs (one per event)
- `hash` — SHA-256 of `JSON(index + previousHash + entries)`

Verification traverses all batches and checks that each `previousHash` matches the `hash` of the preceding batch.

## Checkpoint structure

A `Checkpoint` contains:
- `recordIndex` — the batch index at checkpoint time
- `recordHash` — the `hash` of that batch
- `timestamp` — UTC time of checkpoint creation
- `signerID` — identifies which component signed (e.g. `platform-api`)
- `signature` — Ed25519 signature over `JSON(recordIndex + recordHash + timestamp + signerID)`

Checkpoints are verified using the same Ed25519 public key used for placement token verification.

## Anchoring backends

| Backend | Mode | Status |
|---|---|---|
| `MemoryAnchorBackend` | kind / testing | Available — not persistent across restarts |
| `FileAnchorBackend` | kind / single-node | Available — persists to `filePath` (see values-kind.yaml) |
| `AzureBlobAnchorBackend` | AKS production | PLANNED — returns "PLANNED" error |
| `S3AnchorBackend` | general cloud | PLANNED — returns "PLANNED" error |

In kind, the file backend is configured to write to `/data/audit/checkpoints.jsonl`.

## Checkpoint frequency

Checkpoints are created:
- On every `platform-api` reconciliation loop (configurable interval)
- On clean shutdown of any component that holds the chain
- On detection of audit anomalies (chain breaks or tampering)

The tighter the checkpoint interval, the smaller the rewrite window. For thesis purposes, checkpoints are triggered every 60 seconds.

## Detection guarantee

Given a checkpoint at batch N and the current chain head at batch M (M >= N):
- Any modification to batches 0..N is detectable by `VerifyChainAgainstCheckpoint`
- Modifications to batches N+1..M are detectable by `VerifyChain` if no subsequent checkpoint has been written over the tampered region

**Attack 14 (rewrite before checkpoint)** is the adversarial scenario for this mechanism. The thesis bench validates that rewrites prior to the most recent checkpoint produce a detected anomaly.

## Known limitations

- Memory backend loses all checkpoints on pod restart
- File backend is not HA (single-node only)
- Azure/S3 backends are planned — production deployments using kind values are NOT secure
- A cluster-admin with write access to the checkpoint file can overwrite both chain and checkpoints simultaneously; this requires out-of-band detection (monitoring, SIEM)
