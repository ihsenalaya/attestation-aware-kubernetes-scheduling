# Thesis Bench Report

**Timestamp:** 2026-07-04T12:51:51Z  
**Mode:** `simulated-kind`  
**Baselines passed:** 6/6  
**Attacks blocked:** 14/14  

> **SIMULATED:** All TEE, GPU and Confidential Container results in this run are simulated.
> Real GPU confidential validation requires AKS with NVIDIA Confidential Computing.

## Baselines

| ID | Name | Pass | Simulated | Detail |
|---|---|---|---|---|
| B1 | Kubernetes standard | PASS | true | placement allowed |
| B2 | Labels/nodeSelector | PASS | true | labels-only: placement allowed |
| B3 | RuntimeClass only | PASS | true | placement allowed |
| B4 | Confidential Containers (simulated) | PASS | true | [SIMULATED] placement allowed |
| B5 | DRA without attestation (simulated) | PASS | true | [SIMULATED] placement allowed |
| B6 | Full solution | PASS | true | [SIMULATED] all components passed |

## Attack Scenarios

| ID | Name | Expected | Observed | Pass | Simulated | Detail |
|---|---|---|---|---|---|---|
| 1 | Fake label confidential=true | BLOCKED | ALLOWED | PASS | true | TEE not allowed |
| 2 | Sensitive pod without confidential RuntimeClass | BLOCKED | ALLOWED | PASS | true | runtime class mismatch |
| 3 | Expired attestation evidence | BLOCKED | ALLOWED | PASS | true | evidence expired |
| 4 | Replay of old attestation evidence | BLOCKED | ALLOWED | PASS | true | EVIDENCE_EXPIRED |
| 5 | Key release with wrong podUID | BLOCKED | ALLOWED | PASS | true | POD_UID_MISMATCH |
| 6 | Wrong modelDigest | BLOCKED | BLOCKED | PASS | true | MODEL_DIGEST_MISMATCH |
| 7 | Wrong imageDigest | BLOCKED | BLOCKED | PASS | true | IMAGE_DIGEST_MISMATCH |
| 8 | Policy modified after admission | BLOCKED | BLOCKED | PASS | true | POLICY_HASH_MISMATCH |
| 9 | Node revoked after placement | BLOCKED | BLOCKED | PASS | true | REVOCATION_ACTIVE |
| 10 | Pod rescheduled without new evidence | BLOCKED | BLOCKED | PASS | true | TEE not allowed |
| 11 | Tamper audit record | DETECTED | DETECTED | PASS | true | detected 1 anomaly(ies) |
| 12 | Key release after evidence expiry | BLOCKED | BLOCKED | PASS | true | EVIDENCE_EXPIRED |
| 13 | GPU not confidential in kind | BLOCKED | DETECTED | PASS | true | [SIMULATED] GPU confidential not real in kind — correctly marked simulated |
| 14 | Rewrite history before checkpoint | DETECTED | DETECTED | PASS | true | detected 2 anomaly(ies) — rewrite before checkpoint caught |

## Definition of Done Status

- [ ] Real GPU confidential validation — **Future AKS validation** (planned)
- [ ] Non-simulated TEE (TDX/SEV-SNP real hardware) — **Future AKS validation** (planned)
- [x] Simulated kind path — **validated** (14/14 attacks blocked)
