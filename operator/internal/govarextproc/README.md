# GOV-AR Envoy trust boundary

The production path has two independent mutual-TLS identities. A governed
workload authenticates to Envoy, which emits a `SANITIZE_SET` XFCC record.
Envoy separately authenticates to `gov-ar-admission` with a client certificate
whose SPIFFE URI is allowlisted by the admission deployment. The processor
requires the XFCC `By` field to equal that authenticated gateway URI and
requires a SHA-256 certificate hash and workload URI before resolving the live
Pod, ServiceAccount, and operator-owned `AIWorkloadBinding`.

The approved Envoy client certificate is a trusted-gateway credential. A
process that possesses that private key can author a syntactically conforming
XFCC record; ext_proc cannot independently reconstruct the downstream TLS
transcript. Consequently the credential must be mounted only in the labeled
gateway Pod, never in application workloads. The production NetworkPolicy
admits ext_proc traffic only from that gateway selector, while mTLS remains the
cryptographic control. Credential possession and compliant `SANITIZE_SET`
configuration are part of the stated trusted computing base, not a property
proved by the ledger.

Route actuation is server-owned. Admission selects an `AIModel`; ext_proc then
loads its typed route, overwrites the cluster and authority routing headers,
rewrites the provider model/body or deployment path, updates content length,
and clears Envoy's route cache. Client-supplied route headers are overwritten.
Streaming and bodies over 1 MiB fail closed until authoritative streaming
settlement is implemented.

Governed clients must provide `max_output_tokens` as a positive exact JSON
integer. The processor rejects a client-supplied provider-native `max_tokens`
field and rewrites the canonical bound to `max_tokens` only for the OpenAI or
Azure OpenAI chat/completions wire paths that require it; Responses keeps
`max_output_tokens`. Fractional, negative, overflowed, duplicated, null, and
otherwise non-integral monetary bounds fail before admission. At this
checkpoint only the closed OpenAI and Azure OpenAI request/usage mappings are
executable. Anthropic, Vertex, Mistral, self-hosted, and custom providers stay
infeasible until authoritative provider-specific adapters and fixtures exist.
