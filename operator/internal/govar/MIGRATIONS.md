# GOV-AR ledger compatibility and migration

The integer ledger does not automatically import the earlier exploratory
`DOUBLE PRECISION` tables. Startup fails if any legacy floating-point layout
exists, even when empty. Reconcile those schemas outside the service, archive their
provenance, and start from a reviewed integer migration or a clean experimental
database.

Existing integer reservations must contain an immutable admission fingerprint,
candidate snapshot version, pricing version, and policy version. Rows missing
the fingerprint are rejected at startup; they must not be backfilled from a
current mutable catalog.

Schema v8 is the route-, cap-, complete-price-snapshot-, tamper-evident
transition-audit, durable-worker, and authoritative-calibration integer ledger
in `article3/infra/postgres/init.sql`. The companion `clean_migrate_v8.sh`
records before/after schema dumps, exact row
counts, reconciliation output, and SHA-256 checksums. It refuses every
nonempty ledger: there is no implicit float-to-integer monetary conversion.
Runtime bootstrap also refuses an empty pre-v3 or unversioned layout. Empty
exact v3 can migrate transactionally through v4, v5, v6, v7, and v8, but nonempty reservations or v1
cohorts are refused and preserved as read-only evidence. Runtime startup accepts
only the exact v7 or v8 layout identifier and required
safety columns, then recomputes tenant aggregates, active holds, carried debt,
audit credit, and reservation/outbox identity. Any discrepancy fails readiness
instead of being repaired from mutable state.

The v5-to-v6 migration also refuses nonempty reservations. Earlier versions did
not atomically commit audit-chain entries, so retroactive rows would be
fabricated provenance. Each v6 tenant chain covers effective reserve, dispatch,
settlement/correction, cancel, expiry, window rollover, budget adjustment, and
cohort-registration mutations in their serializable transaction. A dedicated
tenant sequence row is locked to allocate a contiguous monotone sequence.
PostgreSQL rejects update, delete, and truncate on the event table. The
`govar_runtime` role has only SELECT/INSERT on events; the migration owner owns
the protection trigger. Exported rows are independently verified by
`govaraudit.Verify` and `article3/tools/verify_govar_audit.py`. Production opens
the migrated layout without DDL, requires a distinct nonsuperuser login that
is a member of `govar_runtime`, executes `SET ROLE govar_runtime` on every
pooled connection, and rejects migration-owner credentials and direct audit
mutation privileges. `GOV_AR_SOFTWARE_SHA256` is mandatory in that production
path and is bound into each transition. The migration credential must never be
mounted into the admission service.

The additive v6-to-v7 migration requires the exact v6 layout and its active
append-only audit triggers. It creates only `govar_work_items`, its constraints,
indexes, and least-privilege grants, so existing monetary and audit rows remain
unchanged. Stop application writers while applying it and preserve the
wrapper's before/after evidence. Claims use `FOR UPDATE SKIP LOCKED` and the
database clock; renewals and completions are lease-owner compare-and-set
updates. A worker completion records execution disposition only. Monetary
release still requires the authoritative reservation transition, so a retry,
dead letter, stale owner, or unproven handler result cannot release liability.

The additive v7-to-v8 migration requires the exact v7 durable-worker layout,
or the exact v8 layout for idempotent replay. It adds six append-only evidence
tables and separate `govar_split_authority` and
`govar_calibration_producer` roles. An authority-signed registry and all of its
assignments are one transaction, so late split assignment is structurally
rejected. Producer observations are derived from an immutable pre-admission
assignment, the selected reservation, its authoritative-final settlement audit
commitment, and exact feature, price, cap/path/adapter, cohort, and software
regimes. The observation table has no selected/final/excluded flags and its
split constraint excludes train and frozen-test outcomes.

Artifacts record the exact finite-sample split-conformal rank bound. Drift
windows accept monitoring rows only and record a Wilson interval. Artifact,
drift, and policy-publication insertions append a tenant audit event in the same
database transaction; concurrent producers use transaction advisory locks and
immutable exact replay. Policy publication also binds the Kubernetes policy
UID, generation, and expected resourceVersion before the status-subresource
compare-and-set. Production uses the dedicated
`gov-ar-calibration-producer` binary; legacy ConfigMap rows remain explicitly
non-authoritative diagnostics.

The reviewed v8 wrappers also apply the additive
`article3/datasets/selected_feedback_postgres.sql` module. It adds two coherent
nullable columns to reservations plus three append-only evidence tables; it
does not rewrite existing monetary or calibration rows. An outcome-free
run/dataset/split/item/protocol/oracle/software/config binding is stored during
reserve. On effective delivery, `govar_runtime` derives the request, selected
model, deployment, provider-attempt, decision, dispatch, and authorization
identities and inserts the dispatch row inside the same serializable
transaction. Its privilege is INSERT-only on that table and it has no access
to evaluator commitments or observations. Same-run request/item/attempt
uniqueness conflicts and all insert failures roll back delivery, outbox, inbox,
and transition-audit state. The evaluator and verifier retain separate NOLOGIN
roles; no production Pod receives the migration-owner or optional fixture
issuer role.

Trusted checkpoints are required to detect deletion of an otherwise valid
chain suffix; the independent verifier supports them and regression fixtures
cover truncation, reordering, empty input, and wrong heads. Periodic checkpoint
publication is not yet implemented. PostgreSQL superuser/table-owner actions
remain outside the role-separation claim.

Daily, weekly, and monthly UTC windows are derived from the server clock.
Policy identity and budget are immutable within a window. At renewal,
unresolved holds persist; on-time provisional records gain a guard equal to
their provisional actual so guard plus residual remains the original
reservation until finality. Late actuals and positive corrections become
nonnegative carried debt. Historical downward corrections are audit-only and
never expand admission availability. Debt can change only through a separately
authorized, window-bound adjustment carrying a time-bounded HMAC proof from a
configured ledger authority. Frozen-cohort artifact hashes and the pre-outcome
registry digest require the same authority boundary. Provider retries, hedges,
and fallbacks are disabled: the attempt records `NO_PROVIDER_RETRY`, and a
second attempt fails closed because no separately reserving retry API exists.

Production mode requires PostgreSQL. `GOV_AR_DEV_IN_MEMORY=true` is explicitly
development-only and the Helm chart requires one replica in that mode.
