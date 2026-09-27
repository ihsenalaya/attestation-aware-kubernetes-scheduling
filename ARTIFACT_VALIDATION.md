# Artifact validation — 2026-09-27

This report describes preparation-time checks on version 1.0.0. It is not an
independent replication of the hardware experiments or a peer review.

## Original archive findings and repairs

The initial ZIP passed CRC checks. Its 1,247 files matched the source folder;
three empty `.gitkeep` files were absent from the ZIP. It was not ready to run as
packaged: scripts retained the old monorepo paths, some required scripts and
Terraform configuration were missing, documentation understated Go requirements,
and internal editorial notes remained. The supplied manuscript was older than
the current local version. Metadata in the original upstream repository also
referred to a different article.

The standalone artifact repairs the paths, includes the required scripts and
configuration, supplies pinned analysis dependencies, removes internal editorial
status generation, and keeps derived analysis output separate from raw data.
AKS values disable simulated evidence; local deployment includes the verifier
and node agent. The current manuscript was imported and its PDF rebuilt. Two
reviewer-directed sentences were rephrased without changing technical meaning.
`PROVENANCE.json` records the source snapshot and file hashes. The original
archive and source working tree are preserved.

## Checks completed

| Check | Outcome and scope |
| --- | --- |
| Go build | `go build ./...` passed with Go 1.27.1 |
| Go tests | `go test ./...` passed with envtest Kubernetes 1.31.0 assets; includes controller integration tests; guarded real-kind E2E suite not run |
| Helm | `helm lint` and rendering of default, kind and AKS configurations passed |
| Shell/Python | Bash and Python syntax checks passed; ShellCheck at error severity passed |
| Terraform | `terraform fmt -check` passed; no plan/apply executed |
| Offline analysis | `scripts/reproduce-analysis.sh` ran successfully from outside the artifact directory and generated 10 CSV files, 9 PDF figures and a quality JSON in a temporary output directory |
| Manuscript | pdfLaTeX, BibTeX and two subsequent pdfLaTeX passes succeeded; 12 pages; no undefined references/citations; minor font/box warnings remain |
| Privacy review | Common credential/private-key patterns, text/PDF contents and account identifiers checked; no credential identified; identifiers redacted as documented |

To run the controller integration tests in a fresh environment:

```bash
cd operator
make envtest
export KUBEBUILDER_ASSETS="$(bin/setup-envtest-latest use 1.31.0 --bin-dir ./bin -p path)"
go test ./...
```

This setup downloads the envtest binaries. During preparation, already-cached
1.31.0 binaries were used and Go dependencies were available locally.

## Evidence consistency and limits

The core security CSV contains 330/330 blocked rows (A1–A10 including A5b);
A11 adds one unsupported-GPU-evidence negative control. B4's external window has
median 1104.5 ms and maximum 1483 ms. The 150 successful latency measurements
(30 per baseline) agree with the current manuscript's reported medians. These
are checks of the archived data and reported arithmetic, not new measurements.

The B5 external-window value is explicitly zero in the harness because its path
has no gate-removal event. This does not measure a zero PreBind-to-Bind interval.

Some historical `raw_log_path` targets are unavailable in both the original ZIP
and the current source workspace: `.idb.log` referenced by six identity-binding
rows, `.sched-1.log` through `.sched-30.log` referenced by scheduling CSVs, and
detailed logs referenced by five ablation rows. No replacement logs were invented.
Only two of the 15 top-level AKS CSVs have a `seed` column, so the package does not
support a universal per-run seed-provenance claim. Raw CSVs remain available.

The paper's expanded scheduler-security table contains 30 probes; the main raw
scheduler-security CSV/manuscript subset contains 12 probes. These are distinct
archived views, not a claim that all rows occur in that one raw CSV.

Original MAA tokens were already redacted; their historical signatures cannot be
replayed. A fresh hardware run is required to obtain fresh verifiable MAA tokens.
Public placement-token evidence and verification keys are retained. See
`docs/public-artifact-redaction.md` for the precise redactions.

No AKS/kind hardware campaign, Terraform apply, container-image rebuild or
bibliography-authenticity audit was performed. Regenerated outputs are not
claimed byte-identical to every historical table or figure. The working
manuscript remains anonymous and is not asserted to be an accepted final paper.

Zenodo publication is a separate step described in `PUBLISHING.md`; no DOI was
issued by these checks.
