# Article 1 manuscript

**Title:** *Attestation-Aware Scheduling for Verifiable AI Placement on SEV-SNP
Confidential Kubernetes Nodes*

This is the existing working manuscript from the project's Overleaf source,
including its anonymous review author line. It is not presented as an approved
final publication. The scientific claims, results, and bibliography are preserved
from that source. Two sentences in the related-work discussion have been
rephrased without changing their technical meaning; `main.pdf` is rebuilt from
the included files.

## Build

Use pdfLaTeX and BibTeX with the IEEEtran class and bibliography style. The source
also requires the `hyperref`, `booktabs`, `csvsimple`, `graphicx`, `amssymb`, and
`balance` packages, plus the standard LaTeX font and input encoding packages.

From this directory:

```bash
pdflatex -interaction=nonstopmode -halt-on-error main.tex
bibtex main
pdflatex -interaction=nonstopmode -halt-on-error main.tex
pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

For Overleaf, upload this directory, select `main.tex` as the main document, and
choose pdfLaTeX. The `figures/` directory contains all referenced figures, and
`references.bib` contains the 47 bibliography entries cited by the manuscript.

## Evidence

The manuscript evaluates node-level AMD SEV-SNP placement on AKS. Its principal
raw evidence is in `../results/raw/aks/`. The `tables/` directory contains the
claim/evidence and adversary/attack mappings, security matrix, scheduler RBAC
checks, and B4/B5 summaries referenced by the manuscript. Historical paths
beginning with `article1/` in the scientific source or copied tables refer to this
artifact's root; for example, `article1/results/raw/aks/` corresponds to
`../results/raw/aks/` when reading from this directory.

Local kind/KWOK runs are regression material. The manuscript does not claim
pod-level attestation, confidential GPU execution, Intel TDX evaluation, or
confidentiality of remote model services.
