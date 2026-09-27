# GitHub → Zenodo publication

The dedicated repository is:
https://github.com/ihsenalaya/attestation-aware-kubernetes-scheduling

The `v1.0.0` artifact is prepared as a draft release. Preparation does not issue
a DOI. The manuscript in this artifact is a working anonymous review version;
replace it and regenerate the package if a different final manuscript is intended.

1. Inspect the prepared files and validation report. If the manuscript changes,
   rebuild its PDF, update `PROVENANCE.json`, regenerate `SHA256SUMS` and the ZIP
   with `python3 tools/package_artifact.py`, commit, and update the draft release.
2. Make the dedicated repository public when ready to publish.
3. Open https://zenodo.org/account/settings/github/, select **Sync now**, and
   enable **ihsenalaya/attestation-aware-kubernetes-scheduling** specifically.
   Linking a GitHub account or enabling `article2` does not enable this repository.
4. Publish the prepared GitHub release `v1.0.0` only after enabling this repository.
   Zenodo ingests the tagged repository snapshot, so the tag must point to the
   reviewed artifact tree. Do not rely solely on the attached ZIP.
5. Wait for processing and inspect the resulting Zenodo record: title, author,
   version, license, files and DOI. Add the assigned DOI to the citation metadata
   in a subsequent commit. No placeholder DOI is included here.

The `.zenodo.json` file takes precedence over `CITATION.cff` for Zenodo's GitHub
integration. Both describe the software artifact, while the included manuscript
keeps its own title and review status.

For a manual Zenodo deposit, upload the ZIP created by `tools/package_artifact.py`
and enter the metadata from `.zenodo.json`; uploading a ZIP alone does not import
its internal metadata automatically. Choose one publication route to avoid
creating duplicate records unintentionally.

Official documentation (consulted 2026-09-27):
- https://help.zenodo.org/docs/github/enable-repository/
- https://help.zenodo.org/docs/github/archive-software/github-upload/
- https://help.zenodo.org/docs/github/describe-software/zenodo-json/
