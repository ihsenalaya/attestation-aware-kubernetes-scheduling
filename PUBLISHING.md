# GitHub → Zenodo publication

Repository: https://github.com/ihsenalaya/attestation-aware-kubernetes-scheduling

Version 1.0.1 is the software/data artifact. It includes the implementation,
deployment configuration, experiment scripts, archived measurements and reference
results. The article manuscript is distributed separately.

For subsequent updates:

1. Update the files and validation/provenance records, then increment the version
   in `.zenodo.json`, `CITATION.cff`, the README and release notes.
2. Run the relevant checks. Regenerate `SHA256SUMS` and the ZIP with
   `python3 tools/package_artifact.py --output /path/outside/repository/artifact.zip`.
3. Commit the reviewed artifact tree and create a new version tag. Keep earlier
   published tags associated with their original contents.
4. Ensure this repository is enabled in https://zenodo.org/account/settings/github/
   before publishing the GitHub release. Zenodo ingests the tagged repository
   snapshot; attaching a different ZIP alone does not change that snapshot.
5. Inspect the resulting Zenodo record, version, files and DOI after processing.
   Each new archived version has its own DOI, while the concept DOI groups the
   versions. Changing a GitHub tag does not replace a previously archived record.

Zenodo uses `.zenodo.json` in preference to `CITATION.cff` when both are present.
Both files describe the software/data artifact. No fabricated DOI is included.

For a manual deposit, upload the ZIP and enter the metadata from `.zenodo.json`;
metadata inside a ZIP is not imported automatically. Use one publication route
to avoid unintentionally creating duplicate records.

Official documentation:
- https://help.zenodo.org/docs/github/enable-repository/
- https://help.zenodo.org/docs/github/archive-software/github-upload/
- https://help.zenodo.org/docs/github/describe-software/zenodo-json/
- https://support.zenodo.org/help/en-gb/1-upload-deposit/97-what-is-doi-versioning
