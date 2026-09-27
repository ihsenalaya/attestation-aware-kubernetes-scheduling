# Public artifact redactions

The public archive contains a sanitized export of the experimental data.

- Azure subscription identifiers in resource paths are replaced with
  `00000000-0000-0000-0000-000000000000`.
- Azure kubelet managed-identity client identifiers in node metadata are
  replaced with the same zero UUID. These identifiers are specific to the
  experimental account and are not needed to reproduce the measurements.
- Raw MAA attestation tokens are replaced with redaction placeholders. The
  archive therefore cannot support independent cryptographic replay of the
  original MAA verification from those tokens. A new real-hardware run is
  required to obtain fresh attestation evidence for that verification.

Experimental measurements, node names, pod/evidence UIDs, hashes, signed
placement tokens, and scheduler public verification keys are retained.
The node names and UIDs allow records from the experiment to be correlated;
they are not Azure authentication credentials. The shared public MAA endpoint
is also retained because it is part of the documented configuration.

The export was checked for private-key material, common credential formats,
bearer tokens, embedded kubeconfig credentials, local account paths, and
remaining Azure account identifiers. No credential was identified by these
checks. This is a description of the checks performed, not a guarantee that
pattern matching can recognize every possible secret format.
