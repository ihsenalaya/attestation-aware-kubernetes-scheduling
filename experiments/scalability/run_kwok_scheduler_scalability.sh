#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# KWOK scheduler-only scalability harness.
#
# This is intentionally labelled scheduler-only: KWOK does not provide real TEE,
# real kubelet timing, or cloud placement. It is used only for large object-count
# pressure (100/500/1000 nodes and pods) without a paid AKS cluster.
set -uo pipefail

ENV_NAME="${ENV_NAME:-kwok-scheduler-only}"
OUT_DIR="${OUT_DIR:-results/raw/kwok}"
CSV="${CSV:-${OUT_DIR}/scalability_scheduler_only.csv}"
SIZES="${SIZES:-100 500 1000}"
mkdir -p "${OUT_DIR}"

echo "timestamp,env,nodes_count,pods_count,scheduled,makespan_ms,throughput_pods_per_s,status,reason,raw_log_path" > "${CSV}"

record_not_executed() {
  printf '%s,%s,%s,%s,0,0,0,NOT_EXECUTED,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "${ENV_NAME}" \
    "$1" \
    "$1" \
    "$2" \
    "$3" >> "${CSV}"
}

if ! command -v kwokctl >/dev/null 2>&1; then
  for size in ${SIZES}; do
    record_not_executed "${size}" "kwokctl_not_installed" ""
  done
  echo "KWOK not executed: kwokctl is not installed (CSV: ${CSV})"
  exit 0
fi

echo "KWOK executable detected, but this repository does not yet ship a KWOK cluster manifest."
echo "Writing NOT_EXECUTED rows rather than inventing scheduler-only results."
for size in ${SIZES}; do
  record_not_executed "${size}" "kwok_cluster_manifest_not_present" ""
done
exit 0
