#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# Fine-grained ablation L0-L5 for Article 1.
set -uo pipefail

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
BASE_CSV="${OUT_DIR}/ablation.csv"
CSV="${CSV:-${OUT_DIR}/ablation_fine.csv}"
mkdir -p "${OUT_DIR}"

echo "timestamp,env,layer,configuration,control_removed,attack_id,expected,actual,blocked,status,reason,raw_log_path" > "${CSV}"

record() {
  printf '%s,%s,%s,%s,%s,%s,BLOCKED,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "${ENV_NAME}" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" >> "${CSV}"
}

if kubectl get nodes >/dev/null 2>&1; then
  ENV_NAME="${ENV_NAME}" OUT_DIR="${OUT_DIR}" CSV="${BASE_CSV}" \
    bash experiments/harness/measure_ablation.sh >/dev/null 2>&1
  if [ -f "${BASE_CSV}" ]; then
    python3 - "${BASE_CSV}" "${CSV}" <<'PY'
import csv, datetime, sys
src, dst = sys.argv[1], sys.argv[2]
ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
mapping = {
    "L0": ("L0", "no-enforcement", "all"),
    "L1": ("L1", "admission-only", "scheduler-prebind-token"),
    "L2": ("L2", "scheduler-filter-prebind", "token-offline-verify"),
    "L3": ("L4", "token-verify", "none"),
}
with open(src, newline="", encoding="utf-8") as f, open(dst, "a", newline="", encoding="utf-8") as out:
    reader = csv.DictReader(f)
    writer = csv.writer(out)
    for row in reader:
        layer = row.get("layer", "")
        mapped = mapping.get(layer)
        if not mapped:
            continue
        out_layer, config, removed = mapped
        actual = row.get("actual", "")
        blocked = row.get("blocked", "")
        status = "EXECUTED" if actual not in ("REQUIRES_BUILD_FLAG", "REQUIRES_E2E") else "NOT_EXECUTED"
        writer.writerow([
            ts,
            row.get("env", ""),
            out_layer,
            config,
            removed,
            row.get("attack_id", ""),
            "BLOCKED",
            actual,
            blocked,
            status,
            row.get("note", ""),
            row.get("raw_log_path", ""),
        ])
PY
  fi
else
  record L0 no-enforcement all A1 NOT_EXECUTED no NOT_EXECUTED cluster_unreachable ""
  record L1 admission-only scheduler-prebind-token A5 NOT_EXECUTED no NOT_EXECUTED cluster_unreachable ""
fi

record L3 prebind-only filter-token A7 NOT_EXECUTED no NOT_EXECUTED requires_scheduler_build_flag_disable_filter ""
record L5 full-system none A1 NOT_EXECUTED no NOT_EXECUTED use_security_campaign_A1_A11_for_full_layer ""

echo "ablation fine written: ${CSV}"
