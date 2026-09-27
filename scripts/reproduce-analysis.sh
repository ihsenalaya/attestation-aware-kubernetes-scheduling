#!/usr/bin/env bash
# Recompute supported tables/figures without modifying archived raw evidence.
set -euo pipefail
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-${ARTIFACT_ROOT}/results/reproduced}"
mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
cd "${ARTIFACT_ROOT}"
PYTHON="${PYTHON:-python3}"

"${PYTHON}" scripts/analyze_all.py --raw results/raw/aks \
  --tables "${OUTPUT_DIR}/tables" --figures "${OUTPUT_DIR}/figures" --env aks-real-sevsnp
"${PYTHON}" scripts/analyze_ablation.py --output "${OUTPUT_DIR}/tables/ablation.csv"
"${PYTHON}" scripts/analyze_scheduler_security.py --output "${OUTPUT_DIR}/tables/scheduler_security.csv"
"${PYTHON}" scripts/analyze_scalability.py --output "${OUTPUT_DIR}/tables/scalability.csv"
"${PYTHON}" scripts/analyze_performance.py --input results/raw/aks/performance_high_resolution.csv \
  --output "${OUTPUT_DIR}/tables/performance_high_resolution_aks.csv" --env aks-real-sevsnp
"${PYTHON}" scripts/make_figures.py --figures "${OUTPUT_DIR}/figures" \
  --security "${OUTPUT_DIR}/tables/security.csv" \
  --scalability "${OUTPUT_DIR}/tables/scalability.csv" \
  --ablation "${OUTPUT_DIR}/tables/ablation.csv"
"${PYTHON}" scripts/update_q1_final_artifacts.py --raw results/raw/aks \
  --tables "${OUTPUT_DIR}/tables" --paper-tables "${OUTPUT_DIR}/paper-tables" \
  --figures "${OUTPUT_DIR}/figures"
echo "Regenerated analysis: ${OUTPUT_DIR}"
