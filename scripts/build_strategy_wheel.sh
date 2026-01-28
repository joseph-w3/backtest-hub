#!/usr/bin/env bash
set -euo pipefail

version=""
output_dir="/opt/backtest/strategy_artifacts/wheels"
python_bin="${UV_PROJECT_ENVIRONMENT:-/usr/local/quantvenv}/bin/python"

usage() {
  cat <<'EOF'
Usage: scripts/build_strategy_wheel.sh [--version VERSION] [--output-dir PATH]

Build a minimal wheel containing only the strategies package.
Default version: YYYY.MM.DD.N (UTC), auto-incremented from output-dir wheels.
EOF
}

require_value() {
  local opt="$1"
  local val="${2:-}"
  if [[ -z "${val}" ]]; then
    echo "${opt} requires a value" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      require_value "--version" "${2:-}"
      version="$2"
      shift 2
      ;;
    --output-dir)
      require_value "--output-dir" "${2:-}"
      output_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v uv >/dev/null 2>&1; then
  echo "Missing uv. Please install uv first." >&2
  exit 1
fi

uv pip install --python "${python_bin}" build setuptools wheel >/dev/null 2>&1 || {
  echo "Failed to install build tooling into ${python_bin}." >&2
  exit 1
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
strategy_src="${repo_root}/strategies"

if [[ ! -d "${strategy_src}" ]]; then
  echo "strategies directory not found at ${strategy_src}" >&2
  exit 1
fi

if [[ -z "${version}" ]]; then
  year="$(date -u +%Y)"
  month="$(date -u +%m)"
  day="$(date -u +%d)"
  max_seq=0
  if [[ -d "${output_dir}" ]]; then
    shopt -s nullglob
    for wheel in "${output_dir}"/strategies-*.whl; do
      filename="$(basename "${wheel}")"
      wheel_version="${filename#strategies-}"
      wheel_version="${wheel_version%-py3-none-any.whl}"
      IFS='.' read -r v_year v_month v_day v_seq extra <<<"${wheel_version}"
      if [[ -n "${extra:-}" ]]; then
        continue
      fi
      if [[ "${v_year}" =~ ^[0-9]+$ && "${v_month}" =~ ^[0-9]+$ && "${v_day}" =~ ^[0-9]+$ && "${v_seq}" =~ ^[0-9]+$ ]]; then
        if ((10#${v_year} == 10#${year} && 10#${v_month} == 10#${month} && 10#${v_day} == 10#${day})); then
          if ((10#${v_seq} > max_seq)); then
            max_seq=$((10#${v_seq}))
          fi
        fi
      fi
    done
    shopt -u nullglob
  fi
  version="${year}.${month}.${day}.$((max_seq + 1))"
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

mkdir -p "${tmp_dir}/strategies"
while IFS= read -r file; do
  rel_path="${file#${strategy_src}/}"
  dest_path="${tmp_dir}/strategies/${rel_path}"
  mkdir -p "$(dirname "${dest_path}")"
  cp "${file}" "${dest_path}"
done < <(find "${strategy_src}" -type f -name "*.py")

cat >"${tmp_dir}/pyproject.toml" <<EOF
[build-system]
requires = ["setuptools>=61.0", "wheel", "build"]
build-backend = "setuptools.build_meta"

[project]
name = "strategies"
version = "${version}"
description = "Research strategies package"
requires-python = ">=3.10"

[tool.setuptools]
packages = ["strategies"]
EOF

uv run --python "${python_bin}" --no-sync -m build --wheel --no-isolation --outdir "${tmp_dir}/dist" "${tmp_dir}"

wheel_path="$(ls -1 "${tmp_dir}/dist"/strategies-*.whl 2>/dev/null | head -n 1 || true)"
if [[ -z "${wheel_path}" ]]; then
  echo "Wheel build failed: no wheel produced." >&2
  exit 1
fi

mkdir -p "${output_dir}"
cp "${wheel_path}" "${output_dir}/"

wheel_name="$(basename "${wheel_path}")"
echo "Wheel created: ${output_dir}/${wheel_name}"
