#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <bench-results.csv> [repeats]" >&2
  exit 2
fi

result_file="$1"
expected_repeats="${2:-3}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline_file="${script_dir}/bench-baseline.csv"

if [[ ! -f "${result_file}" || ! -r "${result_file}" ]]; then
  echo "result file not readable: ${result_file}" >&2
  exit 1
fi
if ! [[ "${expected_repeats}" =~ ^[1-9][0-9]*$ ]]; then
  echo "repeats must be a positive integer" >&2
  exit 2
fi

declare -A min_tps max_p99 counts seen
while IFS=, read -r scenario min_value max_value; do
  [[ "${scenario}" == "scenario" ]] && continue
  if [[ -z "${scenario}" || -z "${min_value}" || -z "${max_value}" ]]; then
    echo "invalid baseline row" >&2
    exit 1
  fi
  min_tps["${scenario}"]="${min_value}"
  max_p99["${scenario}"]="${max_value}"
  counts["${scenario}"]=0
done < "${baseline_file}"

if [[ "${#min_tps[@]}" -eq 0 ]]; then
  echo "baseline has no scenarios" >&2
  exit 1
fi

while IFS=, read -r scenario repeat output; do
  [[ "${scenario}" == "scenario" ]] && continue
  if [[ -z "${min_tps[${scenario}]+present}" ]]; then
    echo "unexpected scenario: ${scenario}" >&2
    exit 1
  fi
  if ! [[ "${repeat}" =~ ^[1-9][0-9]*$ ]] || (( repeat > expected_repeats )); then
    echo "invalid repeat for ${scenario}: ${repeat}" >&2
    exit 1
  fi
  key="${scenario}:${repeat}"
  if [[ -n "${seen[${key}]+present}" ]]; then
    echo "duplicate repeat for ${scenario}: ${repeat}" >&2
    exit 1
  fi
  seen["${key}"]=1
  if [[ "${output}" =~ TPS=([0-9]+([.][0-9]+)?) ]]; then
    tps="${BASH_REMATCH[1]}"
  else
    echo "missing TPS for ${scenario} repeat ${repeat}" >&2
    exit 1
  fi
  if [[ "${output}" =~ p99_us=([0-9]+([.][0-9]+)?) ]]; then
    p99="${BASH_REMATCH[1]}"
  else
    echo "missing p99_us for ${scenario} repeat ${repeat}" >&2
    exit 1
  fi
  if awk -v actual="${tps}" -v minimum="${min_tps[${scenario}]}" 'BEGIN { exit !(actual + 0 < minimum + 0) }'; then
    echo "TPS regression: ${scenario} repeat ${repeat} actual=${tps} minimum=${min_tps[${scenario}]}" >&2
    exit 1
  fi
  if awk -v actual="${p99}" -v maximum="${max_p99[${scenario}]}" 'BEGIN { exit !(actual + 0 > maximum + 0) }'; then
    echo "p99 regression: ${scenario} repeat ${repeat} actual=${p99} maximum=${max_p99[${scenario}]}" >&2
    exit 1
  fi
  counts["${scenario}"]=$((counts["${scenario}"] + 1))
done < "${result_file}"

for scenario in "${!min_tps[@]}"; do
  if (( counts["${scenario}"] != expected_repeats )); then
    echo "incomplete scenario: ${scenario} repeats=${counts[${scenario}]} expected=${expected_repeats}" >&2
    exit 1
  fi
  echo "bench_ok scenario=${scenario} repeats=${counts[${scenario}]} min_tps=${min_tps[${scenario}]} max_p99_us=${max_p99[${scenario}]}"
done
