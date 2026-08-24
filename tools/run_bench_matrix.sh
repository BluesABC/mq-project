#!/usr/bin/env bash
set -Eeuo pipefail

build_dir="${1:-build-linux}"
messages="${2:-1000000}"
repeats="${3:-3}"
host="${MQ_BENCH_HOST:-127.0.0.1}"
port="${MQ_BENCH_PORT:-9092}"
bench="${build_dir}/mq_bench"
output="bench-results-$(date +%Y%m%d-%H%M%S).csv"

if [[ ! -x "${bench}" ]]; then
  echo "benchmark executable not found: ${bench}" >&2
  exit 1
fi
bench_version="$(${bench} --version 2>/dev/null || true)"
if [[ "${bench_version}" != "bench_version=3" ]]; then
  echo "stale benchmark executable: expected bench_version=3, got '${bench_version}'" >&2
  echo "rebuild ${build_dir}/mq_bench from the current source before running the matrix" >&2
  exit 1
fi
if ! [[ "${messages}" =~ ^[1-9][0-9]*$ && "${repeats}" =~ ^[1-9][0-9]*$ ]]; then
  echo "messages and repeats must be positive integers" >&2
  exit 2
fi

printf 'scenario,repeat,output\n' > "${output}"
printf 'result_file=%s\n' "${output}"
printf 'host=%s port=%s messages=%s repeats=%s\n' "${host}" "${port}" "${messages}" "${repeats}"

run_case() {
  local name="$1" size="$2" connections="$3" batch="$4" partitions="$5" repeat
  for ((repeat = 1; repeat <= repeats; ++repeat)); do
    local topic="bench_${name}_r${repeat}_$(date +%s%N)"
    local output_line
    output_line="$(${bench} produce --host "${host}" --port "${port}" --topic "${topic}" \
      --messages "${messages}" --size "${size}" --connections "${connections}" \
      --batch "${batch}" --partitions "${partitions}")"
    printf '%s,%s,%s\n' "${name}" "${repeat}" "${output_line}" >> "${output}"
    printf '%s repeat=%s %s\n' "${name}" "${repeat}" "${output_line}"
  done
}

run_case "produce_256_b1_c1_p3" 256 1 1 3
run_case "produce_256_b100_c1_p3" 256 1 100 3
run_case "produce_256_b1000_c1_p3" 256 1 1000 3
run_case "produce_256_b1000_c10_p3" 256 10 1000 3
run_case "produce_4096_b1000_c10_p3" 4096 10 1000 3

printf 'consume is run separately because it requires a retained topic with known message count.\n'
printf 'example: %s consume --topic <retained_topic> --messages %s --connections 3 --partitions 3 --group bench_group\n' "${bench}" "${messages}"
