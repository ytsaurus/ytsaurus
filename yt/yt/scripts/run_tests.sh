#!/bin/bash -e
# Shared test orchestrator for the GitHub CI (check-jobs.yaml) and the Arcadia
# run_cmake_unittests.sh / run_cmake_pytest.sh. Single source of truth for how
# unittests and pytest suites are launched. Odin tests stay separate
# (yt/odin/tests/run_tests.sh).
#
# Usage:
#   run_tests.sh unittests [--build-path P] [--jobs N] [unittester-name ...]
#       Parallelism defaults to nproc; override with --jobs or UNITTEST_JOBS.
#       Each binary is bounded by UNITTEST_TIMEOUT seconds (default 1200).
#   run_tests.sh prepare
#   run_tests.sh integration
#   run_tests.sh python [pytest-args ...]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"

setup_pytest_env() {
    export SOURCE_ROOT
    export BUILD_ROOT="$(realpath ../build)"
    export PYTHON_ROOT="$(realpath ../python)"
    export VIRTUALENV_PATH="$(realpath ../venv)"
    export TESTS_SANDBOX="$(realpath ../tests_sandbox)"
    source "${VIRTUALENV_PATH}/bin/activate"
}

# Unittests skipped in the opensource CI. One per line — add a suite here to skip it.
UNITTEST_SKIP=(
    unittester-containers       # requires porto to be installed
    unittester-core-rpc-http
    unittester-library-s3
    unittester-library-ytprof   # requires the pprof binary and a non-empty arc revision
    library-cpp-logger-global
    yt-cpp-mapreduce
)

is_unittest_skipped() {
    local binary="$1" pattern
    for pattern in "${UNITTEST_SKIP[@]}"; do
        [[ ${binary} == *"${pattern}"* ]] && return 0
    done
    return 1
}

cmd_unittests() {
    local build_path=""
    local jobs="${UNITTEST_JOBS:-$(nproc)}"
    local names=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --build-path) build_path="$2"; shift 2;;
            --jobs)       jobs="$2"; shift 2;;
            *)            names+=("$1"); shift;;
        esac
    done

    [ -n "${build_path}" ] && cd "${build_path}"

    # Collect the binaries to run, then fan them out (they are independent
    # processes writing per-binary junit xml, so they parallelize cleanly).
    local binaries=()
    if [ ${#names[@]} -gt 0 ]; then
        local n
        for n in "${names[@]}"; do binaries+=("./${n}"); done
    else
        local b
        while IFS= read -r b; do
            is_unittest_skipped "${b}" && continue
            binaries+=("${b}")
        done < <(find . -name "unittester-*" -type f)
        while IFS= read -r b; do
            is_unittest_skipped "${b}" && continue
            binaries+=("${b}")
        done < <(find . -name "*-ut" -type f)
    fi

    if [ ${#binaries[@]} -eq 0 ]; then
        echo "No unittest binaries found${build_path:+ in ${build_path}}" >&2
        exit 1
    fi

    echo "Running ${#binaries[@]} unittest binaries with parallelism ${jobs}"
    # Each binary runs through the internal 'run-one' action (retry + gtest xml +
    # captured log). xargs bounds concurrency and exits non-zero if any binary fails.
    printf '%s\0' "${binaries[@]}" \
        | xargs -0 -P "${jobs}" -I{} "${SCRIPT_DIR}/run_tests.sh" run-one --binary {}
}

# Internal: run a single unittest binary (invoked in parallel by cmd_unittests
# via xargs). The captured stdout/stderr is always printed as one block (pass or
# fail) to keep parallel logs readable; the per-binary gtest xml is always written.

# Prefix each line with a wall-clock time as it is produced. Because the output is
# buffered and flushed as one block, GitHub would otherwise stamp every line with
# the flush time; this records when each line was actually written. Uses bash
# printf '%()T' to avoid forking date per line.
prepend_ts() {
    local line
    while IFS= read -r line || [ -n "${line}" ]; do
        printf '%(%H:%M:%S)T %s\n' -1 "${line}"
    done
}

cmd_run_one() {
    local binary=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --binary) binary="$2"; shift 2;;
            *) echo "run-one: unknown arg $1" >&2; exit 1;;
        esac
    done

    local name
    name="$(basename "${binary}")"

    # Announce the start immediately (single atomic line). Especially useful for a
    # hang: its completion block only appears after the timeout, but this shows
    # right away that the binary was launched.
    echo "Running ${name}"

    local log
    log="$(mktemp)"

    # Bound every binary: a hanging unittest must not stall the whole step (a
    # single hang once burned the 6h job limit). timeout kills it (exit 124) and
    # it is reported as a failure like any other. Override via UNITTEST_TIMEOUT.
    local timeout_secs="${UNITTEST_TIMEOUT:-1200}"
    local max_attempts=3

    local cmd=(timeout -k 10 "${timeout_secs}" "${binary}")
    if [[ ${name} =~ ^unittester- ]]; then
        cmd+=(--gtest_output="xml:junit-${name}.xml")
    fi

    # Own retry loop (rather than a generic wrapper) so we can report on which
    # attempt the binary finally succeeded / gave up. pipefail keeps the test's
    # exit code (not prepend_ts's) as the pipeline status, so failures still count.
    set -o pipefail
    local attempt=0 rc=0
    while true; do
        attempt=$((attempt + 1))
        # Append (don't overwrite) so the output of failed earlier attempts is
        # kept too; a banner separates each retry.
        if [ ${attempt} -gt 1 ]; then
            printf -- '----- retry: attempt %s/%s -----\n' "${attempt}" "${max_attempts}" >> "${log}"
        fi
        if "${cmd[@]}" 2>&1 | prepend_ts >> "${log}"; then
            rc=0
            break
        else
            rc=$?
        fi
        if [ ${attempt} -ge ${max_attempts} ]; then
            break
        fi
    done

    local status
    if [ ${rc} -eq 0 ]; then
        status="PASS on attempt ${attempt}/${max_attempts}"
    elif [ ${rc} -eq 124 ] || [ ${rc} -eq 137 ]; then
        status="FAIL — timed out after ${timeout_secs}s (${attempt}/${max_attempts} attempts)"
    else
        status="FAIL — exit ${rc} (${attempt}/${max_attempts} attempts)"
    fi

    # Always emit the name + status header and the full captured output, whether
    # the binary passed or failed, as a single block (avoids interleaving with
    # other parallel binaries).
    printf '===== %s: %s =====\n%s\n===== END %s =====\n' \
        "${name}" "${status}" "$(cat "${log}")" "${name}"
    rm -f "${log}"
    return ${rc}
}

# Idempotent: each step checks its own result and is skipped if already done, so
# 'prepare' can be run repeatedly and is invoked on demand by integration/python.
cmd_prepare() {
    setup_pytest_env

    mkdir -p "${PYTHON_ROOT}"

    pip3 install -e "${SOURCE_ROOT}/yt/python/packages"

    # generate_python_proto + prepare_python_modules populate ${PYTHON_ROOT}/yt
    # (python modules) and the bindings .so files. Skip both if already populated.
    if [ ! -d "${PYTHON_ROOT}/yt" ]; then
        generate_python_proto \
            --source-root "${SOURCE_ROOT}" \
            --output "${PYTHON_ROOT}"

        prepare_python_modules \
            --source-root "${SOURCE_ROOT}" \
            --build-root "${BUILD_ROOT}" \
            --output-path "${PYTHON_ROOT}" \
            --prepare-bindings-libraries
    fi

    # ln -sf is idempotent (re-points if already present).
    mkdir -p "${BUILD_ROOT}/yt/yt/packages/tests_package/"
    ln -sf "${BUILD_ROOT}/yt/yt/server/all/ytserver-all" "${BUILD_ROOT}/yt/yt/packages/tests_package/ytserver-all"

    mkdir -p "${BUILD_ROOT}/yt/python/yt/environment/bin/"
    ln -sf "${SOURCE_ROOT}/yt/python/yt/environment/bin/yt_env_watcher" "${BUILD_ROOT}/yt/python/yt/environment/bin/yt_env_watcher"

    mkdir -p "${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/"
    ln -sf "${SOURCE_ROOT}/yt/yt/tools/prepare_scheduling_usage/__main__.py" "${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/prepare_scheduling_usage"

    pip3 install -r "${SOURCE_ROOT}/yt/yt/scripts/pytest_requirements.txt"
}

cmd_integration() {
    cmd_prepare
    cd "${SOURCE_ROOT}/yt/yt/tests/integration"
    ./run_tests.sh -m opensource
}

cmd_python() {
    cmd_prepare
    cd "${PYTHON_ROOT}"

    export PYTHONPATH="${PYTHON_ROOT}"
    export YT_BUILD_ROOT="${BUILD_ROOT}"
    export YT_TESTS_SANDBOX="${TESTS_SANDBOX}"

    if [ $# -gt 0 ]; then
        python3 -m pytest -vs "yt/wrapper/tests" -m opensource "$@"
        return 0
    fi

    python3 -m pytest -vs "yt/local" "yt/yson" "yt/skiff"
    python3 -m pytest -vs "yt/wrapper/tests" -m opensource
}

action="${1:-}"
[ -z "${action}" ] && { echo "Usage: $0 {unittests|prepare|integration|python} [args]" >&2; exit 1; }
shift

case "${action}" in
    unittests)   cmd_unittests "$@";;
    run-one)     cmd_run_one "$@";;
    prepare)     cmd_prepare "$@";;
    integration) cmd_integration "$@";;
    python)      cmd_python "$@";;
    *) echo "Unknown action: ${action}" >&2; exit 1;;
esac
