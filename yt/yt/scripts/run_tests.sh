#!/bin/bash -e
# Usage:
#   run_tests.sh unittests [--build-path P] [unittester-name ...]
#   run_tests.sh prepare
#   run_tests.sh integration
#   run_tests.sh python [pytest-args ...]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"

retry() {
    local retries=3
    local count=0
    until "$@"; do
        local exit_code=$?
        count=$((count + 1))
        if [ ${count} -lt ${retries} ]; then
            echo "Attempt ${count} failed with exit code ${exit_code}. Retrying..."
        else
            echo "Attempt ${count} failed with exit code ${exit_code}. No more retries."
            return ${exit_code}
        fi
    done
    return 0
}

setup_pytest_env() {
    export SOURCE_ROOT
    export BUILD_ROOT="$(realpath ../build)"
    export PYTHON_ROOT="$(realpath ../python)"
    export VIRTUALENV_PATH="$(realpath ../venv)"
    export TESTS_SANDBOX="$(realpath ../tests_sandbox)"
    source "${VIRTUALENV_PATH}/bin/activate"
}

cmd_unittests() {
    set -x
    local build_path=""
    local names=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --build-path) build_path="$2"; shift 2;;
            *) names+=("$1"); shift;;
        esac
    done

    [ -n "${build_path}" ] && cd "${build_path}"

    if [ ${#names[@]} -gt 0 ]; then
        for unittester_name in "${names[@]}"; do
            local unittester_binary="./${unittester_name}"
            echo "Running ${unittester_binary}"
            if [[ ${unittester_name} =~ ^unittester- ]]; then
                retry "${unittester_binary}" --gtest_output="xml:junit-${unittester_name}.xml"
            else
                retry "${unittester_binary}"
            fi
        done
        return 0
    fi

    # Flow unit gtests run first and, unlike the loops below, do not abort on
    # failure, so a flaky generic unittester cannot skip them; a flow failure
    # is recorded and returned at the end. Flow unit gtests have path-derived
    # *-unittest(s) names; flow gtests that need a running YT are named
    # differently and thus excluded.
    local flow_unittests_status=0
    for unittester_binary in $(find ./yt/yt/flow -type f \( -name "*-unittests" -o -name "*-unittest" \) 2>/dev/null); do
        echo "Running ${unittester_binary}"
        local unittester_name
        unittester_name="$(basename "${unittester_binary}")"
        retry "${unittester_binary}" --gtest_output="xml:junit-${unittester_name}.xml" || flow_unittests_status=1
    done

    local skip_unittesters=(
        unittester-containers
        unittester-core-rpc-http
        unittester-library-s3
        unittester-library-ytprof
    )
    local skip_unittesters_re
    skip_unittesters_re=$(IFS='|'; echo "${skip_unittesters[*]}")

    for unittester_binary in $(find . -name "unittester-*" -type f); do
        [[ ${unittester_binary} =~ (${skip_unittesters_re}) ]] && continue
        echo "Running ${unittester_binary}"
        local unittester_name
        unittester_name="$(basename "${unittester_binary}")"
        retry "${unittester_binary}" --gtest_output="xml:junit-${unittester_name}.xml"
    done

    local skip_uts=(
        library-cpp-logger-global
        yt-cpp-mapreduce
    )
    local skip_uts_re
    skip_uts_re=$(IFS='|'; echo "${skip_uts[*]}")

    for unittester_binary in $(find . -name "*-ut" -type f); do
        [[ ${unittester_binary} =~ (${skip_uts_re}) ]] && continue
        echo "Running ${unittester_binary}"
        retry "${unittester_binary}"
    done

    return ${flow_unittests_status}
}

cmd_prepare() {
    setup_pytest_env

    mkdir -p "${PYTHON_ROOT}"

    pip3 install -e "${SOURCE_ROOT}/yt/python/packages"

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

cmd_flow() {
    cmd_prepare

    # The Flow runner is self-contained (it assembles the flow-specific python,
    # protos and pipeline binaries on top of `prepare`, starts the local YT
    # recipe and runs pytest). Without arguments it runs the opensource CI
    # scope; pass --all or explicit targets to run more (see the runner).
    cd "${SOURCE_ROOT}/yt/yt/flow/tests"
    if [ $# -gt 0 ]; then
        ./run_tests.sh "$@"
    else
        ./run_tests.sh --ci
    fi
}

action="${1:-}"
[ -z "${action}" ] && { echo "Usage: $0 {unittests|prepare|integration|python|flow} [args]" >&2; exit 1; }
shift

case "${action}" in
    unittests)   cmd_unittests "$@";;
    prepare)     cmd_prepare "$@";;
    integration) cmd_integration "$@";;
    python)      cmd_python "$@";;
    flow)        cmd_flow "$@";;
    *) echo "Unknown action: ${action}" >&2; exit 1;;
esac
