#!/bin/bash

# Flow opensource test runner. Invoked as `run_tests.sh flow [...]` by the
# top-level yt/yt/scripts/run_tests.sh dispatcher, which runs `prepare` first
# and exports the *_ROOT / VIRTUALENV_PATH variables this script relies on.
#
# Usage (the argument is forwarded from the dispatcher):
#   run_tests.sh --ci                 the opensource CI scope (the lists below)
#   run_tests.sh --all                every flow test shipped to opensource
#   run_tests.sh <pytest targets...>  exactly the given paths/node ids/flags

if [ -z "$PYTHONPATH" ]; then
    export PYTHONPATH=""
fi

set -eu

# Exported by the dispatcher's `prepare` step (setup_pytest_env + cmd_prepare).
: "${SOURCE_ROOT:?SOURCE_ROOT must be set (run via yt/yt/scripts/run_tests.sh flow)}"
: "${BUILD_ROOT:?BUILD_ROOT must be set (run via yt/yt/scripts/run_tests.sh flow)}"
: "${PYTHON_ROOT:?PYTHON_ROOT must be set (run via yt/yt/scripts/run_tests.sh flow)}"
: "${VIRTUALENV_PATH:?VIRTUALENV_PATH must be set (run via yt/yt/scripts/run_tests.sh flow)}"
: "${TESTS_SANDBOX:?TESTS_SANDBOX must be set (run via yt/yt/scripts/run_tests.sh flow)}"

FLOW_SRC="${SOURCE_ROOT}/yt/yt/flow"
FLOW_TESTS_DIR="${FLOW_SRC}/tests"
VENV_PYTHON="${VIRTUALENV_PATH}/bin/python3"

# ============================================================================
# All example tests shipped to opensource, relative to yt/yt/flow.
FLOW_EXAMPLE_INTEGRATION_PATHS=(
    "examples/cpp/*/test"
    "examples/go/*/test"
    "examples/python/*/test"
)
FLOW_EXAMPLE_CPP_UNITTEST_PATHS=(
    "examples/cpp/*/unittest"
    "examples/cpp/*/pipeline/unittest"
)
FLOW_EXAMPLE_PYTHON_UNITTEST_PATHS=(
    "examples/python/*/unittests"
)

# Node-id substrings excluded unless FLOW_TESTS_IGNORE_BLACKLIST=1.
export FLOW_TESTS_BLACKLIST="
    vanilla
"
# ============================================================================

expand_paths() {
    for pattern in "$@"; do
        for path in ${FLOW_SRC}/${pattern}; do
            if [ -d "${path}" ]; then
                echo "${path}"
            fi
        done
    done | sort
}

usage() {
    sed -n '3,10p' "$0" >&2
    exit 1
}

if [ "$#" -eq 0 ]; then
    usage
fi

MODE=manual
CPP_UNITTEST_ROOTS=""
PYTHON_UNITTEST_ROOTS=""
INTEGRATION_ROOTS=""
case "$1" in
    --ci)
        MODE=ci
        CPP_UNITTEST_ROOTS=$(expand_paths "${FLOW_EXAMPLE_CPP_UNITTEST_PATHS[@]}")
        PYTHON_UNITTEST_ROOTS=$(expand_paths "${FLOW_EXAMPLE_PYTHON_UNITTEST_PATHS[@]}")
        INTEGRATION_ROOTS=$(expand_paths "${FLOW_EXAMPLE_INTEGRATION_PATHS[@]}")
        ;;
    --all)
        MODE=all
        CPP_UNITTEST_ROOTS=$(expand_paths "${FLOW_EXAMPLE_CPP_UNITTEST_PATHS[@]}")
        PYTHON_UNITTEST_ROOTS=$(expand_paths "${FLOW_EXAMPLE_PYTHON_UNITTEST_PATHS[@]}")
        INTEGRATION_ROOTS=$(
            {
                echo "${FLOW_TESTS_DIR}"
                find "${FLOW_SRC}/library" -type d \( -name "tests" -o -name "tests_*" -o -name "test" \)
                expand_paths "${FLOW_EXAMPLE_INTEGRATION_PATHS[@]}"
            } | sort -u
        )
        ;;
    *)
        # Explicit pytest targets (paths, node ids, flags such as -k): run
        # exactly those. The recipe still starts, so any integration test has
        # its local YT.
        INTEGRATION_ROOTS="$*"
        ;;
esac

# ============================================================================
# Assemble the extra Flow bits on top of the environment `prepare` produced.
#
# `prepare` assembles yt/python/yt. The Flow tests additionally import
# yt.yt.flow.* and yt.recipe.* from the source tree, and -- in the opensource
# build -- reach yt_sync through the yt.yt_sync.* shim over yt_sync_mini.
# Expose all three inside the already-regular `yt` package.
YT_SYNC_MINI="${FLOW_SRC}/library/python/yt_sync_mini"

mkdir -p "${PYTHON_ROOT}/yt/yt"
ln -sfn "${FLOW_SRC}" "${PYTHON_ROOT}/yt/yt/flow"
ln -sfn "${SOURCE_ROOT}/yt/recipe" "${PYTHON_ROOT}/yt/recipe"

mkdir -p "${PYTHON_ROOT}/yt/yt_sync/core"
cp -f "${YT_SYNC_MINI}/runner.py" "${PYTHON_ROOT}/yt/yt_sync/runner.py"
cp -f "${YT_SYNC_MINI}/constants.py" "${PYTHON_ROOT}/yt/yt_sync/core/constants.py"

# Generate the Flow proto python modules the python companion imports.
#
# The pb2 files must be importable as yt.yt.flow.library.cpp.{common,companion}
# .proto.* (the real module path _proto_compat aliases to), so they are
# generated next to their .proto sources -- the same trick the yt runner uses
# when it symlinks a conftest into the source tree. Plain protoc emits the pb2
# code (gencode from protoc >= 3.20 runs on the pinned protobuf runtime); the
# grpc stub comes from grpc_tools.protoc, version-locked by the grpcio-tools
# pin in pytest_requirements.txt.
PROTO_STAGING="${TESTS_SANDBOX}/flow_proto_gen"
rm -rf "${PROTO_STAGING}"
mkdir -p "${PROTO_STAGING}"

find "${FLOW_SRC}/library/cpp/common/proto" "${FLOW_SRC}/library/cpp/companion/proto" -name "*.proto" \
    | while read -r proto_file; do
        protoc --proto_path "${SOURCE_ROOT}/yt" --python_out "${PROTO_STAGING}" "${proto_file}"
    done

"${VENV_PYTHON}" -m grpc_tools.protoc --proto_path "${SOURCE_ROOT}/yt" \
    --grpc_python_out "${PROTO_STAGING}" \
    "${FLOW_SRC}/library/cpp/companion/proto/companion_service.proto"

cp -f "${PROTO_STAGING}/yt/flow/library/cpp/common/proto/"*_pb2*.py "${FLOW_SRC}/library/cpp/common/proto/"
cp -f "${PROTO_STAGING}/yt/flow/library/cpp/companion/proto/"*_pb2*.py "${FLOW_SRC}/library/cpp/companion/proto/"

# The companion test proto has no PROTO_NAMESPACE, so its import path is the
# full source-tree path and protoc runs from the source root.
protoc --proto_path "${SOURCE_ROOT}" --python_out "${PROTO_STAGING}" \
    "${FLOW_SRC}/library/python/companion/test/proto/message.proto"
cp -f "${PROTO_STAGING}/yt/yt/flow/library/python/companion/test/proto/"*_pb2*.py \
    "${FLOW_SRC}/library/python/companion/test/proto/"

# Example protos have no PROTO_NAMESPACE either: their tests import the pb2
# modules by the full source-tree path, so generate them the same way.
find "${FLOW_SRC}/examples" -name "*.proto" \
    | while read -r proto_file; do
        protoc --proto_path "${SOURCE_ROOT}" --python_out "${PROTO_STAGING}" "${proto_file}"
        proto_dir="$(dirname "${proto_file}")"
        cp -f "${PROTO_STAGING}/${proto_dir#"${SOURCE_ROOT}"/}/"*_pb2*.py "${proto_dir}/"
    done

# Generate launcher scripts for the PY3_PROGRAM pipelines (python examples and
# python companion pipelines). In arcadia these are self-contained python
# binaries resolved via yatest.common.binary_path; here an executable wrapper
# at the expected build-root location runs the program from the assembled
# PYTHON_ROOT. The paths are baked in, so the wrapper also works when a
# vanilla operation ships it into a local YT job on this host.
grep -rl "^PY3_PROGRAM" "${FLOW_SRC}/tests" "${FLOW_SRC}/examples" "${FLOW_SRC}/library" --include=ya.make \
    | while read -r program_ya_make; do
        program_dir="$(dirname "${program_ya_make}")"
        if [ ! -e "${program_dir}/__main__.py" ]; then
            continue
        fi
        program_name="$(sed -n 's/^PY3_PROGRAM(\([^)]\{1,\}\)).*/\1/p' "${program_ya_make}" | head -n 1)"
        if [ -z "${program_name}" ]; then
            program_name="$(basename "${program_dir}")"
        fi
        relative_dir="${program_dir#"${SOURCE_ROOT}"/}"
        module_name="$(echo "${relative_dir}" | tr / .)"
        wrapper_path="${BUILD_ROOT}/${relative_dir}/${program_name}"
        mkdir -p "$(dirname "${wrapper_path}")"
        cat > "${wrapper_path}" <<EOF
#!/bin/sh
export PYTHONPATH="${PYTHON_ROOT}"
exec "${VENV_PYTHON}" -m ${module_name} "\$@"
EOF
        chmod +x "${wrapper_path}"
    done

# The local YT recipe resolves yt_local from the build root; the cmake build
# does not produce it, so link the source script (it runs on the venv python
# via PATH and PYTHONPATH of the test environment).
mkdir -p "${BUILD_ROOT}/yt/python/yt/local/bin"
ln -sf "${SOURCE_ROOT}/yt/python/yt/local/bin/yt_local" "${BUILD_ROOT}/yt/python/yt/local/bin/yt_local"
chmod +x "${SOURCE_ROOT}/yt/python/yt/local/bin/yt_local" || true

# flow_execute drives pipelines through the yt CLI, resolved as the arcadia
# yt_make binary; wrap the source script the same way as the python pipelines.
YT_CLI_WRAPPER="${BUILD_ROOT}/yt/python/yt/wrapper/bin/yt_make/yt"
mkdir -p "$(dirname "${YT_CLI_WRAPPER}")"
cat > "${YT_CLI_WRAPPER}" <<EOF
#!/bin/sh
export PYTHONPATH="${PYTHON_ROOT}"
exec "${VENV_PYTHON}" "${PYTHON_ROOT}/yt/wrapper/bin/yt" "\$@"
EOF
chmod +x "${YT_CLI_WRAPPER}"

# yatest.common.binary_path resolves <build_root>/<arcadia-relative-dir>/<name>.
required_binary_dirs="yt/yt/flow/bin/flow_server"
for example_test_dir in "${FLOW_SRC}"/examples/{cpp,go}/*/test; do
    if [ -d "${example_test_dir}" ]; then
        required_binary_dirs="${required_binary_dirs} $(dirname "${example_test_dir#"${SOURCE_ROOT}"/}")"
    fi
done

missing_binaries=""
for relative_dir in ${required_binary_dirs}; do
    program_name="$(basename "${relative_dir}")"
    expected="${BUILD_ROOT}/${relative_dir}/${program_name}"
    if [ ! -e "${expected}" ]; then
        found="$(find "${BUILD_ROOT}/${relative_dir}" -name "${program_name}" -type f 2>/dev/null | head -n 1 || true)"
        if [ -n "${found}" ]; then
            mkdir -p "$(dirname "${expected}")"
            ln -sf "${found}" "${expected}"
        else
            missing_binaries="${missing_binaries}  ${relative_dir}/${program_name}"$'\n'
        fi
    fi
done
if [ -n "${missing_binaries}" ]; then
    if [ "${MODE}" = "ci" ]; then
        echo "error: flow binaries not found under ${BUILD_ROOT}:" >&2
        printf '%s' "${missing_binaries}" >&2
        exit 1
    else
        # --all and manual runs may target suites outside the CI scope whose
        # pipeline binaries this build does not produce; warn and let the
        # individual test surface the missing binary.
        echo "warning: some example flow binaries were not found under ${BUILD_ROOT}:" >&2
        printf '%s' "${missing_binaries}" >&2
    fi
fi
# ============================================================================

# yt.* (including the flow / recipe / yt_sync entries assembled above) resolve
# from PYTHON_ROOT; library.python.* and yatest / yatest_lib come straight from
# the source tree; FLOW_TESTS_DIR carries the opensource pytest plugin
# (flow_os_test_env).
export PYTHONPATH="${PYTHON_ROOT}:${SOURCE_ROOT}:${SOURCE_ROOT}/library/python/testing/yatest_common:${SOURCE_ROOT}/library/python/testing:${FLOW_TESTS_DIR}:${PYTHONPATH}"

export YT_FLOW_SOURCE_ROOT="${SOURCE_ROOT}"
export YT_FLOW_BUILD_ROOT="${BUILD_ROOT}"
export YT_FLOW_TEST_SANDBOX="${TESTS_SANDBOX}"
export YT_FLOW_OS_TEST_MODE=1

# flow_execute feature toggles. ya-make sets these per variant package; the
# only variant shipped to opensource is trunk, whose ya.make.inc defaults are
# all 1.
export FLOW_EXECUTE_ENABLE_PROXY_SIGNATURES=1
export FLOW_EXECUTE_CHECK_PLAINTEXT=1
export FLOW_EXECUTE_CHECK_KNOWN_COMMANDS=1
export FLOW_EXECUTE_CHECK_SPEC_VERSIONS=1
export FLOW_EXECUTE_TEST_FLOW_CORE_TARGET=1

# --rootdir + --import-mode=importlib make pytest import test modules under
# their source-tree dotted names (yt.yt.flow....test_x), which resolve through
# the PYTHON_ROOT flow entry; this keeps same-named test files apart and makes
# relative imports inside test dirs work.
# python_files additionally matches plain test.py, which several flow test
# dirs use (ya-make lists TEST_SRCS explicitly, so the name never mattered).
run_pytest() {
    python3 -m pytest -s -v \
        --rootdir="${SOURCE_ROOT}" \
        --confcutdir="${FLOW_SRC}" \
        --import-mode=importlib \
        -o "python_files=test_*.py test.py" \
        -p flow_os_test_env \
        "$@"
}

run_cpp_unittests() {
    for source_dir in ${CPP_UNITTEST_ROOTS}; do
        relative_dir="${source_dir#"${SOURCE_ROOT}"/}"
        binary_path=""
        for candidate in "${BUILD_ROOT}/${relative_dir}"/*; do
            if [ -f "${candidate}" ] && [ -x "${candidate}" ]; then
                if [ -n "${binary_path}" ]; then
                    echo "error: multiple C++ example unit test binaries found under ${BUILD_ROOT}/${relative_dir}" >&2
                    status=1
                    binary_path=""
                    break
                fi
                binary_path="${candidate}"
            fi
        done
        if [ -z "${binary_path}" ]; then
            echo "error: C++ example unit test binary not found under ${BUILD_ROOT}/${relative_dir}" >&2
            status=1
            continue
        fi
        binary_name=$(basename "${binary_path}")
        "${binary_path}" --gtest_output="xml:${TESTS_SANDBOX}/${binary_name}.xml" || status=$?
    done
}

status=0

# Unit tests need no local YT, so run them before (and without) the recipe.
if [ -n "${CPP_UNITTEST_ROOTS}" ]; then
    run_cpp_unittests
fi
if [ -n "${PYTHON_UNITTEST_ROOTS}" ]; then
    run_pytest ${PYTHON_UNITTEST_ROOTS} || status=$?
fi

if [ -n "${INTEGRATION_ROOTS}" ]; then
    # Share PortManager reservations between clusters prepared before any of them starts.
    export PORT_SYNC_PATH="${PORT_SYNC_PATH:-${TESTS_SANDBOX}/flow_port_sync}"

    # Start only the clusters required by the selected scope.
    if [ "${MODE}" = "ci" ]; then
        YT_CLUSTER_NAMES="primary,remote_0"
    else
        YT_CLUSTER_NAMES="primary,remote_0,remote_1"
    fi
    YT_CONFIG_PATCH='{fqdn="localhost";rpc_proxy_count=1;node_count=3;scheduler_count=1;queue_agent_state_target_version=7;}'
    RECIPE_ENV_FILE="${TESTS_SANDBOX}/flow_recipe_env.json"
    # The plugin applies the variables the recipe exported (YT_PROXY*,
    # YT_TOKEN, YT_PROXY_URL_ALIASING_CONFIG) to the test process, like the
    # ya-make runtime.
    export YT_FLOW_RECIPE_ENV_FILE="${RECIPE_ENV_FILE}"

    flow_recipe() {
        python3 -m yt.recipe.basic \
            --source-root "${SOURCE_ROOT}" \
            --build-root "${BUILD_ROOT}" \
            --output-dir "${TESTS_SANDBOX}" \
            --env-file "${RECIPE_ENV_FILE}" \
            "$@"
    }

    # Start the local YT clusters -- the recipe the tests reach through
    # get_yt_clusters(). ytserver-all is picked from the tests_package symlink
    # that `prepare` created.
    flow_recipe start \
        --cluster-names "${YT_CLUSTER_NAMES}" \
        --config-patch "${YT_CONFIG_PATCH}" \
        --package-dir yt/yt/packages/tests_package \
        --cleanup-working-directory

    # Package artifacts may contain a stale yt_local wrapper; use the stable source wrapper for teardown.
    "${VENV_PYTHON}" -c '
import json
import sys

recipe_info_path, yt_local_path = sys.argv[1:]
with open(recipe_info_path) as recipe_info_file:
    recipe_info = json.load(recipe_info_file)
for instance in recipe_info:
    instance["yt_local_exec"] = [yt_local_path]
with open(recipe_info_path, "w") as recipe_info_file:
    json.dump(recipe_info, recipe_info_file)
' "${TESTS_SANDBOX}/yt_recipe_info.json" "${BUILD_ROOT}/yt/python/yt/local/bin/yt_local"

    run_pytest ${INTEGRATION_ROOTS} || status=$?

    flow_recipe stop --cluster-names "${YT_CLUSTER_NAMES}" || true
fi

exit $status
