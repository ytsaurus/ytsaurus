#!/bin/bash -ex
# Usage:
#   build_cmake.sh configure     --source-path P --build-path P --build-type T \
#                                --ccache-remote-storage S [--ccache-base-dir P] \
#                                [--cxx-flags-init F] [--read-only-cache]
#   build_cmake.sh build-python  --source-path P --build-path P --venv-path P --commit-hash H
#   build_cmake.sh build         --build-path P [--targets "t1 t2 ..."]
#   build_cmake.sh move-binaries --build-path P --output-build-path P

cmd_configure() {
    local source_path="" build_path="" build_type="" ccache_remote_storage=""
    local ccache_base_dir="" read_only_cache="false" cxx_flags_init=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --source-path)           source_path="$2"; shift 2;;
            --build-path)            build_path="$2"; shift 2;;
            --build-type)            build_type="$2"; shift 2;;
            --ccache-remote-storage) ccache_remote_storage="$2"; shift 2;;
            --ccache-base-dir)       ccache_base_dir="$2"; shift 2;;
            --cxx-flags-init)        cxx_flags_init="$2"; shift 2;;
            --read-only-cache)       read_only_cache="true"; shift;;
            *) echo "configure: unknown arg $1" >&2; exit 1;;
        esac
    done

    source_path="$(realpath "${source_path}")"
    mkdir -p "${build_path}"
    build_path="$(realpath "${build_path}")"
    [ -z "${ccache_base_dir}" ] && ccache_base_dir="$(realpath "${build_path}/..")"

    mkdir -p ~/.conan2/
    cat > ~/.conan2/global.conf <<EOF
core.net.http:timeout=5
core.sources:download_urls=["origin", "https://c3i.jfrog.io/artifactory/conan-center-backup-sources"]
EOF

    local extra_cmake_args=()
    [ -n "${cxx_flags_init}" ] && extra_cmake_args+=(-DCMAKE_CXX_FLAGS_INIT="${cxx_flags_init}")

    cd "${build_path}"
    cmake \
        -G Ninja \
        -DCMAKE_BUILD_TYPE="${build_type}" \
        -DCMAKE_TOOLCHAIN_FILE="${source_path}/clang.toolchain" \
        -DCMAKE_C_COMPILER_LAUNCHER=ccache \
        -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
        -DREQUIRED_LLVM_TOOLING_VERSION=18 \
        -DCMAKE_PROJECT_TOP_LEVEL_INCLUDES="${source_path}/cmake/conan_provider.cmake" \
        "${extra_cmake_args[@]}" \
        "${source_path}"

    ccache --set-config base_dir="${ccache_base_dir}"
    ccache --set-config remote_only=true
    ccache --set-config remote_storage="${ccache_remote_storage}"
    if [ "${read_only_cache}" == "true" ]; then
        ccache --set-config read_only=true
    fi
}

cmd_build_python() {
    local source_path="" build_path="" venv_path="" commit_hash=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --source-path) source_path="$2"; shift 2;;
            --build-path)  build_path="$2"; shift 2;;
            --venv-path)   venv_path="$2"; shift 2;;
            --commit-hash) commit_hash="$2"; shift 2;;
            *) echo "build-python: unknown arg $1" >&2; exit 1;;
        esac
    done

    source_path="$(realpath "${source_path}")"
    build_path="$(realpath "${build_path}")"
    venv_path="$(realpath "${venv_path}")"

    cd "${build_path}"
    ninja yson_lib driver_lib driver_rpc_lib
    ccache --show-stats

    strip yt/yt/python/yson_shared/libyson_lib.so
    strip yt/yt/python/driver/native_shared/libdriver_lib.so
    strip yt/yt/python/driver/rpc_shared/libdriver_rpc_lib.so

    source "${venv_path}/bin/activate"
    YTSAURUS_COMMIT_HASH="${commit_hash}" "${source_path}/yt/python/packages/build_ytsaurus_packages.sh" \
        --ytsaurus-source-path "${source_path}" \
        --ytsaurus-build-path "${build_path}"
}

cmd_build() {
    local build_path="" targets=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --build-path) build_path="$2"; shift 2;;
            --targets)    targets="$2"; shift 2;;
            *) echo "build: unknown arg $1" >&2; exit 1;;
        esac
    done

    build_path="$(realpath "${build_path}")"
    cd "${build_path}"
    if [ -z "${targets}" ]; then
        ninja
    else
        # shellcheck disable=SC2086
        ninja ${targets}
    fi
    ccache --show-stats
}

cmd_move_binaries() {
    local build_path="" output_build_path=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --build-path)        build_path="$2"; shift 2;;
            --output-build-path) output_build_path="$2"; shift 2;;
            *) echo "move-binaries: unknown arg $1" >&2; exit 1;;
        esac
    done

    build_path="$(realpath "${build_path}")"
    mkdir -p "${output_build_path}"
    output_build_path="$(realpath "${output_build_path}")"

    cd "${build_path}"

    for unittester_binary in $(find . -name "unittester-*" -type f | grep -v "unittester-containers"); do
        strip "${unittester_binary}"
        mv "${unittester_binary}" "${output_build_path}"
    done

    for ut_binary in $(find . -name "*-ut" -type f); do
        strip "${ut_binary}"
        mv "${ut_binary}" "${output_build_path}"
    done

    local scheduler_simulator="yt/yt/tools/scheduler_simulator/bin/scheduler_simulator"
    strip "${scheduler_simulator}"
    mv "${scheduler_simulator}" "${output_build_path}"

    local ytserver_all="yt/yt/server/all/ytserver-all"
    strip "${ytserver_all}"
    mkdir -p "${output_build_path}/yt/yt/server/all/"
    mv "${ytserver_all}" "${output_build_path}/yt/yt/server/all/"

    for so_dir in \
        yt/yt/python/driver/native_shared \
        yt/yt/python/driver/rpc_shared \
        yt/yt/python/yson_shared; do
        mkdir -p "${output_build_path}/${so_dir}"
        mv "${so_dir}"/*.so "${output_build_path}/${so_dir}"
    done
}

action="${1:-}"
[ -z "${action}" ] && { echo "Usage: $0 {configure|build-python|build|move-binaries} [args]" >&2; exit 1; }
shift

case "${action}" in
    configure)     cmd_configure "$@";;
    build-python)  cmd_build_python "$@";;
    build)         cmd_build "$@";;
    move-binaries) cmd_move_binaries "$@";;
    *) echo "Unknown action: ${action}" >&2; exit 1;;
esac
