#!/bin/bash

set -euo pipefail

source_root=$(cd "$(dirname "$0")/../../.." && pwd)
build_root=$1
target_arch=${2:-$(uname -m)}
build_flags=(-DOS_SDK=local)
wheel_flags=(--py-limited-api cp34)

if [[ $(uname -s) == Linux ]]; then
    wheel_flags+=(--plat-name "linux_$target_arch")
    if [[ $target_arch == aarch64 ]]; then
        # Use the SDK headers, but link against the manylinux2014 libraries and CRT.
        sysroot="$build_root/sysroot"
        python_include=("$sysroot"/opt/_internal/cpython-3.11.*/include/python3.11)
        gcc_lib=("$sysroot"/opt/rh/devtoolset-*/root/usr/lib/gcc/aarch64-redhat-linux/*)
        build_flags=(
            --target-platform=default-linux-aarch64
            -DOS_SDK=ubuntu-16
            "-DPYTHON_INCLUDE=-I${python_include[0]}"
            "-DUSER_LDFLAGS=--sysroot=$sysroot -B${gcc_lib[0]}"
        )
    fi
    build_flags+=(
        -DPROVIDE_GETRANDOM_GETENTROPY=yes
        -DUSER_CXXFLAGS=-UHAVE___CXA_THREAD_ATEXIT_IMPL
    )
fi

python3 -m pip install setuptools wheel abi3audit
python3 -m pip install -e "$source_root/yt/python/packages"

cd "$source_root/yt"
python3 ../ya --intent build make \
    yt/python/driver/rpc_shared \
    --build=release \
    -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON \
    -DPYTHON_CONFIG="$(command -v python3-config)" -DPYTHON_BIN="$(command -v python3)" \
    "${build_flags[@]}" \
    --output "$build_root"

mkdir -p "$build_root/package"
cp -R "$source_root/yt/yt/python/yt_driver_rpc_bindings" "$build_root/package/"
cp "$build_root/yt/yt/python/driver/rpc_shared/driver_rpc_lib.so" \
    "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"
cp "$source_root/yt/python/packages/ytsaurus-rpc-driver/setup.py" "$build_root/package/"

case $(uname -s) in
    Linux)
        python3 ../ya tool strip --strip-debug \
            "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"
        ;;
    Darwin) strip -S "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so" ;;
esac

cd "$build_root/package"
python3 setup.py bdist_wheel "${wheel_flags[@]}" --dist-dir "$build_root/wheels"

case $(uname -s) in
    Linux)
        python3 -m pip install auditwheel patchelf
        auditwheel_flags=(--plat "manylinux2014_$target_arch")
        if [[ $target_arch == aarch64 ]]; then
            # Cross repair infers the architecture; the runtime check requires glibc 2.17.
            auditwheel_flags=(--plat auto --ldpaths "$sysroot/lib64:$sysroot/usr/lib64")
        fi
        auditwheel repair "$build_root"/wheels/*.whl \
            "${auditwheel_flags[@]}" --wheel-dir "$build_root/dist"
        ;;
    Darwin)
        python3 -m pip install delocate
        delocate-wheel --require-archs "$(uname -m)" \
            --wheel-dir "$build_root/dist" "$build_root"/wheels/*.whl
        ;;
esac

abi3audit --strict --summary "$build_root"/dist/*.whl
