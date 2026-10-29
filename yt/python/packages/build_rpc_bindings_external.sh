#!/bin/bash

set -euo pipefail

source_root=$(cd "$(dirname "$0")/../../.." && pwd)
build_root=$1
target_arch=$(uname -m)
python3_config=$(command -v python3-config)
wheel_flags=(--py-limited-api cp34)

if [[ $(uname -s) == Linux ]]; then
    wheel_flags+=(--plat-name "linux_$target_arch")
    # Build against ya's default Linux SDK and keep __cxa_thread_atexit_impl weak to support glibc 2.17.
    ya_flags=(-DUSER_CXXFLAGS=-UHAVE___CXA_THREAD_ATEXIT_IMPL)
    repair_packages=(auditwheel patchelf)
    repair_command=(auditwheel repair --plat "manylinux_2_17_$target_arch" --only-plat)
elif [[ $(uname -s) == Darwin ]]; then
    wheel_flags+=(--plat-name "macosx_${MACOSX_DEPLOYMENT_TARGET//./_}_$target_arch")
    ya_flags=(-DOS_SDK=local)
    repair_packages=(delocate)
    repair_command=(delocate-wheel --require-archs "$target_arch")
else
    echo "Unsupported OS: $(uname -s)" >&2
    exit 1
fi

python3 -m pip install --upgrade setuptools wheel abi3audit "${repair_packages[@]}"
python3 -m pip install -e "$source_root/yt/python/packages"

cd "$source_root"
# MACOSX_DEPLOYMENT_TARGET is for wheel packaging; keep ya make on its own deployment target.
env -u MACOSX_DEPLOYMENT_TARGET ./ya make \
    yt/yt/python/driver/rpc_shared \
    --build=release \
    -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON \
    -DPYTHON_CONFIG="$python3_config" -DPYTHON_BIN="$(command -v python3)" \
    "${ya_flags[@]}" \
    --output "$build_root"

mkdir -p "$build_root/package"
cp -R "$source_root/yt/yt/python/yt_driver_rpc_bindings" "$build_root/package/"
cp "$build_root/yt/yt/python/driver/rpc_shared/driver_rpc_lib.so" \
    "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"
cp "$source_root/yt/python/packages/ytsaurus-rpc-driver/setup.py" "$build_root/package/"

./ya tool strip \
    "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"

cd "$build_root/package"
python3 setup.py bdist_wheel "${wheel_flags[@]}" --dist-dir "$build_root/wheels"

"${repair_command[@]}" \
    --wheel-dir "$build_root/dist" "$build_root"/wheels/*.whl

abi3audit --strict --summary "$build_root"/dist/*.whl
