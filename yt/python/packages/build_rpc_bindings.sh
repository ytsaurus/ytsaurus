#!/bin/bash

set -euo pipefail

source_root=$(cd "$(dirname "$0")/../../.." && pwd)
build_root=$1
target_arch=$(uname -m)
wheel_flags=(--py-limited-api cp34)

if [[ $(uname -s) == Linux ]]; then
    wheel_flags+=(--plat-name "linux_$target_arch")
    repair_packages=(auditwheel patchelf)
    repair_command=(auditwheel repair --plat "manylinux_2_28_$target_arch" --only-plat)
elif [[ $(uname -s) == Darwin ]]; then
    wheel_flags+=(--plat-name "macosx_${MACOSX_DEPLOYMENT_TARGET//./_}_$target_arch")
    repair_packages=(delocate)
    repair_command=(delocate-wheel --require-archs "$target_arch")
fi

python3 -m pip install setuptools wheel abi3audit
python3 -m pip install -e "$source_root/yt/python/packages"

cd "$source_root/yt"
../ya make \
    yt/python/driver/rpc_shared \
    --build=release \
    -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON \
    -DPYTHON_CONFIG="$(command -v python3-config)" -DPYTHON_BIN="$(command -v python3)" \
    -DOS_SDK=local \
    --output "$build_root"

mkdir -p "$build_root/package"
cp -R "$source_root/yt/yt/python/yt_driver_rpc_bindings" "$build_root/package/"
cp "$build_root/yt/yt/python/driver/rpc_shared/driver_rpc_lib.so" \
    "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"
cp "$source_root/yt/python/packages/ytsaurus-rpc-driver/setup.py" "$build_root/package/"

../ya tool strip --strip-debug \
    "$build_root/package/yt_driver_rpc_bindings/driver_rpc_lib.abi3.so"

cd "$build_root/package"
python3 setup.py bdist_wheel "${wheel_flags[@]}" --dist-dir "$build_root/wheels"

python3 -m pip install "${repair_packages[@]}"
"${repair_command[@]}" \
    --wheel-dir "$build_root/dist" "$build_root"/wheels/*.whl

abi3audit --strict --summary "$build_root"/dist/*.whl
