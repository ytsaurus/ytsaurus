#!/bin/bash -eux

yum install -y python3-devel

python3 /ytsaurus/ya make \
    /ytsaurus/yt/yt/python/yson_shared \
    /ytsaurus/yt/yt/python/driver/rpc_shared \
    -T --build=relwithdebinfo \
    -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON -DPYTHON_CONFIG=python3-config -DPYTHON_BIN=python3 -DOS_SDK=ubuntu-14 \
    --output /build

strip --remove-section=.gnu_debuglink /build/yt/yt/python/yson_shared/*.so /build/yt/yt/python/driver/rpc_shared/*.so

