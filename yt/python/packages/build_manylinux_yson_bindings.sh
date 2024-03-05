#!/bin/bash -eux

yum install -y python3-devel
python3 /ytsaurus/ya make /ytsaurus/yt/yt/python/yson_shared -T --build=relwithdebinfo -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON -DPYTHON_CONFIG=python3-config -DPYTHON_BIN=python3 --output /ya_build
