#!/bin/bash

# This script installs ytsaurus-client==0.13.999-dev0 package from local repo (and links package code with it)

set -e

pip3 install wheel
pip3 install six

ARC_ROOT=$(realpath $(dirname $0)/../../../..)
BUILD_DIR="/tmp/yt_tmp_build_dir"

test -d $BUILD_DIR || mkdir -p $BUILD_DIR
rm -rf $BUILD_DIR/*

pip3 install -e $ARC_ROOT/yt/python/packages

# TODO: proto
# python3 $ARC_ROOT/yt/python/packages/yt_setup/generate_python_proto.py --source-root $ARC_ROOT --output $BUILD_DIR

# TODO: yson
# (cd $A/yt/yt/python/yson_shared && ya make)

# package content
cd $ARC_ROOT/yt/python/packages
python3 -m yt_setup.prepare_python_modules --source-root $ARC_ROOT --output-path $BUILD_DIR --build-root $ARC_ROOT --use-modules-from-contrib

cd $BUILD_DIR
python3 -c 'import yt.wrapper' && echo "import yt.wraper - ok"

# make links
for d in clickhouse entry skiff type_info wrapper yson cli cpp_wrapper environment wire_format ypath; do
	rm -rf $BUILD_DIR/yt/$d
	ln -sf $ARC_ROOT/yt/python/yt/$d $BUILD_DIR/yt/$d
done
python3 -c 'import yt.wrapper' && echo "import linked yt.wraper - ok"

rm -rf $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build/*
mv ./* $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build/
cp $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build_setup.py $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build/setup.py

cd $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build
python3 -c 'import yt.wrapper' && echo "import build yt.wraper - ok"

pip3 uninstall -y ytsaurus-yson ytsaurus-client || true
pip3 install ytsaurus-yson --no-deps
pip3 install -e $ARC_ROOT/yt/python/packages/ytsaurus-client-trunk-dev/build

pip3 list | grep ytsaurus

rm -rf $BUILD_DIR

cd $HOME
python3 -c 'import yt.wrapper; print("yt.wrapper version - {}".format(yt.wrapper.VERSION))' && echo "yt.wraper - ok"

echo -e "\n\nAll ok"
