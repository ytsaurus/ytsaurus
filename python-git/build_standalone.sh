#!/bin/bash

CURRENT_DIR="$(pwd)"

if [ "$#" -ne 2  ]; then
    echo "Usage: build_standalone.sh package python-source-tree-root" && exit 1
fi

PACKAGE="$1" && shift
PYTHON_SOURCE_TREE_ROOT="$1" && shift

prepare_dir() {
    local dir=$1 && shift
    if [ -d "$dir" ]; then
        rm -rf "$dir"
    fi
    mkdir "$dir"
}

BUILD_DIR="$(pwd)/build_python"
INSTALL_DIR="$(pwd)/install_python"

cleanup() {
    rm -rf "$BUILD_DIR" "$INSTALL_DIR"
    rm -rf setup.py requirements.txt debian build dist *.egg-info Makefile
}

cleanup

prepare_dir "$BUILD_DIR"
prepare_dir "$INSTALL_DIR"

cd "$BUILD_DIR"

"$PYTHON_SOURCE_TREE_ROOT/configure" --enable-unicode=ucs4 --prefix="$INSTALL_DIR"
EXTRA_CFLAGS="-O2 -g -DNDEBUG" make -j $(grep -c ^processor /proc/cpuinfo) && make install

cd "$CURRENT_DIR"

export PYTHONHOME="$INSTALL_DIR"

# Installing pip
curl "https://bootstrap.pypa.io/get-pip.py" | "$INSTALL_DIR/bin/python"

# Installing requirements
if [ -f "$PACKAGE/requirements.txt" ]; then
    "$INSTALL_DIR/bin/python" -m pip install \
        -r "$PACKAGE/requirements.txt" \
        --ignore-installed \
        -i https://pypi.yandex-team.ru/simple
fi

# Preparing debian directory and setup.py
prepare_dir debian
cp "$PACKAGE/debian/changelog" debian
cp "$PACKAGE/debian/control" debian
cp "$PACKAGE/setup.py" .
cp standalone-package/debian/rules debian
cp standalone-package/debian/compat debian

# Building package wheel and installing package
"$INSTALL_DIR/bin/python" setup.py bdist_wheel
"$INSTALL_DIR/bin/python" -m pip install dist/*.whl --ignore-installed

# Replacing python in shebangs to newly built
grep "#\!" -R "$INSTALL_DIR/bin" -l -I | xargs sed -i '1 s/^.*$/#\!\/opt\/venvs\/'"$PACKAGE"'\/bin\/python/g'

unset PYTHONHOME

# Building package
cp standalone-package/Makefile .

# This file must be used with "source bin/activate" *from bash*
# you cannot run it directly. It allowes to deploy standalone
# packages and packages built by dh-virtualenv without modifying
# sv run script.
echo "export PYTHONHOME=/opt/venvs/$PACKAGE" >"$INSTALL_DIR/bin/activate"

PACKAGE="$PACKAGE" PYTHON_INSTALLDIR="$INSTALL_DIR" dpkg-buildpackage -b

cleanup
