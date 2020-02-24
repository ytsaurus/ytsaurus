#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from prepare_source_tree_helpers import (
    apply_multiple,
    cp_r,
    logger,
    replace,
    replace_symlink,
    rm_rf,
)

import argparse
import glob


YT_PYTHON_PACKAGE_LIST = [
    "argcomplete",
    "certifi",
    "dill",
    "requests",
    "six",
    "distro",
    "urllib3",
    "chardet",
    "idna",
]

CONTRIB_PYTHON_PACKAGE_LIST = [
    "simplejson",
    "cloudpickle",
    "backports_abc",
    "singledispatch",
    "tornado",
    "tqdm",
]

PY23_BINARIES = [
    "yt/wrapper/bin/yt",
    "yt/wrapper/bin/yt-fuse",
    "yt/wrapper/bin/yt-admin",
    "yt/wrapper/bin/yt-job-tool",
    "yt/wrapper/bin/mapreduce-yt",
]

YT_PREFIX_BINARIES = [
    "yt/tools/bin/add_user.py",
    "yt/tools/bin/checksum.py",
    "yt/tools/bin/lock.py",
    "yt/tools/bin/set_account.py",
]


def prepare_python_source_tree(python_root, yt_root, prepare_binary_symlinks=True, prepare_bindings=True):
    def python_contrib_path(path):
        return os.path.join(python_root, "contrib", path)

    packages_dir = os.path.join(python_root, "yt", "packages")

    # Cleanup old stuff
    for fname in os.listdir(packages_dir):
        for package in YT_PYTHON_PACKAGE_LIST + CONTRIB_PYTHON_PACKAGE_LIST:
            if fname.startswith(package):
                rm_rf(os.path.join(packages_dir, fname))

    for package_name in YT_PYTHON_PACKAGE_LIST:
        logger.info("Preparing package %s", package_name)
        files_to_copy = glob.glob(
            "{python_root}/contrib/python-{package_name}/{package_name}*.py".format(
                python_root=python_root,
                package_name=package_name))
        files_to_copy += glob.glob(
            "{python_root}/contrib/python-{package_name}/{package_name}".format(
                python_root=python_root,
                package_name=package_name))
        for path in files_to_copy:
            cp_r(path, packages_dir)

    for package_name in CONTRIB_PYTHON_PACKAGE_LIST:
        logger.info("Preparing package %s", package_name)
        # By some reason tqdm has both tqdm dir and tqdm.py file, second must be ignored.
        files_to_copy = []
        if package_name != "tqdm":
            files_to_copy += glob.glob(
                "{yt_root}/contrib/python/{package_name}/{package_name}*.py".format(
                    yt_root=yt_root,
                    package_name=package_name))
        files_to_copy += glob.glob(
            "{yt_root}/contrib/python/{package_name}/{package_name}".format(
                yt_root=yt_root,
                package_name=package_name))
        for path in files_to_copy:
            cp_r(path, packages_dir)

    replace(python_contrib_path("python-decorator/src/decorator.py"), packages_dir)
    replace(python_contrib_path("python-fusepy/fuse.py"), packages_dir)

    if prepare_bindings:
        replace(os.path.join(yt_root, "yt/python/yt_yson_bindings"), python_root)
        replace(os.path.join(yt_root, "yt/python/yt_driver_bindings"), python_root)
        yt_driver_rpc_bindings = os.path.join(yt_root, "yt/python/yt_driver_rpc_bindings")
        if os.path.exists(yt_driver_rpc_bindings):
            replace(yt_driver_rpc_bindings, python_root)

    if prepare_binary_symlinks:
        for binary in PY23_BINARIES:
            binary_path = os.path.join(python_root, binary)
            for suffix in ("2", "3"):
                replace_symlink(binary_path, binary_path + suffix)

        for binary in YT_PREFIX_BINARIES:
            binary_path = os.path.join(python_root, binary)
            dirname, basename = os.path.split(binary_path)
            link_path = os.path.join(dirname, "yt_" + basename)
            replace_symlink(binary_path, link_path)


def get_default_python_root():
    return apply_multiple(times=2, func=os.path.dirname, argument=os.path.abspath(__file__))


def get_default_yt_root():
    return apply_multiple(times=3, func=os.path.dirname, argument=os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python-root", default=get_default_python_root())
    parser.add_argument("--yt-root", default=get_default_yt_root())
    args = parser.parse_args()

    prepare_python_source_tree(python_root=args.python_root, yt_root=args.yt_root)


if __name__ == "__main__":
    main()
