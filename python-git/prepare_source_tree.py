#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil
import logging

logger = logging.getLogger("python_repo")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

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

def rm_rf(path):
    """remove recursive"""
    logger.info("Remove %s", path)
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.unlink(path)


def cp_r(path, dest_dir):
    """copy recursive"""
    logger.info("Copy %s to %s", path, dest_dir)
    assert os.path.isdir(dest_dir)
    if os.path.isdir(path):
        shutil.copytree(path, os.path.join(dest_dir, os.path.basename(path)))
    else:
        shutil.copy2(path, dest_dir)

def ln_s(path, link):
    """create symlink"""
    logger.info("Making symlink from %s to %s", path, link)
    if not os.path.exists(link):
        os.symlink(path, link)

def replace(path, dest_dir):
    dst_path = os.path.join(dest_dir, os.path.basename(path))
    if os.path.exists(dst_path):
        rm_rf(dst_path)
    cp_r(path, dest_dir)


def replace_symlink(source, destination):
    if os.path.lexists(destination):
        os.remove(destination)
    os.symlink(source, destination)


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
        files_to_copy = glob.glob(
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

    if prepare_binary_symlinks:
        for binary in PY23_BINARIES:
            binary_path = os.path.join(python_root, binary)
            for suffix in ("2", "3"):
                ln_s(binary_path, binary_path + suffix)

        for binary in YT_PREFIX_BINARIES:
            binary_path = os.path.join(python_root, binary)
            dirname, basename = os.path.split(binary_path)
            link_path = os.path.join(dirname, "yt_" + basename)
            ln_s(binary_path, link_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python-root", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--yt-root", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parser.parse_args()

    prepare_python_source_tree(python_root=args.python_root, yt_root=args.yt_root)


if __name__ == "__main__":
    main()
