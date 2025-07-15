#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import glob
import os
import re
import sys

from .os_helpers import (
    apply_multiple,
    cp_r,
    cp,
    ignore_patterns,
    logger,
    replace,
    replace_symlink,
    rm_rf,
)


YT_PYTHON_PACKAGE_LIST = [
    ("dill", "dill/py2"),
    "requests",
    ("urllib3", "urllib3/src"),
    "chardet",
    ("typing_extensions", "typing-extensions"),
]

CONTRIB_PYTHON_PACKAGE_LIST = [
    ("argcomplete", "argcomplete/py2"),
    ("simplejson", "simplejson/py2"),
    ("cloudpickle", "cloudpickle/py2"),
    ("charset_normalizer", "charset-normalizer"),
    ("decorator", "decorator/py2"),
    ("distro", "distro/py2"),
    ("backports_abc", "backports-abc"),
    "singledispatch",
    ("tornado", "tornado/tornado-4"),
    ("tqdm", "tqdm/py2"),
    ("idna", "idna/py2"),
    ("six", "six/py2"),
    ("attr", "attrs/py2"),
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


def cp_r_755(src, dst):
    cp_r(src, dst, permissions=0o755, ignore=ignore_patterns("*.pyc"))


def fix_type_info_package(type_info_path):
    for root, dirs, files in os.walk(type_info_path):
        for file in files:
            try:
                with open(os.path.join(root, file)) as fin:
                    data = fin.read()
                data = data.replace(
                    "import six\n",
                    "import yt.packages.six as six\n")
                with open(os.path.join(root, file), "w") as fout:
                    fout.write(data)
            except BaseException as ex:
                logger.error("Failed while patching file %s %s", os.path.join(root, file), ex)
                raise


def patch_os_files(patcheable_files, to_os=True):
    for file_name in patcheable_files:
        if file_name.endswith(".py"):
            with open(file_name) as fin:
                data = fin.read()

            if to_os:
                data = re.sub(r"(\n *# *__IF_NOT_OS: *\n *)([^#])", r"\1# \2", data)
                data = re.sub(r"(\n *# *__IF_OS: *\n *)#+ *", r"\1", data)
            else:
                data = re.sub(r"(\n *# *__IF_NOT_OS: *\n *)#+ *", r"\1", data)
                data = re.sub(r"(\n *# *__IF_OS: *\n *)([^#])", r"\1# \2", data)

            with open(file_name, "w") as fout:
                fout.write(data)
        else:
            raise RuntimeError("Wrong filename extension: {}".format(file_name))


def prepare_python_modules(
        source_root,
        output_path,
        build_root,
        use_modules_from_contrib=False,
        prepare_binary_symlinks=False,
        prepare_bindings=True,
        prepare_bindings_libraries=True,
        bindings_libraries=None,
        should_fix_type_info_package=True,
        should_patch_os_files=False):
    """Prepares python libraries"""

    python_root = os.path.join(source_root, "yt/python")

    def python_contrib_path(path):
        return os.path.join(python_root, "contrib", path)

    def prepare_bindings_module(module_path, library_path, name):
        replace(os.path.join(source_root, module_path), output_path)

        if not prepare_bindings_libraries:
            return

        pattern = "*{name}.so"
        if sys.platform == "darwin":
            pattern = "*{name}.dylib"
        dir = os.path.join(build_root, library_path)
        lib_paths = glob.glob(os.path.join(dir, pattern.format(name=name)))
        if not lib_paths:
            raise RuntimeError("Bindings library {name} was not found in {dir}".format(name=name, dir=dir))
        if len(lib_paths) > 1:
            raise RuntimeError("Several bindings libraries {name} were found in {dir}: {lib_paths}"
                               .format(name=name, dir=dir, lib_paths=", ".join(lib_paths)))
        lib_path = lib_paths[0]
        cp(
            lib_path,
            os.path.join(output_path, "{path}/{name}.so".format(path=os.path.basename(module_path), name=name)))

    cp_r_755(os.path.join(python_root, "yt"), output_path)
    yt_data_path = os.path.join(python_root, "yt", "data")
    if os.path.exists(yt_data_path):
        rm_rf(yt_data_path)

    packages_dir = os.path.join(output_path, "yt", "packages")

    if should_fix_type_info_package:
        fix_type_info_package(os.path.join(output_path, "yt", "type_info"))

    if should_patch_os_files:
        patch_os_files([os.path.join(output_path, "yt/wrapper/client.py")], to_os=True)

    # Prepare contribs
    for package_name in YT_PYTHON_PACKAGE_LIST:
        if isinstance(package_name, tuple):
            package_name, package_relative_path = package_name
        else:
            package_relative_path = package_name

        logger.info("Preparing package %s", package_name)
        files_to_copy = glob.glob(
            "{python_root}/contrib/python-{package_relative_path}/{package_name}*.py".format(
                python_root=python_root,
                package_relative_path=package_relative_path,
                package_name=package_name))
        files_to_copy += glob.glob(
            "{python_root}/contrib/python-{package_relative_path}/{package_name}".format(
                python_root=python_root,
                package_relative_path=package_relative_path,
                package_name=package_name))
        for path in files_to_copy:
            cp_r_755(path, packages_dir)

    if use_modules_from_contrib:
        assert os.path.exists(os.path.join(source_root, "contrib/python"))
        for package_name in CONTRIB_PYTHON_PACKAGE_LIST:
            if isinstance(package_name, tuple):
                package_name, package_relative_path = package_name
            else:
                package_relative_path = package_name

            logger.info("Preparing package %s", package_name)

            def find_package_files(contrib_path):
                files_to_copy = glob.glob(
                    "{contrib_path}/{package_relative_path}/{package_name}*.py".format(
                        contrib_path=contrib_path,
                        package_relative_path=package_relative_path,
                        package_name=package_name))
                files_to_copy += glob.glob(
                    "{contrib_path}/{package_relative_path}/{package_name}".format(
                        contrib_path=contrib_path,
                        package_relative_path=package_relative_path,
                        package_name=package_name))
                return files_to_copy

            files_to_copy = find_package_files("{source_root}/contrib/python".format(source_root=source_root))

            if not files_to_copy:
                files_to_copy += find_package_files("{source_root}/contrib/deprecated/python".format(source_root=source_root))
                if files_to_copy:
                    logger.warning("Package %s was not found at default contrib path and was taken from contrib/deprecated", package_name)
                else:
                    logger.error("Package %s not found (%s)", package_name, package_relative_path)

            for path in files_to_copy:
                cp_r_755(path, packages_dir)

    # Replace certificate.
    cp_r_755(os.path.join(source_root, "certs", "cacert.pem"), os.path.join(packages_dir, "requests"))

    replace(python_contrib_path("python-fusepy/fuse.py"), packages_dir)

    if prepare_bindings:
        if bindings_libraries is None or "yson_lib" in bindings_libraries:
            prepare_bindings_module(
                module_path="yt/yt/python/yt_yson_bindings",
                library_path="yt/yt/python/yson_shared/",
                name="yson_lib")
        if bindings_libraries is None or "driver_lib" in bindings_libraries:
            prepare_bindings_module(
                module_path="yt/yt/python/yt_driver_bindings",
                library_path="yt/yt/python/driver/native_shared/",
                name="driver_lib")
        if bindings_libraries is None or "driver_rpc_lib" in bindings_libraries:
            prepare_bindings_module(
                module_path="yt/yt/python/yt_driver_rpc_bindings",
                library_path="yt/yt/python/driver/rpc_shared/",
                name="driver_rpc_lib")

    if prepare_binary_symlinks:
        for binary in YT_PREFIX_BINARIES:
            binary_path = os.path.join(python_root, binary)
            dirname, basename = os.path.split(binary_path)
            link_path = os.path.join(dirname, "yt_" + basename)
            replace_symlink(binary_path, link_path)


def get_default_source_root():
    return apply_multiple(times=5, func=os.path.dirname, argument=os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", default=get_default_source_root())
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--build-root")
    parser.add_argument("--fix-type-info-package", action="store_true", default=False)
    parser.add_argument("--patch-os-files", action="store_true", default=False)
    parser.add_argument("--use-modules-from-contrib", action="store_true", default=False)
    parser.add_argument("--prepare-bindings-libraries", action="store_true", default=False)
    parser.add_argument("--bindings-library", action="append",
                        choices=["yson_lib", "driver_lib", "driver_rpc_lib"], default=None)
    args = parser.parse_args()

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    prepare_python_modules(
        source_root=args.source_root,
        output_path=args.output_path,
        build_root=args.build_root,
        use_modules_from_contrib=args.use_modules_from_contrib,
        should_fix_type_info_package=args.fix_type_info_package,
        should_patch_os_files=args.patch_os_files,
        prepare_bindings_libraries=args.prepare_bindings_libraries,
        bindings_libraries=args.bindings_library,
    )


if __name__ == "__main__":
    main()
