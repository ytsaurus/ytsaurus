#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../packaging"))

from os_helpers import copy_content, clean_path, run_captured, run, cwd, mkdirp
from teamcity_helpers import teamcity_message

import pypi
import debian

import argparse
import contextlib
import shutil
import tempfile

class PackageError(Exception):
    pass

def comma_separated_list(val):
    return val.strip(",").split(",")

@contextlib.contextmanager
def create_working_directory(working_directory, keep_temp_files):
    if working_directory is None:
        working_directory = os.getcwd()
    tmp_dir = tempfile.mkdtemp(dir=working_directory)
    try:
        yield working_directory
    finally:
        if not keep_temp_files:
            shutil.rmtree(tmp_dir)

def prepare_debian_files(changelog_path, package_name, python_suffix, build_dir):
    teamcity_message("Preparing debian files")
    shutil.copyfile(changelog_path, os.path.join(build_dir, "debian/changelog"))
    with open(os.path.join(build_dir, "debian/control"), "rw+") as control_file:
        data = control_file.read()
        data = data.replace("%%PACKAGE_NAME%%", package_name)
        data = data.replace("%%PY_VERSION_SUFFIX%%", python_suffix)
        data = data.replace("%%PY_VERSION_RESTRICTION%%",
                            ">=3.4" if python_suffix == "3" else ">= 2.6")
        control_file.seek(0)
        control_file.write(data)
        control_file.truncate()
    with open(os.path.join(build_dir, "debian/changelog"), "rw+") as changelog_file:
        data = changelog_file.read()
        data = data.replace("package-name", package_name)
        changelog_file.seek(0)
        changelog_file.write(data)
        changelog_file.truncate()

def prepare_library_file(library_path, debug, python_suffix, python_module_dir):
    teamcity_message("Preparing library file")

    def get_debug_name(name):
        parts = name.split(".")
        shift = 1
        if parts[-2] == "abi3":
            shift = 2
        return ".".join(parts[:-shift] + ["dbg"] + parts[-shift:])

    target_library_name = os.path.basename(library_path)

    if python_suffix == "3":
        # See: https://www.python.org/dev/peps/pep-3149/#pep-384
        name, ext = target_library_name.rsplit(".", 1)
        target_library_name = "{}.abi3.{}".format(name, ext)

    target_library_path = os.path.join(python_module_dir, target_library_name)
    shutil.copyfile(library_path, target_library_path)

    # TODO(ignat): extraction of debug symbols is not supported for MacOS.
    if not debug and target_library_name.endswith(".so"):
        debug_target_library_name = get_debug_name(target_library_name)
        debug_target_library_path = os.path.join(python_module_dir, debug_target_library_name)
        run(["objcopy", "--only-keep-debug", target_library_path, debug_target_library_path])
        run(["strip", "--remove-section=.gnu_debuglink", target_library_path])
        run(["objcopy", "--add-gnu-debuglink={0}".format(debug_target_library_path), target_library_path])

def prepare_version_file(checkout_directory, python_module_dir):
    teamcity_message("Preparing version file")

    package_version = debian.get_local_package_version()
    commit_hash = run_captured(["git", "rev-parse", "--short", "HEAD"], cwd=checkout_directory).strip()
    with open(os.path.join(python_module_dir, "version.py"), "w") as fout:
        fout.write('VERSION = "{0}"\n'.format(package_version))
        fout.write('COMMIT = "{0}"\n'.format(commit_hash))

def prepare_install_directory(python_module_dir, python_type, install_dir):
    teamcity_message("Preparing install directory")

    python_suffix = {
        "2": "2-7",
        "3": "3-4",
        "skynet": "skynet",
    }[python_type]

    module_dir = os.path.join(install_dir, "lib/pyshared-" + python_suffix)
    mkdirp(module_dir)
    shutil.copytree(python_module_dir, os.path.join(module_dir, os.path.basename(python_module_dir)))

def get_python_package_name(build_dir):
    import imp
    setup = imp.load_source("setup", os.path.join(build_dir, "setup.py"))
    return setup.PACKAGE_NAME

def build_pypi_package(python_type, python_suffix, upload, build_dir):
    teamcity_message("Building pypi package")

    package_version = debian.get_local_package_version()

    env = {}
    python = "python" + python_suffix
    pypi_package_name = get_python_package_name(build_dir)
    if python_type == "skynet":
        pypi_package_name += "-skynet"
        env["PYTHON_SUFFIX"] = "-skynet"
        python = "/skynet/python/bin/python"

    python_api_version = "cp27"
    if python_type == "3":
        python_api_version = "cp34"

    if package_version in pypi.get_package_versions(pypi_package_name, tag_filter_string=python_api_version):
        return

    #  Also we should change get_abi_version in python to return the same version as MacOS python.
    # macosx_platforms = ["macosx_{0}_{1}_{2}".format(v1, v2, platform_type) for v1 in (10,)
    #                     for v2 in xrange(9, 14) for platform_type in ("intel", "x86_64")]
    # for platform in platforms:
    #     args += ["--plat-name", platform]
    args = [python, "setup.py", "bdist_wheel"]
    if python_suffix == "3":
        args += ["--py-limited-api", "cp34"]
    if upload:
        args += ["upload", "-r", "yandex"]
    run(args, env=env)

def process_changes_files(upload, build_dir, repositories_to_upload):
    teamcity_message("Process changes file (upload: {})".format("true" if upload else "false"))

    package_dir = os.path.dirname(build_dir)
    changes_files = filter(lambda file: file.endswith(".changes"), os.listdir(package_dir))
    assert len(changes_files) == 1
    changes_file = os.path.join(package_dir, changes_files[0])
    if upload:
        for repository in repositories_to_upload:
            run(["dupload", "--to", repository, "--nomail", "--force", changes_file])
    os.remove(changes_file)

def build_debian_package(package_name, python_type, python_suffix, repositories, debug, upload, build_dir, install_dir):
    teamcity_message("Building debian package")

    package_version = debian.get_local_package_version()

    repositories_to_upload = []
    for repository in repositories:
        versions = debian.get_package_versions(package_name, repository)
        if package_version in versions:
            teamcity_message(
                "Package {0}={1} is already uploaded to {2}, skipping this repo"
                .format(package_name, package_version, repository))
        else:
            repositories_to_upload.append(repository)

    if not repositories_to_upload:
        return

    # Debian strip is not necessary, we do it manually in this script.
    deb_build_options = "nostrip"

    run(["dch", "-r", package_version, "'Resigned by teamcity'"])
    python = "python2" if not python_suffix else "python3"
    dpkg_buildpackage_env = {
        # NB: SOURCE_DIR can be used to manually copy module directory in debian/rules.
        "SOURCE_DIR": install_dir,
        "DEB_BUILD_OPTIONS": deb_build_options,
        "PYBUILD_DEST_INTERPRETER": python,
        "PYBUILD_PACKAGE_NAME": package_name
    }
    run(["dpkg-buildpackage", "-b"], env=dpkg_buildpackage_env)

    process_changes_files(upload, build_dir, repositories_to_upload)

def build_package(
    working_directory,
    checkout_directory,
    build_type,
    python_type,
    library_path,
    changelog_path,
    source_python_module_path,
    package_path,
    package_name,
    debian_repositories,
    debug,
    upload
):
    # TODO: add all parameters to this message
    teamcity_message("Preparing package {} with python to build "
                     "(working_directory: {}, checkout_directory: {}, build_type: {}, "
                     "python_type: {}, library_path: {}, changelog_path: {}, "
                     "source_python_module_path: {}, package_path: {}, package_name: {}, "
                     "debian_repositories: {}, debug: {}, upload: {})".format(
                         package_name, working_directory, checkout_directory, build_type,
                         python_type, library_path, changelog_path,
                         source_python_module_path, package_path, package_name,
                         debian_repositories, debug, upload))

    python_suffix = ""
    if python_type == "3":
        python_suffix = "3"

    dbg_suffix = "_dbg" if debug else ""
    dirname = "package_" + package_name.replace("-", "_") + "_" + build_type + dbg_suffix

    build_dir = os.path.join(working_directory, dirname, "build")
    clean_path(build_dir)

    install_dir = os.path.join(working_directory, dirname, "install")
    clean_path(install_dir)

    with cwd(build_dir):
        python_module_dir = os.path.join(build_dir, os.path.basename(source_python_module_path))

        copy_content(package_path, build_dir)
        shutil.copytree(os.path.join(checkout_directory, source_python_module_path), python_module_dir)

        prepare_debian_files(changelog_path, package_name, python_suffix, build_dir)
        prepare_library_file(library_path, debug, python_suffix, python_module_dir)
        prepare_version_file(checkout_directory, python_module_dir)
        prepare_install_directory(python_module_dir, python_type, install_dir)

        if build_type == "pypi":
            build_pypi_package(python_type, python_suffix, upload, build_dir)
            return

        if build_type == "debian":
            build_debian_package(package_name, python_type, python_suffix, debian_repositories, debug, upload, build_dir, install_dir)
            return


def main():
    parser = argparse.ArgumentParser(description="YT Build package locally")

    parser.add_argument(
        "--build-type",
        required=True,
        choices=["debian", "pypi"],
        help="Type of package to build")
    parser.add_argument(
        "--python-type",
        default="2",
        choices=["2", "3", "skynet"],
        help="Version of python")
    parser.add_argument(
        "--library-path",
        required=True,
        help="path to the binary library file ('so' of 'dylib')")
    parser.add_argument(
        "--changelog-path",
        required=True,
        help="path to the changelog file")
    parser.add_argument(
        "--source-python-module-path",
        required=True,
        help="path to the python module for the package")
    parser.add_argument(
        "--package-path",
        required=True,
        help="path to the directory with package files ('debian', 'setup.py', ...)")
    parser.add_argument(
        "--package-name",
        required=True,
        help="package name")
    parser.add_argument(
        "--debian-repositories",
        type=comma_separated_list,
        default=[],
        help="comma separated list of repositories to upload")
    parser.add_argument(
        "--no-upload",
        action="store_false",
        dest="upload",
        default=True,
        help="Do not upload packages (useful for debugging with --keep-tmp-files)")
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Additionally build debug package (has effect only for debian)")
    parser.add_argument("--working-directory")
    parser.add_argument(
        "--keep-tmp-files",
        action="store_true",
        default=False,
        help="Keep tmp files (useful for debugging with --no-upload)")
    args = parser.parse_args()

    checkout_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    with create_working_directory(working_directory=args.working_directory, keep_temp_files=args.keep_tmp_files) as working_directory:
        build_package(
            working_directory=working_directory,
            checkout_directory=checkout_directory,
            build_type=args.build_type,
            python_type=args.python_type,
            library_path=args.library_path,
            changelog_path=args.changelog_path,
            source_python_module_path=args.source_python_module_path,
            package_path=args.package_path,
            package_name=args.package_name,
            debian_repositories=args.debian_repositories,
            debug=args.debug,
            upload=args.upload)

if __name__ == "__main__":
    main()
