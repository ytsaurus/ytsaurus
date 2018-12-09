#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../scripts/teamcity-build/python"))

from teamcity import teamcity_message
from helpers import (mkdirp, run, run_captured, cwd, rmtree)

import argparse
import contextlib
import gzip
import re
import requests
import shutil
import tempfile
from cStringIO import StringIO

def copy_element(src, dst, elem):
    source = os.path.join(src, elem)
    destination = os.path.join(dst, elem)
    if os.path.isdir(source):
        shutil.copytree(source, destination)
    else:
        shutil.copy(source, destination)

def copy_content(src, dst):
    for elem in os.listdir(src):
        copy_element(src, dst, elem)

def remove_content(path):
    for elem in os.listdir(path):
        elem_path = os.path.join(path, elem)
        if os.path.isfile(elem_path):
            os.remove(elem_path)
        else:
            shutil.rmtree(elem_path)

def clean_path(path):
    if os.path.exists(path):
        rmtree(path)
    mkdirp(path)

def create_if_missing(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

class PackagingContext(object):
    def __init__(self, module_path, checkout_directory, working_directory, codename):
        self.module_path = module_path
        self.checkout_directory = checkout_directory
        self.working_directory = working_directory
        self.codename = codename

    @property
    def module_name(self):
        return os.path.basename(self.module_path)

    @property
    def yson_debian_directory(self):
        return os.path.join(self.checkout_directory, "yt/python/yson-debian")

    @property
    def install_directory(self):
        return create_if_missing(os.path.join(self.working_directory, "install"))

    def package_work_directory(self, package_name, debug=False):
        package_dir = os.path.join(self.working_directory, "package_" + package_name.replace("-", "_"))
        return create_if_missing(package_dir)


@contextlib.contextmanager
def packaging_context(module_path, checkout_directory, working_directory=None, keep_tmp_files=False):
    if working_directory is None:
        working_directory = os.getcwd()
    tmp_dir = tempfile.mkdtemp(dir=working_directory)
    codename = re.sub(r"^Codename:\s*", "", run_captured(["lsb_release", "-c"]))
    ctx = PackagingContext(module_path, checkout_directory, tmp_dir, codename)
    try:
        yield ctx
    finally:
        if not keep_tmp_files:
            shutil.rmtree(tmp_dir)

def build_targets(
    ctx,
    targets,
    repositories,
    enable_dbg,
    upload=False,
):
    def find_library(path, library_name):
        for root, dirs, files in os.walk(path):
            for name in files:
                if name.startswith(library_name):
                    return os.path.join(root, name)

    def get_package_versions(package_name, repo):
        arch_output = run_captured(["dpkg-architecture"])
        arch = filter(lambda line: line.startswith("DEB_BUILD_ARCH="), arch_output.split("\n"))[0].split("=")[1]

        versions = set()
        for branch in ["unstable", "testing", "prestable", "stable"]:
            url = "http://dist.yandex.ru/{0}/{1}/{2}/Packages.gz".format(repo, branch, arch)
            rsp = requests.get(url)
            # Temporary workaround, remove it when problem with dist will be fixed.
            if rsp.status_code != 200:
                teamcity_message("Failed to get packages list from {0}, skipping it".format(url))
                continue
            content_gz = rsp.content
            content = gzip.GzipFile(fileobj=StringIO(content_gz))
            current_package = None
            current_version = None
            for line in content:
                line = line.strip()
                if line.startswith("Package: "):
                    current_package = line.split()[1]
                if line.startswith("Version: "):
                    current_version = line.split()[1]
                if not line:
                    if current_package == package_name and current_version is not None:
                        versions.add(current_version)
                    current_package = None
                    current_version = None

        return versions

    def build_package(package_name, build_dir, python_suffix, build_wheel, debug, is_skynet):
        library = find_library(ctx.install_directory, "yson_lib.so")
        deb_build_options = ""
        if debug:
            debug_library = os.path.join(os.path.dirname(library), "yson_lib.dbg.so")
            shutil.copy(library, debug_library)
            library = debug_library
            package_name += "-dbg"
            deb_build_options = "nostrip"
        else:
            copy_content(os.path.join(build_dir, ctx.module_name), os.path.dirname(library))

        if python_suffix == "3":
            # See: https://www.python.org/dev/peps/pep-3149/#pep-384
            library_with_suffix = "{0}.abi3.so".format(library[:-3])
            shutil.copy(library, library_with_suffix)
            library = library_with_suffix

        shutil.copy(library, os.path.join(build_dir, ctx.module_name))
        if not debug and enable_dbg:
            with cwd(build_dir + "-dbg", ctx.module_name):
                # See: https://sourceware.org/gdb/onlinedocs/gdb/Separate-Debug-Files.html
                debug_library_name = "yson_lib.dbg.abi3.so" if python_suffix == "3" else "yson_lib.dbg.so"
                run(["strip", "--remove-section=.gnu_debuglink", library])
                run(["objcopy", "--add-gnu-debuglink={0}".format(debug_library_name), library])

        # FOR DEBUGGING
        #print >>sys.stderr, "BUILD_DIR CONTENT"
        #print >>sys.stderr, run_captured(["find", build_dir, "-name", "*"])
        #print >>sys.stderr, "INTSTALL_DIR CONTENT"
        #print >>sys.stderr, run_captured(["find", options.yt_install_directory, "-name", "*"])

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

        with cwd(build_dir):
            package_version = run_captured(
                "dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()
            commit_hash = run_captured(["git", "rev-parse", "--short", "HEAD"], cwd=ctx.checkout_directory).strip()
            with open(os.path.join(ctx.module_name, "version.py"), "w") as fout:
                fout.write('VERSION = "{0}"\n'.format(package_version))
                fout.write('COMMIT = "{0}"\n'.format(commit_hash))

            if build_wheel:
                env = {}
                python = "python" + python_suffix
                if is_skynet:
                    env["PYTHON_SUFFIX"] = "-skynet"
                    python = "/skynet/python/bin/python"
                # Also we should change get_abi_version in python to return the same version as MacOS python.
                #macosx_platforms = ["macosx_{0}_{1}_{2}".format(v1, v2, platform_type) for v1 in (10,) for v2 in xrange(9, 14) for platform_type in ("intel", "x86_64")]
                #for platform in platforms:
                #    args += ["--plat-name", platform]
                args = [python, "setup.py", "bdist_wheel"]
                if python_suffix == "3":
                    args += ["--py-limited-api", "cp34"]
                if upload:
                    args += ["upload", "-r", "yandex"]
                run(args, env=env)

            repositories_to_upload = []
            for repository in repositories:
                versions = get_package_versions(package_name, repository)
                if package_version in versions:
                    teamcity_message("Package {0}={1} is already uploaded to {2}, skipping this repo".format(package_name, package_version, repository))
                else:
                    repositories_to_upload.append(repository)

            teamcity_message("Building package {0}".format(package_name))

            run(["dch", "-r", package_version, "'Resigned by teamcity'"])
            interpreter = "python2" if not python_suffix else "python3"
            dpkg_buildpackage_env = {
                "SOURCE_DIR": ctx.install_directory,
                "DEB_BUILD_OPTIONS": deb_build_options,
                "PYBUILD_DEST_INTERPRETER": interpreter,
                "PYBUILD_PACKAGE_NAME": package_name
            }

            run(["dpkg-buildpackage", "-b"], env=dpkg_buildpackage_env)

            package_dir = os.path.dirname(build_dir)
            changes_files = filter(lambda file: file.endswith(".changes"), os.listdir(package_dir))
            assert len(changes_files) == 1
            changes_file = os.path.join(package_dir, changes_files[0])
            if upload:
                for repository in repositories_to_upload:
                    run(["dupload", "--to", repository, "--nomail", "--force", changes_file])

            os.remove(changes_file)

    def build_goal(yson_lib_so_path, package_name, python_suffix=None, build_wheel=False, is_skynet=False):
        teamcity_message("Preparing package {0}(-dbg) to build".format(package_name))
        package_dir = ctx.package_work_directory(package_name)
        build_dir = os.path.join(package_dir, "build")

        # Hack for skynet python.
        if is_skynet:
            package_path = os.path.join(ctx.yson_debian_directory, "yandex-yt-python-yson")
            assert os.path.exists(package_path)
        else:
            package_path = os.path.join(ctx.yson_debian_directory, package_name)

        if not os.path.exists(package_path):
            package_path = os.path.join(ctx.yson_debian_directory, "yandex-yt-python-any-yson")

        def prepare(build_dir):
            clean_path(build_dir)
            copy_content(package_path, build_dir)
            copy_element(ctx.yson_debian_directory, build_dir, "debian/changelog")

            shutil.copytree(
                os.path.join(ctx.checkout_directory, ctx.module_path),
                os.path.join(build_dir, ctx.module_name))

            shutil.rmtree(ctx.install_directory)
            yson_lib_install_dir = os.path.join(ctx.install_directory, "lib/pyshared-{0}/yt_yson_bindings".format(
                {
                    "": "2-7",
                    "3": "3-4",
                }[python_suffix]
            ))
            os.makedirs(yson_lib_install_dir)
            shutil.copy(yson_lib_so_path, yson_lib_install_dir)

        if enable_dbg:
            prepare(build_dir + "-dbg")
            build_package(
                package_name,
                build_dir + "-dbg",
                python_suffix,
                build_wheel=False,
                debug=True,
                is_skynet=is_skynet
            )

        prepare(build_dir)
        build_package(
            package_name,
            build_dir,
            python_suffix,
            build_wheel=build_wheel,
            debug=False,
            is_skynet=is_skynet
        )

    for target_key, yson_lib_so_path in targets.iteritems():
        python_suffix = target_key.split("-")[2]
        if python_suffix in ("2", "skynet"):
            python_suffix = ""
        is_skynet_python = (
            ctx.codename == "precise"
            and target_key == "yt-python-skynet-yson"
        )
        is_native_python = (
            ctx.codename in ["precise", "trusty"]
            and target_key in ["yt-python-2-7-yson", "yt-python-3-4-yson"]
        )

        # First of all we want to build packages
        #  - yandex-yt-python-2-7-yson,
        #  - yandex-yt-python-3-4-yson,
        #  - yandex-yt-python-skynet-yson
        # we don't build these packages for pip.
        build_goal(
            yson_lib_so_path,
            "yandex-" + target_key,
            python_suffix=python_suffix,
            build_wheel=False,
            is_skynet=is_skynet_python
        )

        # Then we want packages
        #  - yandex-yt-python-yson,
        #  - yandex-yt-python3-4-yson,
        # we don't build these packages for pip.
        if is_native_python or is_skynet_python:
            # Wheels are tagged only with interpreter type and platform (e.g. win32, macosx, etc.)
            # and not linux distribution aware. To preserve binary compatibility with as many
            # Ubuntu distributions as possible wheel should be built only on the oldest
            # distribution (since all new distributions have backward compatibility).
            # See PEP-425, PEP-513 and https://github.com/pypa/manylinux for more details.
            # This is why oldest distributions are chosen - precise.
            build_wheel = ctx.codename == "precise"
            build_goal(
                yson_lib_so_path,
                "yandex-yt-python{0}-yson".format(python_suffix),
                python_suffix=python_suffix,
                build_wheel=build_wheel,
                is_skynet=is_skynet_python
            )

def coma_separated_list(val):
    return val.strip(",").split(",")

def main():
    parser = argparse.ArgumentParser(description="YT Build package locally")

    parser.add_argument("--working-directory", action="store")
    parser.add_argument("--repositories", type=coma_separated_list, default=[], help="coma separated list of repositories to upload")
    parser.add_argument("--enable-dbg", action="store_true", default=False)
    parser.add_argument(
        "--no-upload",
        action="store_false",
        dest="upload",
        default=True,
        help="Do not upload packages (useful for debugging with --keep-tmp-files)"
    )
    parser.add_argument(
        "--keep-tmp-files",
        action="store_true",
        default=False,
        help="Keep tmp files (useful for debugging with --no-upload)"
    )
    parser.add_argument("target", nargs="+")

    args = parser.parse_args()

    pattern = "^(yt-python-[^:]*-yson):(.*)$"
    target_map = {}
    for goal in args.target:
        m = re.match(pattern, goal)
        if not m:
            raise RuntimeError("Incorrect target {0}, it should match {1}".format(goal, pattern))
        target_map[m.group(1)] = m.group(2)

    checkout_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    module_path = "yt/python/yt_yson_bindings"
    with packaging_context(module_path, checkout_directory, args.working_directory, keep_tmp_files=args.keep_tmp_files) as ctx:
        build_targets(
            ctx,
            targets=target_map,
            repositories=args.repositories,
            enable_dbg=args.enable_dbg,
            upload=args.upload
        )

if __name__ == "__main__":
    main()
