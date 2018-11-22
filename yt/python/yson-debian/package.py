import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build", "python"))

from teamcity import teamcity_message
from helpers import (mkdirp, run, run_captured, cwd, rmtree)

import argparse
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

def build_package(yt_install_directory, yt_build_directory, checkout_directory, working_directory, goals, repositories, codename, enable_dbg, module_name):
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

    def build_package(package_dir, package_name, build_dir, python_suffix, build_wheel, debug, is_skynet):
        library = find_library(yt_install_directory, "yson_lib.so")
        deb_build_options = ""
        if debug:
            debug_library = os.path.join(os.path.dirname(library), "yson_lib.dbg.so")
            if yt_build_directory is None:
                shutil.copy(library, debug_library)
            else:
                shutil.move(library, debug_library)
            library = debug_library
            package_name += "-dbg"
            deb_build_options = "nostrip"
        else:
            copy_content(os.path.join(build_dir, module_name), os.path.dirname(library))

        if python_suffix == "3":
            # See: https://www.python.org/dev/peps/pep-3149/#pep-384
            library_with_suffix = "{0}.abi3.so".format(library[:-3])
            if yt_build_directory is None:
                shutil.copy(library, library_with_suffix)
            else:
                shutil.move(library, library_with_suffix)
            library = library_with_suffix

        shutil.copy(library, os.path.join(build_dir, module_name))
        if not debug and enable_dbg:
            with cwd(build_dir + "-dbg", module_name):
                # See: https://sourceware.org/gdb/onlinedocs/gdb/Separate-Debug-Files.html
                library_name = "yson_lib.dbg.abi3.so" if python_suffix == "3" else "yson_lib.dbg.so"
                run(["objcopy", "--add-gnu-debuglink={0}".format(library_name), library])

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
            commit_hash = run_captured(["git", "rev-parse", "--short", "HEAD"], cwd=checkout_directory).strip()
            with open(os.path.join(module_name, "version.py"), "w") as fout:
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
                args += ["upload", "-r", "yandex"]
                run(args, env=env)

            repositories_to_upload = []
            for repository in repositories:
                versions = get_package_versions(package_name, repository)
                if package_version in versions:
                    teamcity_message("Package {0}={1} is already uploaded to {2}, skipping this repo".format(package_name, package_version, repository))
                else:
                    repositories_to_upload.append(repository)

            if not repositories_to_upload:
                teamcity_message("No repositories to upload, skip building package {0}".format(package_name))
                return

            teamcity_message("Building package {0}".format(package_name))

            run(["dch", "-r", package_version, "'Resigned by teamcity'"])
            interpreter = "python2" if not python_suffix else "python3"
            dpkg_buildpackage_env = {
                "SOURCE_DIR": yt_install_directory,
                "DEB_BUILD_OPTIONS": deb_build_options,
                "PYBUILD_DEST_INTERPRETER": interpreter,
                "PYBUILD_PACKAGE_NAME": package_name
            }

            run(["dpkg-buildpackage", "-b"], env=dpkg_buildpackage_env)

            changes_files = filter(lambda file: file.endswith(".changes"), os.listdir(package_dir))
            assert len(changes_files) == 1
            changes_file = os.path.join(package_dir, changes_files[0])
            for repository in repositories_to_upload:
                run(["dupload", "--to", repository, "--nomail", "--force", changes_file])

            os.remove(changes_file)

    def build_goal(goal, package_name, python_suffix=None, build_wheel=False, is_skynet=False):
        teamcity_message("Preparing package {0}(-dbg) to build".format(package_name))
        package_dir = os.path.join(working_directory, "package_" + package_name.replace("-", "_"))
        build_dir = os.path.join(package_dir, "build")

        # Hack for skynet python.
        if is_skynet:
            package_path = os.path.join(checkout_directory, "yandex-yt-python-yson")
            assert os.path.exists(package_path)
        else:
            package_path = os.path.join(checkout_directory, package_name)

        if not os.path.exists(package_path):
            package_path = os.path.join(checkout_directory, "yandex-yt-python-any-yson")

        def prepare(build_dir, cmake_options):
            clean_path(build_dir)
            copy_content(package_path, build_dir)

            copy_element(checkout_directory, build_dir, "debian/changelog")
            copy_element(checkout_directory, build_dir, module_name)

            if yt_build_directory is None:
                return

            shutil.rmtree(yt_install_directory)
            run([
                    "cmake",
                    "-DCOMPONENT=" + goal + "-shared",
                ] +
                cmake_options +
                [
                    "-P",
                    os.path.join(yt_build_directory, "cmake_install.cmake")
                ],
                cwd=yt_build_directory)

        if enable_dbg:
            prepare(build_dir + "-dbg", ["-DCMAKE_INSTALL_DO_STRIP=OFF"])
            build_package(package_dir, package_name, build_dir + "-dbg", python_suffix, build_wheel=False, debug=True, is_skynet=is_skynet)

        prepare(build_dir, [])
        build_package(package_dir, package_name, build_dir, python_suffix, build_wheel=build_wheel, debug=False, is_skynet=is_skynet)

    for goal in goals:
        python_suffix = goal.split("-")[2]
        if python_suffix in ("2", "skynet"):
            python_suffix = ""
        is_skynet_python = (codename == "precise" and goal == "yt-python-skynet-yson")
        is_native_python = \
            codename in ["precise", "trusty"] and goal in ("yt-python-2-7-yson", "yt-python-3-4-yson")

        #if not is_skynet_python:
        build_goal(goal, "yandex-" + goal, python_suffix=python_suffix)

        if is_native_python or is_skynet_python:
            # Wheels are tagged only with interpreter type and platform (e.g. win32, macosx, etc.)
            # and not linux distribution aware. To preserve binary compatibility with as many
            # Ubuntu distributions as possible wheel should be built only on the oldest
            # distribution (since all new distributions have backward compatibility).
            # See PEP-425, PEP-513 and https://github.com/pypa/manylinux for more details.
            # This is why oldest distributions are chosen - precise.
            build_wheel = codename == "precise"
            build_goal(goal, "yandex-yt-python{0}-yson".format(python_suffix),
                       python_suffix=python_suffix, build_wheel=build_wheel, is_skynet=is_skynet_python)

def main():
    parser = argparse.ArgumentParser(description="YT Build package locally")

    parser.add_argument("--yt-install-directory", action="store", required=True)
    parser.add_argument("--yt-build-directory", action="store")
    parser.add_argument("--goal", nargs="+")
    parser.add_argument("--repository", nargs="*")

    args = parser.parse_args()

    if args.repository is None:
        args.repository = []

    checkout_directory = os.path.abspath(os.path.dirname(__file__))
    working_directory = tempfile.mkdtemp(dir=checkout_directory)
    codename = re.sub(r"^Codename:\s*", "", run_captured(["lsb_release", "-c"]))

    pattern = "^yt-python-.*-yson$"
    for goal in args.goal:
        if not re.match(pattern, goal):
            raise RuntimeError("Incorrect goal {0}, it should match {1}".format(goal, pattern))

    build_package(
        yt_install_directory=args.yt_install_directory,
        yt_build_directory=args.yt_build_directory,
        checkout_directory=checkout_directory,
        working_directory=working_directory,
        goals=args.goal,
        repositories=args.repository,
        codename=codename,
        enable_dbg=False,
        module_name="yt_yson_bindings")

if __name__ == "__main__":
    main()
