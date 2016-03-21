#!/usr/bin/env python

import os
import sys
# TODO(asaitgalin): Maybe replace it with PYTHONPATH=... in teamcity command?
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build", "python"))

from teamcity import build_step, cleanup_step, teamcity_main, \
                     teamcity_message, teamcity_interact, \
                     StepFailedWithNonCriticalError

from helpers import mkdirp, run, run_captured, cwd, copytree, \
                    kill_by_name, sudo_rmtree, ls, get_size, \
                    rmtree, ChildHasNonZeroExitCode

from pytest_helpers import get_sandbox_dirs, save_failed_test

import argparse
import glob
import os.path
import pprint
import re
import socket
import tempfile
import xml.etree.ElementTree as etree

@build_step
def prepare(options):
    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["LC_ALL"] = "en_US.UTF-8"

    script_directory = os.path.dirname(os.path.realpath(__file__))

    options.build_number = os.environ["BUILD_NUMBER"]
    options.build_vcs_number = os.environ["BUILD_VCS_NUMBER"]
    options.build_git_depth = run_captured(
        [os.path.join(script_directory, "git-depth.py")],
        cwd=options.checkout_directory)

    def checked_yes_no(s):
        s = s.upper()
        if s != "YES" and s != "NO":
            raise RuntimeError("'{0}' should be either 'YES' or 'NO'".format(s))
        return s

    options.build_enable_nodejs = checked_yes_no(os.environ.get("BUILD_ENABLE_NODEJS", "YES"))
    options.build_enable_python = checked_yes_no(os.environ.get("BUILD_ENABLE_PYTHON", "YES"))
    options.build_enable_perl = checked_yes_no(os.environ.get("BUILD_ENABLE_PERL", "YES"))

    options.branch = re.sub(r"^refs/heads/", "", options.branch)
    options.branch = options.branch.split("/")[0]

    codename = run_captured(["lsb_release", "-c"])
    codename = re.sub(r"^Codename:\s*", "", codename)

    if codename not in ["lucid", "precise", "trusty"]:
        raise RuntimeError("Unknown LSB distribution code name: {0}".format(codename))

    options.codename = codename
    options.repositories = ["yt-" + codename]

    # Now determine the compiler.
    options.cc = run_captured(["which", options.cc])
    options.cxx = run_captured(["which", options.cxx])

    if not options.cc:
        raise RuntimeError("Failed to locate C compiler")

    if not options.cxx:
        raise RuntimeError("Failed to locate CXX compiler")

    # Temporaly turn off
    # options.use_lto = (options.type != "Debug")
    options.use_lto = False

    if os.path.exists(options.working_directory) and options.clean_working_directory:
        teamcity_message("Cleaning working directory...", status="WARNING")
        rmtree(options.working_directory)
    mkdirp(options.working_directory)

    if os.path.exists(options.sandbox_directory) and options.clean_sandbox_directory:
        teamcity_message("Cleaning sandbox directory...", status="WARNING")
        rmtree(options.sandbox_directory)

        sandbox_storage = os.path.expanduser("~/sandbox_storage/")
        if os.path.exists(sandbox_storage):
            rmtree(sandbox_storage)

    teamcity_message("Creating cgroups...", status="WARNING")
    cgroup_names = ("blkio", "cpu", "cpuacct", "freezer", "memory")
    teamcity_cgpaths = [os.path.join("/sys/fs/cgroup", name, "teamcity") for name in cgroup_names]
    yt_cgpaths = [os.path.join(path, "yt") for path in teamcity_cgpaths]
    run(["sudo", "mkdir", "-p"] + yt_cgpaths)
    run(["sudo", "chown", "-R", str(os.getuid())+":"+str(os.getgid())] + teamcity_cgpaths)
    run(["sudo", "chmod", "-R", "u+rw"] + teamcity_cgpaths)

    teamcity_message("Cleaning cgroups...", status="WARNING")
    for cgpath in yt_cgpaths:
        for root, subFolders, files in os.walk(cgpath, topdown=False):
            if root != cgpath:
                try:
                    os.rmdir(root)
                except:
                    pass

    mkdirp(options.sandbox_directory)

    os.chdir(options.sandbox_directory)

    teamcity_message(pprint.pformat(options.__dict__))


@build_step
def configure(options):
    run([
        "cmake",
        "-DCMAKE_INSTALL_PREFIX=/usr",
        "-DCMAKE_BUILD_TYPE={0}".format(options.type),
        "-DCMAKE_COLOR_MAKEFILE:BOOL=OFF",
        "-DYT_BUILD_ENABLE_EXPERIMENTS:BOOL=ON",
        "-DYT_BUILD_ENABLE_TESTS:BOOL=ON",
        "-DYT_BUILD_BRANCH={0}".format(options.branch),
        "-DYT_BUILD_NUMBER={0}".format(options.build_number),
        "-DYT_BUILD_VCS_NUMBER={0}".format(options.build_vcs_number[0:7]),
        "-DYT_BUILD_GIT_DEPTH={0}".format(options.build_git_depth),
        "-DYT_BUILD_ENABLE_NODEJS={0}".format(options.build_enable_nodejs),
        "-DYT_BUILD_ENABLE_PYTHON={0}".format(options.build_enable_python),
        "-DYT_BUILD_ENABLE_PERL={0}".format(options.build_enable_perl),
        "-DYT_USE_LTO={0}".format(options.use_lto),
        "-DCMAKE_CXX_COMPILER={0}".format(options.cxx),
        "-DCMAKE_C_COMPILER={0}".format(options.cc),
        options.checkout_directory],
        cwd=options.working_directory)


@build_step
def fast_build(options):
    cpus = int(os.sysconf("SC_NPROCESSORS_ONLN"))
    try:
        run(["make", "-j", str(cpus)], cwd=options.working_directory, silent_stdout=True)
    except ChildHasNonZeroExitCode:
        teamcity_message("(ignoring child failure to provide meaningful diagnostics in `slow_build`)")


@build_step
def slow_build(options):
    run(["make"], cwd=options.working_directory)


@build_step
def set_suid_bit(options):
    path = "{0}/bin/ytserver".format(options.working_directory)
    run(["sudo", "chown", "root", path])
    run(["sudo", "chmod", "4755", path])


@build_step
def package(options):
    if not options.package:
        return

    with cwd(options.working_directory):
        run(["make", "-j", "8", "package"])
        run(["make", "-j", "8", "python-package"])
        run(["make", "version"])

        with open("ytversion") as handle:
            version = handle.read().strip()

        teamcity_message("We have built a package")
        teamcity_interact("setParameter", name="yt.package_built", value=1)
        teamcity_interact("setParameter", name="yt.package_version", value=version)
        teamcity_interact("buildStatus", text="{{build.status.text}}; Package: {0}".format(version))

        artifacts = glob.glob("./ARTIFACTS/yandex-yt*{0}*.changes".format(version))
        if artifacts:
            for repository in options.repositories:
                run(["dupload", "--to", repository, "--nomail", "--force"] + artifacts)
                teamcity_message("We have uploaded a package to " + repository)
                teamcity_interact("setParameter", name="yt.package_uploaded." + repository, value=1)


@build_step
def run_prepare(options):
    with cwd(options.checkout_directory):
        run(["make", "-C", "./python/yt/wrapper"])
        run(["make", "-C", "./python", "version"])

    with cwd(options.working_directory, "yt/nodejs"):
        if os.path.exists("node_modules"):
            rmtree("node_modules")
        run(["npm", "install"])


@build_step
def run_unit_tests(options):
    sandbox_current = os.path.join(options.sandbox_directory, "unit_tests")
    sandbox_archive = os.path.join(options.failed_tests_path,
        "__".join([options.btid, options.build_number, "unit_tests"]))

    mkdirp(sandbox_current)
    try:
        run([
            "gdb",
            "--batch",
            "--return-child-result",
            "--command={0}/scripts/teamcity-build/teamcity-gdb-script".format(options.checkout_directory),
            "--args",
            os.path.join(options.working_directory, "bin", "unittester"),
            "--gtest_color=no",
            "--gtest_death_test_style=threadsafe",
            "--gtest_output=xml:" + os.path.join(options.working_directory, "gtest_unittester.xml")],
            cwd=sandbox_current,
            timeout=10 * 60)
    except ChildHasNonZeroExitCode as err:
        teamcity_message('Copying unit tests sandbox from "{0}" to "{1}"'.format(
            sandbox_current, sandbox_archive), status="WARNING")
        copytree(sandbox_current, sandbox_archive)

        raise StepFailedWithNonCriticalError(str(err))
    finally:
        rmtree(sandbox_current)


@build_step
def run_javascript_tests(options):
    if options.build_enable_nodejs != "YES":
        return

    try:
        run(
            ["./run_tests.sh", "-R", "xunit"],
            cwd="{0}/yt/nodejs".format(options.working_directory),
            env={"MOCHA_OUTPUT_FILE": "{0}/junit_nodejs_run_tests.xml".format(options.working_directory)})
    except ChildHasNonZeroExitCode as err:
        raise StepFailedWithNonCriticalError(str(err))


def run_pytest(options, suite_name, suite_path, pytest_args=None, env=None):
    if options.build_enable_python != "YES":
        return

    if pytest_args is None:
        pytest_args = []

    sandbox_current, sandbox_storage = get_sandbox_dirs(options, suite_name)
    mkdirp(sandbox_current)

    failed = False

    if env is None:
        env = {}

    env["PATH"] = "{0}/bin:{0}/yt/nodejs:{1}".format(options.working_directory, os.environ.get("PATH", ""))
    env["PYTHONPATH"] = "{0}/python:{1}".format(options.checkout_directory, os.environ.get("PYTHONPATH", ""))
    env["TESTS_SANDBOX"] = sandbox_current
    env["TESTS_SANDBOX_STORAGE"] = sandbox_storage
    env["YT_CAPTURE_STDERR_TO_FILE"] = "1"
    env["YT_ENABLE_VERBOSE_LOGGING"] = "1"

    with tempfile.NamedTemporaryFile() as handle:
        try:
            run([
                "py.test",
                "-r", "x",
                "--verbose",
                "--capture=fd",
                "--tb=native",
                "--timeout=600",
                "--debug",
                "--junitxml={0}".format(handle.name)]
                + pytest_args,
                cwd=suite_path,
                env=env)
        except ChildHasNonZeroExitCode:
            teamcity_message("(ignoring child failure since we are reading test results from XML)")
            failed = True

        if hasattr(etree, "ParseError"):
            ParseError = etree.ParseError
        else:
            # Lucid case.
            ParseError = TypeError

        try:
            result = etree.parse(handle)
            for node in (result.iter() if hasattr(result, "iter") else result.getiterator()):
                if isinstance(node.text, str):
                    node.text = node.text \
                        .replace("&quot;", "\"") \
                        .replace("&apos;", "\'") \
                        .replace("&amp;", "&") \
                        .replace("&lt;", "<") \
                        .replace("&gt;", ">")

            with open("{0}/junit_python_{1}.xml".format(options.working_directory, suite_name), "w+b") as handle:
                result.write(handle, encoding="utf-8")

        except (UnicodeDecodeError, ParseError):
            failed = True
            teamcity_message("Failed to parse pytest output:\n" + open(handle.name).read())

    try:
        if failed:
            save_failed_test(options, suite_name, suite_path)
            raise StepFailedWithNonCriticalError("Tests '{0}' failed".format(suite_name))
    finally:
        # Note: ytserver tests may create files with that cannot be deleted by teamcity user.
        sudo_rmtree(sandbox_current)
        if os.path.exists(sandbox_storage):
            sudo_rmtree(sandbox_storage)


@build_step
def run_integration_tests(options):
    kill_by_name("^ytserver")

    pytest_args = []
    if options.enable_parallel_testing:
        pytest_args.extend(["--process-count", "6"])

    run_pytest(options, "integration", "{0}/tests/integration".format(options.checkout_directory),
               pytest_args=pytest_args)


@build_step
def run_python_libraries_tests(options):
    kill_by_name("^ytserver")
    kill_by_name("^node")

    pytest_args = []
    if options.enable_parallel_testing:
        pytest_args.extend(["--process-count", "4"])

    run_pytest(options, "python_libraries", "{0}/python".format(options.checkout_directory),
               pytest_args=pytest_args,
               env={"TESTS_JOB_CONTROL": "1"})


@build_step
def build_python_packages(options):
    if not options.package:
        return

    packages = ["yandex-yt-python", "yandex-yt-python-tools", "yandex-yt-python-yson",
                "yandex-yt-transfer-manager", "yandex-yt-transfer-manager-client",
                "yandex-yt-python-fennel", "yandex-yt-local"]

    for package in packages:
        with cwd(options.checkout_directory, "python", package):
            package_version = run_captured(
                "dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()
            run(["dch", "-r", package_version, "'Resigned by teamcity'"])
        with cwd(options.checkout_directory, "python"):
            run(["./deploy.sh", package], cwd=os.path.join(options.checkout_directory, "python"))


@build_step
def run_perl_tests(options):
    if options.build_enable_perl != "YES":
        return
    kill_by_name("^ytserver")
    run_pytest(options, "perl", "{0}/perl/tests".format(options.checkout_directory))


@cleanup_step
def clean_artifacts(options, n=10):
    for path in ls("{0}/ARTIFACTS".format(options.working_directory),
                   reverse=True,
                   select=os.path.isfile,
                   start=n,
                   stop=sys.maxint):
        teamcity_message("Removing {0}...".format(path), status="WARNING")
        if os.path.isdir(path):
            rmtree(path)
        else:
            os.unlink(path)


@cleanup_step
def clean_failed_tests(options, max_allowed_size=None):
    if options.is_bare_metal:
        max_allowed_size = 200 * 1024 * 1024 * 1024
    else:
        max_allowed_size = 50 * 1024 * 1024 * 1024

    total_size = 0
    for path in ls(options.failed_tests_path,
                   select=os.path.isdir,
                   stop=sys.maxint):
        size = get_size(path, enable_cache=True)
        if total_size + size > max_allowed_size:
            teamcity_message("Removing {0}...".format(path), status="WARNING")
            if os.path.isdir(path):
                rmtree(path)
                if os.path.exists(path + ".size"):
                    os.remove(path + ".size")
            else:
                os.unlink(path)
        else:
            total_size += size


################################################################################
# This is an entry-point. Just boiler-plate.

def main():
    def parse_bool(s):
        if s == "YES":
            return True
        if s == "NO":
            return False
        raise argparse.ArgumentTypeError("Expected YES or NO")
    parser = argparse.ArgumentParser(description="YT Build Script")

    parser.add_argument("--btid", type=str, action="store", required=True)
    parser.add_argument("--branch", type=str, action="store", required=True)

    parser.add_argument(
        "--checkout_directory", metavar="DIR",
        type=str, action="store", required=True)

    parser.add_argument(
        "--working_directory", metavar="DIR",
        type=str, action="store", required=True)
    parser.add_argument(
        "--clean_working_directory",
        type=parse_bool, action="store", default=False)

    parser.add_argument(
        "--sandbox_directory", metavar="DIR",
        type=str, action="store", required=True)
    parser.add_argument(
        "--clean_sandbox_directory",
        type=parse_bool, action="store", default=True)

    parser.add_argument(
        "--type",
        type=str, action="store", required=True, choices=("Debug", "Release", "RelWithDebInfo"))

    parser.add_argument(
        "--package",
        type=parse_bool, action="store", default=False)

    parser.add_argument(
        "--cc",
        type=str, action="store", required=False, default="gcc-4.8")
    parser.add_argument(
        "--cxx",
        type=str, action="store", required=False, default="g++-4.8")

    options = parser.parse_args()
    options.failed_tests_path = os.path.expanduser("~/failed_tests")
    options.is_bare_metal = socket.getfqdn().endswith("tc.yt.yandex.net")
    # NB: parallel testing is enabled by default only for bare metal machines.
    options.enable_parallel_testing = options.is_bare_metal
    teamcity_main(options)


if __name__ == "__main__":
    main()
