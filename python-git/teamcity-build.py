#!/usr/bin/python

import os
import sys

# TODO(asaitgalin): Maybe replace it with PYTHONPATH=... in teamcity command?
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build", "python"))

from teamcity import (build_step, teamcity_main, teamcity_message, teamcity_interact,
                      StepFailedWithNonCriticalError)

from helpers import (mkdirp, run, run_captured, cwd, rm_content,
                     rmtree, parse_yes_no_bool, ChildHasNonZeroExitCode,
                     postprocess_junit_xml, sudo_rmtree, kill_by_name)

from pytest_helpers import (copy_artifacts, find_core_dumps_with_report,
                            copy_failed_tests_and_report_stderrs, prepare_python_bindings)

import argparse
import tempfile
import os.path
import pprint
import socket
import functools
import re

def skip_step_if_tests_are_disabled(func):
    @functools.wraps(func)
    def wrapper(options):
        if not options.enabled_python_versions:
            teamcity_message('Skipping step "{0}" since tests are disabled'
                             .format(func.__name__))
            return

        return func(options)
    return wrapper

@build_step
def prepare(options):
    teamcity_message("Environment before build: " + pprint.pformat(dict(os.environ)))

    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["LC_ALL"] = "en_US.UTF-8"

    options.build_number = os.environ["BUILD_NUMBER"]

    codename = run_captured(["lsb_release", "-c"])
    codename = re.sub(r"^Codename:\s*", "", codename)

    if codename not in ["precise", "trusty"]:
        raise RuntimeError("Unknown LSB distribution code name: {0}".format(codename))

    options.codename = codename
    options.repositories = ["yt-" + codename, "yandex-" + codename]

    available_python_versions = ["2.7", "3.4", "pypy"]

    options.enabled_python_versions = set()

    bindings_build_flags = {}

    for version in available_python_versions:
        env_key = "ENABLE_PYTHON_" + version.replace(".", "_").upper() + "_TESTS"
        teamcity_message("Considering {0} with value {1}".format(env_key, os.environ.get(env_key)))
        enabled = parse_yes_no_bool(os.environ.get(env_key, "NO"))

        if enabled:
            options.enabled_python_versions.add(version)

        if version == "pypy":
            version = "2.7"

        bindings_build_flags[version] = bindings_build_flags.get(version, False) or enabled

    options.python_flags = []
    for version, enabled in bindings_build_flags.iteritems():
        options.python_flags.append("-DYT_BUILD_ENABLE_PYTHON_{0}={1}"
            .format(version.replace(".", "_").upper(), "ON" if enabled else "OFF"))

    # Now determine the compiler.
    options.cc = run_captured(["which", options.cc])
    options.cxx = run_captured(["which", options.cxx])
    if not options.cc:
        raise RuntimeError("Failed to locate C compiler")
    if not options.cxx:
        raise RuntimeError("Failed to locate CXX compiler")

    if os.path.exists(options.working_directory) and options.clean_working_directory:
        teamcity_message("Cleaning working directory...", status="WARNING")
        rmtree(options.working_directory)
    mkdirp(options.working_directory)

    if os.path.exists(options.sandbox_directory):
        sudo_rmtree(options.sandbox_directory)
    mkdirp(options.sandbox_directory)

    options.yt_source_directory = os.path.join(options.working_directory, "yt")
    options.yt_build_directory = os.path.join(options.working_directory, "build")
    options.archive_path = os.path.join(options.failed_tests_path,
        options.build_type + "__" +  options.build_number)

    mkdirp(options.yt_build_directory)
    os.chdir(options.working_directory)

    teamcity_message(pprint.pformat(options.__dict__))

@build_step
@skip_step_if_tests_are_disabled
def checkout(options):
    if not os.path.exists(options.yt_source_directory):
        run([
                "git",
                "clone",
                "git@github.yandex-team.ru:yt/yt.git",
                "--recurse-submodules",
                "--branch",
                "prestable/19.3",
                options.yt_source_directory
            ],
            cwd=options.yt_build_directory)
    else:
        run(["git", "checkout", "prestable/19.3"], cwd=options.yt_source_directory)
        run(["git", "pull"], cwd=options.yt_source_directory)
        run(["git", "submodule", "update", "--init", "--recursive"], cwd=options.yt_source_directory)

@build_step
@skip_step_if_tests_are_disabled
def configure(options):
    run([
            "cmake",
            "-DCMAKE_BUILD_TYPE=RelWithDebInfo",
            "-DCMAKE_COLOR_MAKEFILE:BOOL=OFF",
            "-DYT_BUILD_ENABLE_EXPERIMENTS:BOOL=OFF",
            "-DYT_BUILD_ENABLE_TESTS:BOOL=OFF",
            "-DYT_BUILD_ENABLE_BENCHMARKS:BOOL=OFF",
            "-DYT_BUILD_ENABLE_NODEJS:BOOL=ON",
            "-DYT_BUILD_ENABLE_PERL:BOOL=OFF",
            "-DYT_BUILD_ENABLE_GDB_INDEX:BOOL=ON",
            "-DYT_USE_SSE=OFF",
            "-DCMAKE_CXX_COMPILER={0}".format(options.cxx),
            "-DCMAKE_C_COMPILER={0}".format(options.cc),
            options.yt_source_directory,
        ] +
        options.python_flags,
        cwd=options.yt_build_directory)

@build_step
@skip_step_if_tests_are_disabled
def fast_build(options):
    cpus = int(os.sysconf("SC_NPROCESSORS_ONLN"))
    try:
        run(["make", "-j", str(cpus)], cwd=options.yt_build_directory, silent_stdout=True)
    except ChildHasNonZeroExitCode:
        teamcity_message("(ignoring child failure to provide meaningful diagnostics in `slow_build`)")

@build_step
@skip_step_if_tests_are_disabled
def slow_build(options):
    run(["make"], cwd=options.yt_build_directory)

@build_step
@skip_step_if_tests_are_disabled
def set_ytserver_permissions(options):
    for binary in ["ytserver-node", "ytserver-exec", "ytserver-job-proxy", "ytserver-tools"]:
        path = os.path.join(options.yt_build_directory, "bin", binary)
        run(["sudo", "chown", "root", path])
        run(["sudo", "chmod", "4755", path])

@build_step
@skip_step_if_tests_are_disabled
def run_prepare(options):
    nodejs_source = os.path.join(options.yt_source_directory, "yt", "nodejs")
    nodejs_build = os.path.join(options.yt_build_directory, "yt", "nodejs")

    yt_node_binary_path = os.path.join(nodejs_source, "lib", "ytnode.node")
    run(["rm", "-f", yt_node_binary_path])
    run(["ln", "-s", os.path.join(nodejs_build, "ytnode.node"), yt_node_binary_path])

    with cwd(nodejs_build):
        if os.path.exists("node_modules"):
            rmtree("node_modules")
        run(["npm", "install"])

    link_path = os.path.join(nodejs_build, "node_modules", "yt")
    run(["rm", "-f", link_path])
    run(["ln", "-s", nodejs_source, link_path])

@build_step
def copy_modules_from_contrib(options):
    run(["cmake", "."], cwd=options.checkout_directory)

def _run_tests(options, python_version):
    sandbox_directory = os.path.join(options.sandbox_directory, python_version)
    mkdirp(sandbox_directory)

    rm_content(options.core_path)

    interpreter = "pypy" if python_version == "pypy" else "python" + python_version

    env = {
        "PATH": "{0}/bin:{0}/yt/nodejs:/usr/sbin:{1}".format(options.yt_build_directory, os.environ["PATH"]),
        "PYTHONPATH": os.pathsep.join([options.checkout_directory, os.environ.get("PYTHONPATH", "")]),
        "NODE_PATH": os.path.join(options.yt_build_directory, "yt", "nodejs", "node_modules"),
        "TESTS_SANDBOX": sandbox_directory,
        "YT_CAPTURE_STDERR_TO_FILE": "1",
        "YT_ENABLE_VERBOSE_LOGGING": "1",
        "YT_ENABLE_REQUEST_LOGGING": "1",
        "TESTS_JOB_CONTROL": "1"
    }
    for token in ("TEAMCITY_YT_TOKEN", "TEAMCITY_SANDBOX_TOKEN"):
        if token in os.environ:
            env[token] = os.environ[token]

    failed = False
    with tempfile.NamedTemporaryFile() as handle:
        additional_args = []
        if options.enable_parallel_testing:
            # Currently python tests can only scale up to fifteen processes.
            additional_args.append("--process-count=15")

        try:
            run([
                    interpreter,
                    "-m",
                    "pytest",
                    "-r", "x",
                    "--verbose",
                    "--capture=fd",
                    "--tb=native",
                    "--timeout=600",
                    "--debug",
                    "--junitxml={0}".format(handle.name)] + additional_args,
                    cwd=options.checkout_directory,
                    env=env)
        except ChildHasNonZeroExitCode as err:
            if err.return_code == 66:
                teamcity_interact("buildProblem", description="Too many executors crashed during tests. "
                                                              "Test session was terminated.")
            failed = True

        junit_path = os.path.join(options.working_directory,
                                  "junit_python_{0}.xml".format(python_version))
        if not postprocess_junit_xml(handle.name, junit_path):
            failed = True

    if not hasattr(options, "artifacts"):
        # Copying artifacts only once and saving artifact list to options.
        options.artifacts = copy_artifacts(
            options.yt_build_directory, os.path.join(options.archive_path, "artifacts"))

    archive_path = os.path.join(options.archive_path, python_version)
    find_core_dumps_with_report(interpreter, [sandbox_directory, options.core_path],
                                options.artifacts, archive_path)

    try:
        if failed:
            copy_failed_tests_and_report_stderrs([sandbox_directory], archive_path)
            raise StepFailedWithNonCriticalError("Tests failed")
    finally:
        # Note: ytserver tests may create files with that cannot be deleted by teamcity user.
        sudo_rmtree(sandbox_directory)

def _run_tests_for_python_version(options, python_version):
    kill_by_name("^ytserver")
    kill_by_name("^node")
    kill_by_name("^run_proxy")

    if python_version not in options.enabled_python_versions:
        teamcity_message("Python {0} tests are disabled".format(python_version))
        return

    bindings_version = "2.7" if python_version == "pypy" else python_version
    prepare_python_bindings(options.checkout_directory, options.yt_build_directory, bindings_version)
    _run_tests(options, python_version)

@build_step
def run_python_2_7_tests(options):
    _run_tests_for_python_version(options, "2.7")

@build_step
def run_python_3_4_tests(options):
    _run_tests_for_python_version(options, "3.4")

@build_step
def run_python_pypy_tests(options):
    _run_tests_for_python_version(options, "pypy")

@build_step
def build_packages(options):
    if not options.package or options.codename != "precise":
        return

    packages = ["yandex-yt-python", "yandex-yt-python-tools",
                "yandex-yt-transfer-manager-client",
                "yandex-yt-fennel", "yandex-yt-local"]

    for package in packages:
        with cwd(options.checkout_directory, package):
            package_version = run_captured(
                "dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()
            run(["dch", "-r", package_version, "'Resigned by teamcity'"])
        run(["./deploy.sh", package], cwd=options.checkout_directory, env={"TMPDIR": options.working_directory})


################################################################################
# This is an entry-point. Just boiler-plate.

def main():
    def parse_bool(s):
        try:
            return parse_yes_no_bool(s)
        except:
            raise argparse.ArgumentTypeError('Expected "YES" or "NO" value')

    parser = argparse.ArgumentParser(description="YT Build Script")

    parser.add_argument(
        "--build-type",
        action="store", required=True)
    parser.add_argument(
        "--working-directory", metavar="DIR",
        action="store", required=True)
    parser.add_argument(
        "--sandbox-directory", metavar="DIR",
        action="store", required=True)
    parser.add_argument(
        "--checkout-directory", metavar="DIR",
        action="store", required=True)
    parser.add_argument(
        "--clean-working-directory",
        type=parse_bool, action="store", default=False)
    parser.add_argument(
        "--package",
        type=parse_bool, action="store", default=False)

    parser.add_argument(
        "--cc",
        action="store", required=False, default="gcc-4.9")
    parser.add_argument(
        "--cxx",
        action="store", required=False, default="g++-4.9")

    options = parser.parse_args()
    options.failed_tests_path = os.path.expanduser("~/failed_tests")
    options.core_path = os.path.expanduser("~/core")
    options.is_bare_metal = socket.getfqdn().endswith("tc.yt.yandex.net")
    options.enable_parallel_testing = options.is_bare_metal
    teamcity_main(options)


if __name__ == "__main__":
    main()
