#!/usr/bin/python

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build", "python"))

from teamcity import (
    build_step,
    cleanup_step,
    teamcity_main,
    teamcity_message,
    teamcity_interact,
    StepFailedWithNonCriticalError)

from helpers import (mkdirp, run, run_captured, cwd, rm_content,
                     rmtree, parse_yes_no_bool, ChildHasNonZeroExitCode,
                     postprocess_junit_xml, sudo_rmtree, kill_by_name,
                     set_yt_binaries_suid_bit)

from pytest_helpers import (
    copy_artifacts,
    find_core_dumps_with_report,
    copy_failed_tests_and_report_stderrs,
    prepare_python_bindings,
    clean_failed_tests_directory)

import argparse
import tempfile
import os.path
import pprint
import socket
import functools
import re

def get_bin_dir(options):
    return os.path.join(options.yt_build_directory, "bin")

def get_ya_cache_dir(options):
    ya_cache = os.environ.get("YA_CACHE_DIR", None)
    if ya_cache is None:
        ya_cache = os.path.join(options.working_directory, "ya_cache")
    return ya_cache

def ya_make_env(options):
    return {
        "YA_CACHE_DIR": get_ya_cache_dir(options),
    }


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

    options.build_system = os.environ.get("BUILD_SYSTEM", "ya")
    if options.build_system not in ("ya",):
        raise RuntimeError("Unknown build system: {}".format(options.build_system))

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
    options.build_python_version_list = []
    for version, enabled in sorted(bindings_build_flags.iteritems()):
        options.python_flags.append("-DYT_BUILD_ENABLE_PYTHON_{0}={1}"
            .format(version.replace(".", "_").upper(), "ON" if enabled else "OFF"))
        if enabled:
            options.build_python_version_list.append(version)

    if os.path.exists(options.working_directory) and options.clean_working_directory:
        teamcity_message("Cleaning working directory...", status="WARNING")
        rmtree(options.working_directory)
    mkdirp(options.working_directory)

    if os.path.exists(get_ya_cache_dir(options)) and parse_yes_no_bool(os.environ.get("BUILD_CLEAN_YA_CACHE_DIR", "YES")):
        teamcity_message("Cleaning ya cache...")
        # We temorary use sudo rmtree, since due to a bug we have root owned files in ya cache dir on some machines.
        sudo_rmtree(get_ya_cache_dir(options))
    mkdirp(get_ya_cache_dir(options))

    if os.path.exists(options.sandbox_directory):
        sudo_rmtree(options.sandbox_directory)
    mkdirp(options.sandbox_directory)

    options.build_enable_ya_yt_store = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_YA_YT_STORE", "NO"))

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
                "prestable/19.4",
                options.yt_source_directory
            ],
            cwd=options.yt_build_directory)
    else:
        run(["git", "checkout", "prestable/19.4"], cwd=options.yt_source_directory)
        run(["git", "pull"], cwd=options.yt_source_directory)
        run(["git", "submodule", "update", "--init", "--recursive"], cwd=options.yt_source_directory)

@build_step
@skip_step_if_tests_are_disabled
def build(options):
    yall = os.path.join(options.yt_source_directory, "yall")
    env = ya_make_env(options)
    args = [
        yall,
        "-T",
        "--build", "release",
        "--yall-cmake-like-install", options.yt_build_directory,
        "--yall-python-version-list", ",".join(options.build_python_version_list),
    ]
    if options.build_enable_ya_yt_store:
        args += [
            "--yall-enable-dist-cache",
            "--yall-dist-cache-put",
            "--yall-dist-cache-no-auto-token",
        ]
        env["YT_TOKEN"] = os.environ["TEAMCITY_YT_TOKEN"]

    run(args, env=env)


@build_step
@skip_step_if_tests_are_disabled
def set_suid_bit(options):
    set_yt_binaries_suid_bit(bin_dir=get_bin_dir(options))


@build_step
def copy_modules_from_contrib(options):
    run(["./prepare_source_tree.py", "--yt-root", options.yt_source_directory], cwd=options.checkout_directory)

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
            teamcity_interact("buildProblem", description="Pytest failed (python: {}; exit code: {})".format(python_version, err.return_code))
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
                "yandex-yt-transfer-manager-client", "yandex-yt-local"]

    for package in packages:
        with cwd(options.checkout_directory, package):
            package_version = run_captured(
                "dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()
            run(["dch", "-r", package_version, "'Resigned by teamcity'"])
        run(["./deploy.sh", package], cwd=options.checkout_directory, env={"TMPDIR": options.working_directory})

@cleanup_step
def clean_failed_tests(options, build_context, max_allowed_size=None):
    clean_failed_tests_directory(options.failed_tests_path)

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
