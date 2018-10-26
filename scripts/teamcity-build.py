#!/usr/bin/env python
import os
import sys
# TODO(asaitgalin): Maybe replace it with PYTHONPATH=... in teamcity command?
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build", "python"))

from teamcity import (build_step, cleanup_step, teamcity_main,
                      teamcity_message, teamcity_interact,
                      StepFailedWithNonCriticalError)

from helpers import (mkdirp, run, run_captured, cwd, copytree,
                     kill_by_name, sudo_rmtree, ls, get_size,
                     rmtree, rm_content, clear_system_tmp,
                     format_yes_no, parse_yes_no_bool, cleanup_cgroups,
                     ChildHasNonZeroExitCode)

from pytest_helpers import (get_sandbox_dirs, save_failed_test,
                            find_core_dumps_with_report, copy_artifacts,
                            prepare_python_bindings)

from datetime import datetime

import argparse
import collections
import contextlib
import fnmatch
import functools
import glob
import json
import os.path
import pprint
import re
import resource
import shutil
import socket
import tarfile
import tempfile
import urlparse
import xml.etree.ElementTree as etree
import xml.parsers.expat

import urllib3
urllib3.disable_warnings()
import requests

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB

NODEJS_RESOURCE = "sbr:629132696"
INTEGRATION_TESTS_PARALLELISM = 4
PYTHON_TESTS_PARALLELISM = 6
YP_TESTS_PARALLELISM = 6

YA_CACHE_YT_STORE_PROXY = "freud"
YA_CACHE_YT_DIR = "//home/yt-teamcity-build/cache"

YA_CACHE_YT_MAX_STORE_SIZE = 2 * TB
YA_CACHE_YT_STORE_TTL = 24 # hours
YA_CACHE_YT_STORE_CODEC = "zstd08_1"

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "nanny-releaselib", "src"))
    from releaselib.sandbox import client as sandbox_client
except:
    sandbox_client = None

def yt_processes_cleanup():
    kill_by_name("^ytserver")
    kill_by_name("^node")
    kill_by_name("^run_proxy")

def comma_separated_set(s):
    return set(el for el in s.split(",") if el)

def process_core_dumps(options, suite_name, suite_path):
    sandbox_archive = os.path.join(options.failed_tests_path,
        "__".join([options.btid, options.build_number, suite_name]))

    # Prepare artifact paths.
    artifact_path = os.path.join(sandbox_archive, "artifacts")
    artifacts = copy_artifacts(options.working_directory, artifact_path, dry_run=True)

    search_paths = [suite_path]
    if hasattr(options, "core_path"):
        search_paths.append(options.core_path)

    if find_core_dumps_with_report(suite_name, search_paths, artifacts, sandbox_archive):
        # Copy artifacts if cores are present.
        copy_artifacts(options.working_directory, artifact_path)
        return True

    return False

def only_for_projects(*projects):
    def decorator(func):
        @functools.wraps(func)
        def wrapped_function(options, build_context):
            if not any(p in options.build_project for p in projects):
                teamcity_message("Skipping step {0} due to build_project configuration".format(func.__name__))
                return
            func(options, build_context)
        return wrapped_function
    return decorator

def get_artifacts_dir(options):
    return os.path.join(options.working_directory, "ARTIFACTS")

def get_node_modules_dir(options):
    return os.path.join(options.working_directory, "yt", "nodejs", "node_modules")

def get_bin_dir(options):
    return os.path.join(options.working_directory, "bin")

def get_yt_token_file(options):
    return os.path.join(options.working_directory, "yt_token")

@contextlib.contextmanager
def temporary_yt_token_file(options):
    filename = get_yt_token_file(options)

    try:
        with open(filename, "w") as outf:
            outf.write(os.environ["TEAMCITY_YT_TOKEN"])
            outf.write("\n")
        yield
    finally:
        if os.path.exists(filename):
            os.remove(filename)

def get_lib_dir_for_python(options, python_version):
    return os.path.join(
        options.working_directory,
        "lib",
        "pyshared-" + python_version.replace(".", "-"))

def iter_enabled_python_versions(options):
    if options.build_enable_python_2_6:
        yield "2.6"

    if options.build_enable_python_2_7:
        yield "2.7"

    if options.build_enable_python_3_4:
        yield "3.4"

def get_ya(options):
    return os.path.join(options.checkout_directory, "ya")

def get_ya_cache_dir(options):
    ya_cache = os.environ.get("YA_CACHE_DIR", None)
    if ya_cache is None:
        ya_cache = os.path.join(options.working_directory, "ya_cache")
    return ya_cache

def get_git_depth(options):
    git_depth = os.path.join(options.checkout_directory, "git-depth.py")
    run_result = run(
        [git_depth],
        cwd=options.checkout_directory,
        capture_output=True)
    return run_result.stdout.rstrip("\n")

def get_http_proxy_nanny_tar_ya_package(options):
    with cwd(get_artifacts_dir(options)):
        generated_package_list = glob.glob("yandex-yt-http-proxy-nanny*.tar")
        assert len(generated_package_list) == 1, "Expected exactly one package, actual: {0}".format(generated_package_list)
        return os.path.realpath(generated_package_list[0])

def ya_make_env(options):
    return {
        "YA_CACHE_DIR": get_ya_cache_dir(options),
    }

def ya_make_definition_args(options):
    # This args cannot be passed to ya package.
    return [
        "-DYT_ENABLE_GDB_INDEX=yes",
        "-DYT_VERSION_PATCH={0}".format(options.patch_number),
        "-DYT_VERSION_BRANCH={0}".format(options.branch),
    ]

def ya_make_yt_store_args(options):
    return [
        "--yt-store",
        "--yt-put",
        "--yt-proxy", YA_CACHE_YT_STORE_PROXY,
        "--yt-dir", YA_CACHE_YT_DIR,
        "--yt-store-codec", YA_CACHE_YT_STORE_CODEC,
        "--yt-max-store-size", str(YA_CACHE_YT_MAX_STORE_SIZE),
        "--yt-store-ttl", str(YA_CACHE_YT_STORE_TTL),
        "--yt-token-path", get_yt_token_file(options),
    ]

def ya_make_args(options):
    return [
        "--build", options.ya_build_type,
    ]

@build_step
def prepare(options, build_context):
    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["LC_ALL"] = "en_US.UTF-8"

    options.build_number = os.environ["BUILD_NUMBER"]
    options.build_vcs_number = os.environ["BUILD_VCS_NUMBER"]

    options.build_enable_nodejs = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_NODEJS", "YES"))
    options.build_enable_python_2_6 = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_PYTHON_2_6", "YES"))
    options.build_enable_python_2_7 = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_PYTHON_2_7", "YES"))
    options.build_enable_python_3_4 = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_PYTHON_3_4", "YES"))
    options.build_enable_python_skynet = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_PYTHON_SKYNET", "YES"))
    options.build_enable_perl = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_PERL", "YES"))
    options.build_enable_ya_yt_store = parse_yes_no_bool(os.environ.get("BUILD_ENABLE_YA_YT_STORE", "NO"))

    options.use_asan = parse_yes_no_bool(os.environ.get("USE_ASAN", "NO"))
    assert not options.use_asan or options.build_system == "ya", "ASAN build is enabled only for --build-system=ya"

    options.use_tsan = parse_yes_no_bool(os.environ.get("USE_TSAN", "NO"))
    options.use_msan = parse_yes_no_bool(os.environ.get("USE_MSAN", "NO"))
    options.use_asan = options.use_asan or parse_yes_no_bool(os.environ.get("BUILD_ENABLE_ASAN", "NO"))  # compat

    options.git_branch = options.branch
    options.branch = re.sub(r"^refs/heads/", "", options.branch)
    options.branch = options.branch.split("/")[0]

    options.patch_number = run_captured([os.path.join(options.checkout_directory, "git-depth.py")], cwd=options.checkout_directory)

    codename = run_captured(["lsb_release", "-c"])
    codename = re.sub(r"^Codename:\s*", "", codename)

    if codename not in ["lucid", "precise", "trusty"]:
        raise RuntimeError("Unknown LSB distribution code name: {0}".format(codename))

    if codename == "lucid":
        options.build_enable_python = options.build_enable_python_2_6
    elif codename in ["precise", "trusty"]:
        options.build_enable_python = options.build_enable_python_2_7

    options.codename = codename
    extra_repositories = filter(lambda x: x != "", map(str.strip, os.environ.get("EXTRA_REPOSITORIES", "").split(",")))
    options.repositories = ["yt-" + codename] + extra_repositories

    if options.build_system != "ya":
        # Now determine the compiler.
        options.cc = run_captured(["which", options.cc])
        options.cxx = run_captured(["which", options.cxx])

        if not options.cc:
            raise RuntimeError("Failed to locate C compiler")

        if not options.cxx:
            raise RuntimeError("Failed to locate CXX compiler")

    # options.use_lto = (options.type != "Debug")
    options.use_lto = False

    options.ya_build_type = {
        "Debug": "debug",
        "Release": "release",
        "RelWithDebInfo": "release",
    }[options.type]

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

    if options.use_asan:
        options.asan_build_directory = os.path.join(options.working_directory, "asan-build")
        mkdirp(options.asan_build_directory)

    cleanup_cgroups()

    if options.clear_system_tmp:
        clear_system_tmp()

    yt_processes_cleanup()

    # Clean core path from previous builds.
    rm_content(options.core_path)

    # Enable cores.
    resource.setrlimit(
        resource.RLIMIT_CORE,
        (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

    mkdirp(get_ya_cache_dir(options))
    mkdirp(get_bin_dir(options))
    mkdirp(options.sandbox_directory)

    os.chdir(options.sandbox_directory)

    teamcity_message(pprint.pformat(options.__dict__))

@build_step
def configure(options, build_context):
    if options.build_system == "cmake":
        run([
                "cmake",
                "-DCMAKE_INSTALL_PREFIX=/usr",
                "-DCMAKE_BUILD_TYPE={0}".format(options.type),
                "-DCMAKE_COLOR_MAKEFILE:BOOL=OFF",
                "-DYT_BUILD_ENABLE_EXPERIMENTS:BOOL=ON",
                "-DYT_BUILD_ENABLE_TESTS:BOOL=ON",
                "-DYT_BUILD_ENABLE_GDB_INDEX:BOOL=ON",
                "-DYT_BUILD_ENABLE_YP:BOOL=ON",
                "-DYT_BUILD_BRANCH={0}".format(options.branch),
                "-DYT_BUILD_NUMBER={0}".format(options.build_number),
                "-DYT_BUILD_VCS_NUMBER={0}".format(options.build_vcs_number[0:10]),
                "-DYT_BUILD_USERNAME=", # Empty string is used intentionally to suppress username in version identifier.
                "-DYT_BUILD_ENABLE_NODEJS={0}".format(format_yes_no(options.build_enable_nodejs)),
                "-DYT_BUILD_ENABLE_PYTHON_2_6={0}".format(format_yes_no(options.build_enable_python_2_6)),
                "-DYT_BUILD_ENABLE_PYTHON_2_7={0}".format(format_yes_no(options.build_enable_python_2_7)),
                "-DYT_BUILD_ENABLE_PYTHON_3_4={0}".format(format_yes_no(options.build_enable_python_3_4)),
                "-DYT_BUILD_ENABLE_PYTHON_SKYNET={0}".format(format_yes_no(options.build_enable_python_skynet)),
                "-DYT_BUILD_ENABLE_PERL={0}".format(format_yes_no(options.build_enable_perl)),
                "-DYT_USE_ASAN={0}".format(format_yes_no(options.use_asan)),
                "-DYT_USE_TSAN={0}".format(format_yes_no(options.use_tsan)),
                "-DYT_USE_MSAN={0}".format(format_yes_no(options.use_msan)),
                "-DYT_USE_LTO={0}".format(format_yes_no(options.use_lto)),
                "-DCMAKE_CXX_COMPILER={0}".format(options.cxx),
                "-DCMAKE_C_COMPILER={0}".format(options.cc),
                "-DBUILD_SHARED_LIBS=OFF",
                options.checkout_directory,
            ],
            cwd=options.working_directory,
        )
    else:
        assert options.build_system == "ya"
        teamcity_message("Ya build doesn't require configuration")

@build_step
def build(options, build_context):
    cpus = int(os.sysconf("SC_NPROCESSORS_ONLN"))
    if options.build_system == "cmake":
        run(["make", "-j", str(cpus)], cwd=options.working_directory, silent_stdout=True)
    else:
        assert options.build_system == "ya"
        ya = get_ya(options)

        common_args = ["-T"]
        common_args += ya_make_args(options)
        common_args += ya_make_definition_args(options)
        if options.build_enable_ya_yt_store:
            common_args += ya_make_yt_store_args(options)

        args = [ya, "make", "buildall"] + common_args
        args += ["--install", get_bin_dir(options)]
        if options.use_asan:
            args += ["--sanitize=address"]

        # We don't want our token to appear in teamcity logs, so we save it to file
        # that will be shortly removed
        with temporary_yt_token_file(options):
            run(args,
                cwd=options.checkout_directory,
                env=ya_make_env(options))

            for python_version in iter_enabled_python_versions(options):
                args = [ya, "make", "buildall/system-python"] + common_args
                args += ["-DUSE_SYSTEM_PYTHON=" + python_version]
                args += ["--install", get_lib_dir_for_python(options, python_version)]
                run(args,
                    cwd=options.checkout_directory,
                    env=ya_make_env(options))

@build_step
def gather_build_info(options, build_context):
    build_context["yt_version"] = run_captured([os.path.join(get_bin_dir(options), "ytserver-master"), "--version"]).strip()
    build_context["build_time"] = datetime.now().isoformat()

@build_step
def set_suid_bit(options, build_context):
    for binary in ["ytserver-node", "ytserver-exec", "ytserver-job-proxy", "ytserver-tools"]:
        path = os.path.join(get_bin_dir(options), binary)
        if options.build_system == "ya":
            # Binaries in bindir are hardlinks to files stored in ya cache directory.
            # we don't want to change their owner and permissions since
            # it will also affect files in ya cache directory.
            # That can make ya crazy. So we replace hard links with real copy of these files.
            copy_path = path + ".copy"
            shutil.copy(path, copy_path)
            shutil.move(copy_path, path)
        run(["sudo", "chown", "root", path])
        run(["sudo", "chmod", "4755", path])

@build_step
def import_yt_wrapper(options, build_context):
    python_directory = os.path.join(options.checkout_directory, "python")

    # ya build doesn't install bindings automatically so we install it here
    # for cmake build this should do no harm
    prepare_python_bindings(python_directory, options.working_directory, "2.7")
    sys.path.insert(0, python_directory)

    try:
        import yt.wrapper
    except ImportError as err:
        raise RuntimeError("Failed to import yt wrapper: {0}".format(err))
    yt.wrapper.config["token"] = os.environ["TEAMCITY_YT_TOKEN"]
    build_context["yt.wrapper"] = yt.wrapper

def sky_get(resource):
    run(
        ["sky", "get", resource],
        timeout=300
    )

def sky_share(resource, cwd):
    run_result = run(
        ["sky", "share", resource],
        cwd=cwd,
        shell=False,
        timeout=600,
        capture_output=True)

    rbtorrent = run_result.stdout.splitlines()[0].strip()
    # simple sanity check
    if urlparse.urlparse(rbtorrent).scheme != "rbtorrent":
        raise RuntimeError("Failed to parse rbtorrent url: {0}".format(rbtorrent))
    return rbtorrent

def share_packages(options, build_context):
    # Share all important packages via skynet and store in sandbox.
    upload_packages = [
        "yandex-yt-python-skynet-driver",
        "yandex-yt-python-driver",
        "yandex-yt-src",
        "yandex-yt-http-proxy",
        "yandex-yt-proxy",
        "yandex-yt-master",
        "yandex-yt-scheduler",
        "yandex-yt-controller-agent",
        "yandex-yt-node",
        "yandex-yt-http-proxy-dbg",
        "yandex-yt-proxy-dbg",
        "yandex-yt-master-dbg",
        "yandex-yt-scheduler-dbg",
        "yandex-yt-controller-agent-dbg",
        "yandex-yt-node-dbg"
    ]

    try:
        version = build_context["yt_version"]
        build_time = build_context["build_time"]
        cli = sandbox_client.SandboxClient(oauth_token=os.environ["TEAMCITY_SANDBOX_TOKEN"])

        dir = os.path.join(options.working_directory, "./ARTIFACTS")
        rows = []
        for pkg in upload_packages:
            path = "{0}/{1}_{2}_amd64.deb".format(dir, pkg, version)
            if not os.path.exists(path):
                teamcity_message("Failed to find package {0} ({1}) ".format(pkg, path), "WARNING")
            else:
                torrent_id = sky_share(os.path.basename(path), os.path.dirname(path))
                sandbox_ctx = {
                    "created_resource_name" : os.path.basename(path),
                    "resource_type" : "YT_PACKAGE",
                    "remote_file_name" : torrent_id,
                    "store_forever" : True,
                    "remote_file_protocol" : "skynet"}

                task_description = """
                    Build id: {0}
                    Build type: {1}
                    Source host: {2}
                    Teamcity build type id: {3}
                    Package: {4}
                    """.format(
                    version,
                    options.type,
                    socket.getfqdn(),
                    options.btid,
                    pkg)

                task_id = cli.create_task("YT_REMOTE_COPY_RESOURCE", "YT_ROBOT", task_description, sandbox_ctx)
                teamcity_message("Created sandbox upload task: package: {0}, task_id: {1}, torrent_id: {2}".format(pkg, task_id, torrent_id))
                rows.append({
                    "package" : pkg,
                    "version" : version,
                    "ubuntu_codename" : options.codename,
                    "torrent_id" : torrent_id,
                    "task_id" : task_id,
                    "build_time" : build_time})

        # Add to locke.
        yt_wrapper = build_context["yt.wrapper"]
        yt_wrapper.config["proxy"]["url"] = "locke"
        yt_wrapper.insert_rows("//sys/admin/skynet/packages", rows)

    except Exception as err:
        teamcity_message("Failed to share packages via locke and sandbox - {0}".format(err), "WARNING")


@build_step
def package_common_packages(options, build_context):
    if options.build_system != "ya":
        return

    PackageTask = collections.namedtuple(
        "PackageTask", [
            "package_file",
            "ya_package_args"])

    PACKAGE_TASK_LIST = [
        PackageTask(
            "yandex-yt-http-proxy-nanny.json",
            ("--tar", "--no-compression")),
    ]
    artifacts_dir = get_artifacts_dir(options)
    os.mkdir(artifacts_dir)
    with cwd(artifacts_dir):
        for package_task in PACKAGE_TASK_LIST:
            package_file = os.path.join(get_bin_dir(options), package_task.package_file)
            args = [
                get_ya(options), "package", package_file,
                "--custom-version", build_context["yt_version"],
            ]
            args += package_task.ya_package_args
            args += ya_make_args(options)
            run(args, env=ya_make_env(options))

    build_python_packages = os.path.join(options.checkout_directory, "scripts", "build-python-packages.py")
    run([
        build_python_packages,
        "--install-dir", get_lib_dir_for_python(options, "2.7"),
        "--output-dir", artifacts_dir
    ])


@build_step
@only_for_projects("yt")
def package(options, build_context):
    if not options.package:
        return

    with cwd(options.working_directory):
        if options.build_system == "cmake":
            run(["make", "-j", "8", "package"])
            run(["make", "-j", "8", "python-package"])
            run(["make", "-j", "8", "python-yp-package"])
            run(["make", "version"])
        else:
            PACKAGE_LIST = [
                "yandex-yt-python-skynet-driver.json",
                "yandex-yt-controller-agent.json",
                "yandex-yt-http-proxy.json",
                "yandex-yt-master.json",
                "yandex-yt-node.json",
                "yandex-yt-proxy.json",
                "yandex-yt-perl.json",
                "yandex-yt-perl-abi.json",
                "yandex-yt-scheduler.json",
                "yandex-yt-src.json",
            ]
            artifacts_dir = get_artifacts_dir(options)
            with cwd(artifacts_dir):
                for package_file in PACKAGE_LIST:
                    package_file = os.path.join(get_bin_dir(options), package_file)
                    with open(package_file) as inf:
                        try:
                            package_name = json.load(inf)["meta"]["name"]
                        except KeyError:
                            RuntimeError("Bad package file {0}, cannot find /meta/name key".format(package_file))
                    args = [
                        get_ya(options), "package", package_file,
                        "--custom-version", build_context["yt_version"],
                        "--debian", "--strip", "--create-dbg",
                        "-zlow"
                    ]
                    args += ya_make_args(options)
                    run(args, env=ya_make_env(options))
                    expected_tar = "{}.{}.tar.gz".format(
                        package_name,
                        build_context["yt_version"])
                    teamcity_message("Extracting archive {}".format(expected_tar))
                    with tarfile.open(expected_tar) as tarf:
                        tarf.extractall(path=artifacts_dir)
                    teamcity_message("Archive {} is extracted".format(expected_tar))

        teamcity_message("We have built a package")
        teamcity_interact("setParameter", name="yt.package_built", value=1)
        teamcity_interact("setParameter", name="yt.package_version", value=build_context["yt_version"])
        teamcity_interact("buildStatus", text="Package: {0}; {{build.status.text}}".format(build_context["yt_version"]))

        share_packages(options, build_context)

        artifacts = glob.glob("./ARTIFACTS/yandex-*{0}*.changes".format(build_context["yt_version"]))
        if artifacts:
            for repository in options.repositories:
                run(["dupload", "--to", repository, "--nomail", "--force"] + artifacts)
                teamcity_message("We have uploaded a package to " + repository)
                teamcity_interact("setParameter", name="yt.package_uploaded." + repository, value=1)


@build_step
def run_prepare_node_modules(options, build_context):
    nodejs_source = os.path.join(options.checkout_directory, "yt", "nodejs")
    nodejs_build = os.path.join(options.working_directory, "yt", "nodejs")

    if options.build_system == "cmake":
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
    else:
        assert options.build_system == "ya"
        mkdirp(nodejs_build)
        with tarfile.open(get_http_proxy_nanny_tar_ya_package(options)) as http_proxy_tar:
            http_proxy_tar.extractall(path=nodejs_build)
        assert os.path.exists(get_node_modules_dir(options))

@build_step
def run_sandbox_upload(options, build_context):
    if not options.package or sys.version_info < (2, 7):
        return

    build_context["sandbox_upload_root"] = os.path.join(options.working_directory, "sandbox_upload")
    sandbox_ctx = {"upload_urls": {}}
    binary_distribution_folder = os.path.join(build_context["sandbox_upload_root"], "bin")
    mkdirp(binary_distribution_folder)

    # Prepare binary distribution folder
    # {working_directory}/bin contains lots of extra binaries,
    # filter daemon binaries by prefix "ytserver-"

    source_binary_root = get_bin_dir(options)
    processed_files = set()
    filename_prefix_whitelist = ["ytserver-", "ypserver-"]
    for filename in os.listdir(source_binary_root):
        if not any(filename.startswith(x) for x in filename_prefix_whitelist):
            continue
        source_path = os.path.join(source_binary_root, filename)
        destination_path = os.path.join(binary_distribution_folder, filename)
        if not os.path.isfile(source_path):
            teamcity_message("Skip non-file item {0}".format(filename))
            continue
        teamcity_message("Symlink {0} to {1}".format(source_path, destination_path))
        os.symlink(source_path, destination_path)
        processed_files.add(filename)

    yt_binary_upload_list = set((
        "ytserver-job-proxy",
        "ytserver-scheduler",
        "ytserver-controller-agent",
        "ytserver-master",
        "ytserver-core-forwarder",
        "ytserver-exec",
        "ytserver-node",
        "ytserver-proxy",
        "ytserver-tools",
        "ypserver-master",
        "ytserver-skynet-manager",
        "ytserver-http-proxy",
    ))
    if yt_binary_upload_list - processed_files:
        missing_file_string = ", ".join(yt_binary_upload_list - processed_files)
        teamcity_message("Missing files in sandbox upload: {0}".format(missing_file_string), "WARNING")

    # Also, inject python libraries and bindings as debs
    artifacts_directory = os.path.join(options.working_directory, "./ARTIFACTS")
    inject_packages = [
        "yandex-yt-python-skynet-driver",
        "yandex-yt-python-driver",
    ]
    for pkg in inject_packages:
        paths = glob.glob("{0}/{1}_*.deb".format(artifacts_directory, pkg))
        if len(paths) != 1:
            teamcity_message("Failed to find package {0}, found files {1}".format(pkg, paths), "WARNING")
            continue
        destination_path = os.path.join(binary_distribution_folder, os.path.basename(paths[0]))
        os.symlink(paths[0], destination_path)

    rbtorrent = sky_share(
            os.path.basename(binary_distribution_folder),
            os.path.dirname(binary_distribution_folder))
    sandbox_ctx["upload_urls"]["yt_binaries"] = rbtorrent

    tmp_dir = os.path.join(options.working_directory, "tmp_package_build")
    mkdirp(tmp_dir)
    # Nodejs package
    if options.build_system == "cmake":
        nodejs_tar = os.path.join(build_context["sandbox_upload_root"],  "node_modules.tar")
        nodejs_build = os.path.join(options.working_directory, "debian/yandex-yt-http-proxy/usr/lib/node_modules")
        with cwd(tmp_dir):
            sky_get(NODEJS_RESOURCE)
            nodejs_path = os.path.realpath("node")
            if not os.path.exists(nodejs_path):
                raise RuntimeError("nodejs resource does not contain 'node' directory")
        with tarfile.open(nodejs_tar, "w", dereference=True) as tar:
            tar.add(nodejs_build, arcname="/node_modules", recursive=True)
            tar.add(nodejs_path, arcname="/node", recursive=True)

        rbtorrent = sky_share("node_modules.tar", build_context["sandbox_upload_root"])
        sandbox_ctx["upload_urls"]["node_modules"] = rbtorrent
    else:
        assert options.build_system == "ya"
        ya_nodejs_tar = os.path.join(build_context["sandbox_upload_root"],  "ya_node_modules.tar")
        os.symlink(get_http_proxy_nanny_tar_ya_package(options), ya_nodejs_tar)

        rbtorrent = sky_share("ya_node_modules.tar", build_context["sandbox_upload_root"])
        sandbox_ctx["upload_urls"]["node_modules"] = rbtorrent
    shutil.rmtree(tmp_dir)

    sandbox_ctx["git_commit"] = options.build_vcs_number
    sandbox_ctx["git_branch"] = options.git_branch
    sandbox_ctx["build_number"] = options.build_number

    #
    # Start sandbox task
    #

    cli = sandbox_client.SandboxClient(oauth_token=os.environ["TEAMCITY_SANDBOX_TOKEN"])
    task_description = """
    YT version: {0}
    Teamcity build id: {1}
    Teamcity build type: {2}
    Teamcity host: {3}
    Teamcity build type id: {4}
    Git branch: {5}
    Git commit: {6}
    """.format(
        build_context["yt_version"],
        options.build_number,
        options.type,
        socket.getfqdn(),
        options.btid,
        options.git_branch,
        options.build_vcs_number,
    )

    task_id = cli.create_task(
        "YT_UPLOAD_RESOURCES",
        "YT_ROBOT",
        task_description,
        sandbox_ctx)
    teamcity_message("Created sandbox upload task: {0}".format(task_id))
    teamcity_message("Check at: https://sandbox.yandex-team.ru/task/{0}/view".format(task_id))
    build_context["sandbox_upload_task"] = task_id

    teamcity_interact("setParameter", name="yt.sandbox_task_id", value=task_id)
    teamcity_interact("setParameter", name="yt.sandbox_task_url",
                      value="https://sandbox.yandex-team.ru/task/{0}/view".format(task_id))
    status = "Package: {0}; SB: {1}; {{build.status.text}}".format(build_context["yt_version"], task_id)
    teamcity_interact("buildStatus", text=status)

@build_step
def run_unit_tests(options, build_context):
    if options.disable_tests:
        teamcity_message("Skipping unit tests since tests are disabled")
        return

    sandbox_current = os.path.join(options.sandbox_directory, "unit_tests")
    sandbox_archive = os.path.join(options.failed_tests_path,
        "__".join([options.btid, options.build_number, "unit_tests"]))

    all_unittests = fnmatch.filter(os.listdir(get_bin_dir(options)), "unittester*")

    mkdirp(sandbox_current)
    try:
        for unittest_binary in all_unittests:
            args = [
                os.path.join(get_bin_dir(options), unittest_binary),
                "--gtest_color=no",
                "--gtest_death_test_style=threadsafe",
                "--gtest_output=xml:" + os.path.join(options.working_directory, "gtest_" + unittest_binary + ".xml"),
            ]
            if not options.use_asan:
                args = [
                    "gdb",
                    "--batch",
                    "--return-child-result",
                    "--command={0}/scripts/teamcity-build/teamcity-gdb-script".format(options.checkout_directory),
                    "--args",
                ] + args
            run(args, cwd=sandbox_current, timeout=20 * 60)
    except ChildHasNonZeroExitCode as err:
        raise StepFailedWithNonCriticalError(str(err))
    finally:
        process_core_dumps(options, "unit_tests", sandbox_current)
        rmtree(sandbox_current)


@build_step
@only_for_projects("yt")
def run_javascript_tests(options, build_context):
    if not options.build_enable_nodejs or options.disable_tests:
        return

    if options.build_system == "cmake":
        tests_path = "{0}/yt/nodejs".format(options.working_directory)
    else:
        assert options.build_system == "ya"
        tests_path = get_bin_dir(options)

    try:
        run(
            ["./run_tests.sh", "-R", "xunit"],
            cwd=tests_path,
            env={"MOCHA_OUTPUT_FILE": "{0}/junit_nodejs_run_tests.xml".format(options.working_directory)})
    except ChildHasNonZeroExitCode as err:
        raise StepFailedWithNonCriticalError(str(err))
    finally:
        process_core_dumps(options, "javascript", tests_path)


def run_pytest(options, suite_name, suite_path, pytest_args=None, env=None, python_version=None):
    yt_processes_cleanup()

    if not options.build_enable_python:
        return

    if python_version is None:
        if not options.build_enable_python_2_7:
            teamcity_message("Skip test suite '{0}' since python2.7 build is disabled".format(suite_name))
            return
        else:
            python_version = "2.7"

    prepare_python_bindings(
        os.path.join(options.checkout_directory, "python"),
        options.working_directory,
        python_version)

    if pytest_args is None:
        pytest_args = []

    sandbox_current, sandbox_storage = get_sandbox_dirs(options, suite_name)
    mkdirp(sandbox_current)

    failed = False

    if env is None:
        env = {}

    env["PATH"] = "{0}:{1}/yt/nodejs:/usr/sbin:{2}".format(get_bin_dir(options), options.working_directory, os.environ.get("PATH", ""))
    env["PYTHONPATH"] = "{0}/python:{0}/yp/python:{1}".format(options.checkout_directory, os.environ.get("PYTHONPATH", ""))
    env["TESTS_SANDBOX"] = sandbox_current
    env["TESTS_SANDBOX_STORAGE"] = sandbox_storage
    env["YT_CAPTURE_STDERR_TO_FILE"] = "1"
    env["YT_ENABLE_VERBOSE_LOGGING"] = "1"
    env["YT_CORE_PATH"] = options.core_path
    if options.build_system == "ya":
        env["PERL5LIB"] = get_bin_dir(options)
    for var in ["TEAMCITY_YT_TOKEN", "TEAMCITY_SANDBOX_TOKEN"]:
        if var in os.environ:
            env[var] = os.environ[var]

    with tempfile.NamedTemporaryFile() as handle:
        try:
            run([
                "python" + python_version,
                "-m",
                "pytest",
                "-r", "x",
                "--verbose",
                "--verbose",
                "--capture=fd",
                "--tb=native",
                "--timeout=3000",
                "--debug",
                "--junitxml={0}".format(handle.name)]
                + pytest_args,
                cwd=suite_path,
                env=env)
        except ChildHasNonZeroExitCode as err:
            teamcity_interact("buildProblem", description="Pytest '{}' failed, exit code {}".format(suite_name, err.return_code))
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

        except (UnicodeDecodeError, ParseError, xml.parsers.expat.ExpatError):
            failed = True
            teamcity_message("Failed to parse pytest output:\n" + open(handle.name).read())

    cores_found = process_core_dumps(options, suite_name, suite_path)

    try:
        if failed or cores_found:
            save_failed_test(options, suite_name, suite_path)
            raise StepFailedWithNonCriticalError("Tests '{0}' failed".format(suite_name))
    finally:
        # Note: ytserver tests may create files with that cannot be deleted by teamcity user.
        sudo_rmtree(sandbox_current)
        if os.path.exists(sandbox_storage):
            sudo_rmtree(sandbox_storage)

@build_step
@only_for_projects("yt")
def run_yt_integration_tests(options, build_context):
    if options.disable_tests:
        teamcity_message("Integration tests are skipped since all tests are disabled")
        return
    if options.use_asan:
        teamcity_message("Integration tests are skipped since they are not quite stable under ASAN")
        return

    pytest_args = []
    if options.enable_parallel_testing:
        pytest_args.extend(["--process-count", str(INTEGRATION_TESTS_PARALLELISM)])

    run_pytest(options, "integration", "{0}/yt/tests/integration".format(options.checkout_directory),
               pytest_args=pytest_args)

@build_step
@only_for_projects("yt")
def run_yt_cpp_integration_tests(options, build_context):
    if options.disable_tests:
        teamcity_message("C++ integration tests are skipped since all tests are disabled")
        return
    run_pytest(options, "cpp_integration", "{0}/yt/tests/cpp".format(options.checkout_directory))

@build_step
@only_for_projects("yp")
def run_yp_integration_tests(options, build_context):
    if options.disable_tests:
        teamcity_message("YP integration tests are skipped since all tests are disabled")
        return
    if options.use_asan:
        teamcity_message("YP integration tests are skipped since they currently don't play well with ASAN")
        return

    pytest_args = []
    if options.enable_parallel_testing:
        pytest_args.extend(["--process-count", str(YP_TESTS_PARALLELISM)])

    node_path = get_node_modules_dir(options)
    for python_version in iter_enabled_python_versions(options):
        if python_version not in {"2.7", "3.4"}:
            continue
        run_pytest(options, "yp_integration", "{0}/yp/tests".format(options.checkout_directory),
           env={
               "NODE_PATH": node_path
           },
           python_version=python_version,
           pytest_args=pytest_args)

@build_step
@only_for_projects("yt")
def run_python_libraries_tests(options, build_context):
    if options.disable_tests:
        teamcity_message("Python tests are skipped since all tests are disabled")
        return
    if options.use_asan:
        teamcity_message("Python tests are skipped since they don't play well with ASAN")
        return

    pytest_args = []
    if options.enable_parallel_testing:
        pytest_args.extend(["--process-count", str(PYTHON_TESTS_PARALLELISM)])

    node_path = get_node_modules_dir(options)
    run_pytest(options, "python_libraries", "{0}/python".format(options.checkout_directory),
               pytest_args=pytest_args,
               env={
                   "TESTS_JOB_CONTROL": "1",
                   "YT_ENABLE_REQUEST_LOGGING": "1",
                   "NODE_PATH": node_path
                })

@build_step
@only_for_projects("yt")
def run_perl_tests(options, build_context):
    if not options.build_enable_perl:
        teamcity_message("Perl tests are skipped since they are not enabled")
        return
    if options.disable_tests:
        teamcity_message("Perl tests are skipped since all tests are disabled")
        return
    if options.use_asan:
        teamcity_message("Perl tests are skipped since they don't play well with ASAN")
        return
    run_pytest(options, "perl", "{0}/perl/tests".format(options.checkout_directory))

def log_sandbox_upload(options, build_context, task_id):
    client = requests.Session()
    client.headers.update({
        "Authorization" : "OAuth {0}".format(os.environ["TEAMCITY_SANDBOX_TOKEN"]),
        "Accept" : "application/json; charset=utf-8",
        "Content-type" : "application/json",
    })
    resp = client.get("https://sandbox.yandex-team.ru/api/v1.0/task/{0}/resources".format(task_id))
    api_data = resp.json()

    resources = {}
    resource_rows = []
    for resource in api_data["items"]:
        if resource["type"] == "TASK_LOGS":
            continue
        resources.update({
            resource["type"] : resource["id"],
        })
        resource_rows.append({
            "id" : resource["id"],
            "task_id" : task_id,
            "type" : resource["type"],
            "build_number" : int(options.build_number),
            "version" : build_context["yt_version"],
        })

    build_log_record = {
        "version" : build_context["yt_version"],
        "build_number" : int(options.build_number),
        "task_id" : task_id,
        "git_branch" : options.git_branch,
        "git_commit" : options.build_vcs_number,
        "build_time" : build_context["build_time"],
        "build_type" : options.type,
        "build_host" : socket.getfqdn(),
        "build_btid" : options.btid,
        "ubuntu_codename" : options.codename,
        "resources" : resources,
    }

    # Add to locke.
    yt_wrapper = build_context["yt.wrapper"]
    yt_wrapper.config["proxy"]["url"] = "locke"
    yt_wrapper.insert_rows("//sys/admin/skynet/builds", [build_log_record])
    yt_wrapper.insert_rows("//sys/admin/skynet/resources", resource_rows)

@build_step
def wait_for_sandbox_upload(options, build_context):
    if not options.package or sys.version_info < (2, 7):
        return

    task_id = build_context["sandbox_upload_task"]
    teamcity_message("Loaded task id: {0}".format(task_id))
    teamcity_message("Check at: https://sandbox.yandex-team.ru/task/{0}/view".format(task_id))
    cli = sandbox_client.SandboxClient(oauth_token=os.environ["TEAMCITY_SANDBOX_TOKEN"])
    try:
        cli.wait_for_complete(task_id)
    except sandbox_client.SandboxTaskError as err:
        teamcity_message("Failed waiting for task: {0}".format(err), status="WARNING")
    try:
        log_sandbox_upload(options, build_context, task_id)
    except Exception as err:
        teamcity_message("Failed to log sandbox upload: {0}".format(err), status="WARNING")

@cleanup_step
def clean_sandbox_upload(options, build_context):
    if "sandbox_upload_root" in build_context and os.path.exists(build_context["sandbox_upload_root"]):
        shutil.rmtree(build_context["sandbox_upload_root"])

@cleanup_step
def clean_artifacts(options, build_context, n=10):
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
def clean_failed_tests(options, build_context, max_allowed_size=None):
    if options.is_bare_metal:
        max_allowed_size = 700 * 1024 * 1024 * 1024
    else:
        max_allowed_size = 50 * 1024 * 1024 * 1024

    should_remove = False
    total_size = 0
    for path in ls(options.failed_tests_path,
                   select=os.path.isdir,
                   stop=sys.maxint):
        size = get_size(path, enable_cache=True)
        if total_size + size > max_allowed_size:
            should_remove = True

        if should_remove:
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
        "--build_system",
        choices=["cmake", "ya"], default="cmake")

    parser.add_argument(
        "--clear-system-tmp",
        type=parse_bool, action="store", default=True)

    parser.add_argument(
        "--type",
        type=str, action="store", required=True, choices=("Debug", "Release", "RelWithDebInfo"))

    parser.add_argument(
        "--package",
        type=parse_bool, action="store", default=False)

    parser.add_argument(
        "--disable_tests",
        type=parse_bool, action="store", default=False)

    parser.add_argument(
        "--cc",
        type=str, action="store", required=False, default="gcc-4.8")
    parser.add_argument(
        "--cxx",
        type=str, action="store", required=False, default="g++-4.8")

    parser.add_argument(
        "--build_project",
        type=comma_separated_set, action="store", default={"yt", "yp"})

    options = parser.parse_args()
    options.failed_tests_path = os.path.expanduser("~/failed_tests")
    options.core_path = os.path.expanduser("~/core")
    options.is_bare_metal = socket.getfqdn().endswith("tc.yt.yandex.net")
    # NB: parallel testing is enabled by default only for bare metal machines.
    options.enable_parallel_testing = options.is_bare_metal

    teamcity_main(options)

if __name__ == "__main__":
    main()
