from yt.environment import arcadia_interop
import yt.logger as logger

import library.python.testing.pytest_runner.runner as pytest_runner
import library.python.cgroups as cgroups

import yatest.common

import imp
import os

YT_ROOT, PYTHON_ROOT, _ = arcadia_interop.get_root_paths()

def build_bindings(build_dir, python_version):
    ya = yatest.common.source_path("ya")
    args = [
        "/usr/bin/python", ya, "make",
        "--source-root", yatest.common.source_path(),
        "--results-root", build_dir,
        "-DUSE_SYSTEM_PYTHON=" + python_version, "-DPYTHON_CONFIG=python{}-config".format(python_version), "-DPYTHON_BIN=python" + python_version,
        "-C", "tools/fix_elf",
        "-C", "{}yt/python/yson_shared".format(YT_ROOT),
        "-C", "{}yt/python/driver/native_shared".format(YT_ROOT),
        "-C", "{}yt/python/driver/rpc_shared".format(YT_ROOT)
    ]
    yatest.common.execute(args, stdout="ya_make.out", stderr="ya_make.err")

def prepare_python_packages():
    prepare_source_tree = imp.load_source("prepare_source_tree", os.path.join(yatest.common.source_path(), PYTHON_ROOT, "prepare_source_tree.py"))
    prepare_source_tree.prepare_python_source_tree(
        python_root=os.path.join(yatest.common.source_path(), PYTHON_ROOT),
        # TODO(ignat): improve prepare_source_tree.
        # yt_root is actually a root to yt repo (we use both bindings and contrib from it).
        # os.path.join(yatest.common.source_path(), YT_ROOT),
        yt_root=yatest.common.source_path(),
        prepare_binary_symlinks=False,
        prepare_bindings=False)

def run_pytest(python_version):
    build_dir = os.path.join(yatest.common.work_path(), "build")
    bindings_build_dir = os.path.join(build_dir, "bindings")
    os.makedirs(bindings_build_dir)
    build_bindings(bindings_build_dir, python_version)

    prepare_python_packages()

    path = arcadia_interop.prepare_yt_environment(build_dir, use_ytserver_all=True, copy_ytserver_all=True)
    if "PATH" in os.environ:
        path = os.pathsep.join([path, os.environ["PATH"]])

    sandbox_dir = os.path.join(yatest.common.output_ram_drive_path(), "sandbox")
    env = {
        "PATH": path,
        "PYTHONPATH": os.pathsep.join([
            os.path.join(yatest.common.source_path(), PYTHON_ROOT),
            os.path.join(yatest.common.source_path(), YT_ROOT, "yt", "python"),
            os.path.join(bindings_build_dir, YT_ROOT, "yt", "python", "yson_shared"),
            os.path.join(bindings_build_dir, YT_ROOT, "yt", "python", "driver", "native_shared"),
            os.path.join(bindings_build_dir, YT_ROOT, "yt", "python", "driver", "rpc_shared")
        ]),
        "TESTS_SANDBOX": sandbox_dir,
        "TESTS_JOB_CONTROL": "1",
        "YT_CAPTURE_STDERR_TO_FILE": "1",
        "YT_ENABLE_VERBOSE_LOGGING": "1",
    }

    test_paths_file = os.path.join(yatest.common.source_path(), PYTHON_ROOT, "yt/wrapper/system_python_tests/test_paths.txt")
    test_paths = open(test_paths_file).read().split()
    test_files = [
        os.path.join(yatest.common.source_path(), PYTHON_ROOT, "yt/wrapper/tests", name)
        for name in test_paths
    ]

    cgroup = None
    try:
        cgroup = cgroups.CGroup("test", subsystems=("cpuacct", "cpu", "blkio", "freezer")).create()
        pytest_runner.run(
            test_files,
            python_path="/usr/bin/python" + python_version,
            env=env,
            pytest_args=["-v", "-s", "--process-count=10"],
            # Default timeout for large tests is 1 hour.
            # We use 50 minutes here as we need time to save results.
            timeout=3000)
    finally:
        if cgroup is not None:
            cgroup.delete()

    pids = []
    for root, dirs, files in os.walk(sandbox_dir):
        for file in files:
            if file != "pids.txt":
                continue
            pids_file = os.path.join(root, file)
            if os.path.exists(pids_file):
                with open(pids_file) as f:
                    for line in f.readlines():
                        try:
                            pids.append(int(line))
                        except ValueError:
                            pass

    arcadia_interop.collect_cores(
        pids,
        sandbox_dir,
        [os.path.join(build_dir, binary) for binary in os.listdir(build_dir)],
        logger=logger)
