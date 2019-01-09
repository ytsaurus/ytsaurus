from yt.environment import arcadia_interop

import library.python.testing.pytest_runner.runner as pytest_runner
#import library.python.cgroups as cgroups

import yatest.common

import imp
import os
#import sys

YT_ABI = "19_4"

def build_bindings(build_dir):
    ya = yatest.common.source_path("ya")
    yatest.common.execute([
        "/usr/bin/python", ya, "make",
        "--source-root", yatest.common.source_path(),
        "--results-root", build_dir,
        "-DUSE_SYSTEM_PYTHON=2.7", "-DPYTHON_CONFIG=python2.7-config", "-DPYTHON_BIN=python2.7",
        "-C", "yt/{}/yt/python/yson_shared".format(YT_ABI),
        "-C", "yt/{}/yt/python/driver_shared".format(YT_ABI)
    ])

def prepare_python_packages():
    python_root = yatest.common.source_path("yt/python")
    yt_root = yatest.common.source_path(os.path.join("yt", YT_ABI))

    prepare_source_tree = imp.load_source("prepare_source_tree", os.path.join(python_root, "prepare_source_tree.py"))
    prepare_source_tree.prepare_python_source_tree(
        python_root,
        yt_root,
        prepare_binary_symlinks=False,
        prepare_bindings=False)

def run_pytest():
    build_dir = os.path.join(yatest.common.work_path(), "build")
    bindings_build_dir = os.path.join(build_dir, "bindings")
    os.makedirs(bindings_build_dir)
    build_bindings(bindings_build_dir)

    prepare_python_packages()

    path = arcadia_interop.prepare_yt_environment(build_dir)
    if "PATH" in os.environ:
        path = os.pathsep.join([path, os.environ["PATH"]])

    sandbox_dir = os.path.join(yatest.common.output_path(), "sandbox")
    env = {
        "PATH": path,
        "PYTHONPATH": os.pathsep.join([
            os.path.join(yatest.common.source_path(), "yt", "python"),
            os.path.join(yatest.common.source_path(), "yt", YT_ABI, "yt", "python", "yt_yson_bindings"),
            os.path.join(yatest.common.source_path(), "yt", YT_ABI, "yt", "python", "yt_driver_bindings"),
            os.path.join(bindings_build_dir, "yt", YT_ABI, "yt", "python", "yson_shared"),
            os.path.join(bindings_build_dir, "yt", YT_ABI, "yt", "python", "driver_shared")
        ]),
        "TESTS_SANDBOX": sandbox_dir,
        "TESTS_JOB_CONTROL": "1",
        "YT_CAPTURE_STDERR_TO_FILE": "1",
        "YT_ENABLE_VERBOSE_LOGGING": "1",
    }

    test_files = [
        yatest.common.source_path("yt/python/yt/wrapper/tests/test_operations_pickling.py"),
        # User statistics uses cgroups that available only in FAT tests.
        yatest.common.source_path("yt/python/yt/wrapper/tests/test_user_statistics.py"),
    ]

    #cgroup = None
    try:
        #print >>sys.stderr, "AAAAAAAAAAAAA"
        #cgroup = cgroups.CGroup("test", subsystems=("cpuacct", "cpu", "blkio", "freezer")).create()
        #print >>sys.stderr, "BBBBBBBBBBBBB"
        pytest_runner.run(test_files, python_path="/usr/bin/python2.7", env=env)
    finally:
        pass
        #print >>sys.stderr, "CCCCCCCCCC"
        #if cgroup is not None:
        #    cgroup.delete()
