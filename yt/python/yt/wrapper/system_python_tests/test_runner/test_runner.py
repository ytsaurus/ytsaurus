from yt.environment import arcadia_interop
import yt.logger as logger

import library.python.testing.pytest_runner.runner as pytest_runner

import yatest.common

import imp
import os
import shutil

def build_bindings(build_dir, python_version):
    ya = yatest.common.source_path("ya")
    args = [
        "/usr/bin/python", ya, "make",
        "--source-root", yatest.common.source_path(),
        "--install", build_dir,
        "-DUSE_SYSTEM_PYTHON=" + python_version,
        "-DPYTHON_CONFIG=python{}-config".format(python_version),
        "-DPYTHON_BIN=python" + python_version,
        "-C", "tools/fix_elf",
        "-C", "yt/yt/python/yson_shared",
        "-C", "yt/yt/python/driver/native_shared",
        "-C", "yt/yt/python/driver/rpc_shared",
    ]
    yatest.common.execute(args, stdout="ya_make.out", stderr="ya_make.err")

def prepare_python_packages(destination):
    source = os.path.join(yatest.common.source_path(), "yt/python")

    shutil.copytree(source, destination, symlinks=False)

    # Cleanup python artifacts
    for root, dirs, files in os.walk(destination):
        for file in files:
            if file.endswith(".pyc"):
                os.remove(os.path.join(root, file))
        for dir in dirs:
            if dir == "__pycache__":
                shutil.rmtree(os.path.join(root, dir))

    prepare_source_tree = imp.load_source(
        "prepare_source_tree",
        os.path.join(destination, "prepare_source_tree/prepare_source_tree.py"))
    prepare_source_tree.prepare_python_source_tree(
        python_root=destination,
        yt_root=os.path.join(yatest.common.source_path(), "yt"),
        arcadia_root=yatest.common.source_path(),
        prepare_binary_symlinks=False,
        prepare_bindings=True)

def run_pytest(major_python_version):
    # Skip this test on teamcity.
    if yatest.common.get_param("teamcity"):
        return

    system_python_test_param = None
    if major_python_version == 2:
        system_python_test_param = "system_python"
        python_version = yatest.common.get_param("system_python", "2.7")
        assert python_version.split(".")[0] == "2"
    elif major_python_version == 3:
        system_python_test_param = "system_python3"
        python_version = yatest.common.get_param("system_python3", "3.5")
        assert python_version.split(".")[0] == "3"
    else:
        raise Exception("Unknown major python version " + str(major_python_version))


    python_binary = "/usr/bin/python" + python_version
    if not os.path.exists(python_binary):
        raise Exception("Python {} is not available on the host, try to specify "
                        "another one by --test-param {}={{my_version}}"
                        .format(python_binary, system_python_test_param))

    build_dir = os.path.join(yatest.common.work_path(), "build")

    bindings_build_dir = os.path.join(build_dir, "python_bindings")
    os.makedirs(bindings_build_dir)
    build_bindings(bindings_build_dir, python_version)

    prepared_python_dir = os.path.join(yatest.common.output_ram_drive_path(), "prepared_python")
    prepare_python_packages(prepared_python_dir)

    sandbox_dir = os.path.join(yatest.common.output_ram_drive_path(), "sandbox")

    path = arcadia_interop.prepare_yt_environment(
        build_dir,
        use_ytserver_all=True,
        copy_ytserver_all=True,
        need_suid=True)
    if "PATH" in os.environ:
        path = os.pathsep.join([path, os.environ["PATH"]])

    env = {
        "PATH": path,
        "PYTHONPATH": os.pathsep.join([
            bindings_build_dir,
            prepared_python_dir,
        ]),
        "TESTS_SANDBOX": sandbox_dir,
        "YT_CAPTURE_STDERR_TO_FILE": "1",
        "YT_ENABLE_VERBOSE_LOGGING": "1",
    }

    test_paths_file = os.path.join(
        prepared_python_dir,
        "yt/wrapper/system_python_tests/test_paths.txt")
    test_paths = open(test_paths_file).read().split()
    test_files = [
        os.path.join(prepared_python_dir, "yt/wrapper/tests", name)
        for name in test_paths
    ]

    pytest_runner.run(
        test_files,
        python_path="/usr/bin/python" + python_version,
        env=env,
        pytest_args=["-v", "-s", "--process-count=10"],
        # Default timeout for large tests is 1 hour.
        # We use 50 minutes here as we need time to save results.
        timeout=3000)

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
