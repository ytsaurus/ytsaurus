from yt.environment import arcadia_interop

import library.python.testing.pytest_runner.runner as pytest_runner

import yatest.common

import os
import shutil

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

def copy_file(src, dst):
    if os.path.exists(dst):
        os.remove(dst)
    shutil.copy(src, dst)

# NOTE(asaitgalin): Keep this stuff synchronized with yt/python/contrib
def prepare_yt_packages():
    packages_path = yatest.common.source_path("yt/python/yt/packages")
    contrib_path = yatest.common.source_path("yt/python/contrib")

    for package in ("requests", "argcomplete", "certifi",
                    "dill", "urllib3", "certifi", "chardet", "idna"):
        src_path = os.path.join(contrib_path, "python-" + package, package)
        dst_path = os.path.join(packages_path, package)

        if os.path.exists(dst_path):
            shutil.rmtree(dst_path)
        shutil.copytree(src_path, dst_path)

    # Simplejson
    src_path = yatest.common.source_path("contrib/python/simplejson/simplejson")
    dst_path = yatest.common.source_path("yt/python/simplejson")
    if os.path.exists(dst_path):
        shutil.rmtree(dst_path)
    shutil.copytree(src_path, dst_path)
    
    # Tornado
    src_path = yatest.common.source_path("contrib/python/tornado/tornado")
    dst_path = yatest.common.source_path("yt/python/tornado")
    if os.path.exists(dst_path):
        shutil.rmtree(dst_path)
    shutil.copytree(src_path, dst_path)

    # Six
    copy_file(
        os.path.join(contrib_path, "python-six", "six.py"),
        os.path.join(packages_path, "six.py"))

    # Backports
    backports_src = os.path.join(contrib_path, "python-backports.ssl_match_hostname", "backports")
    backports_dst = os.path.join(packages_path, "backports")
    if os.path.exists(backports_dst):
        shutil.rmtree(backports_dst)
    shutil.copytree(backports_src, backports_dst)

    # Backports ABC
    copy_file(
        os.path.join("contrib/python", "python-backports_abc", "backports_abc.py"),
        os.path.join(packages_path, "backports.abc"))

    # Singledispatch
    for file_ in ("singledispatch_helpers.py", "singledispatch.py"):
        copy_file(
            os.path.join("contrib/python", "python-singledispatch", file_),
            os.path.join(packages_path, file_))

    # FUSE
    copy_file(
        os.path.join(contrib_path, "python-fusepy", "fuse.py"),
        os.path.join(packages_path, "fuse.py"))

    # Decorator
    copy_file(
        os.path.join(contrib_path, "python-decorator", "src", "decorator.py"),
        os.path.join(packages_path, "decorator.py"))

def run_pytest():
    build_dir = os.path.join(yatest.common.work_path(), "build")
    bindings_build_dir = os.path.join(build_dir, "bindings")
    os.makedirs(bindings_build_dir)

    prepare_yt_packages()
    path = arcadia_interop.prepare_yt_environment(build_dir)
    build_bindings(bindings_build_dir)

    if "PATH" in os.environ:
        path = os.pathsep.join([path, os.environ["PATH"]])

    sandbox_dir = os.path.join(yatest.common.output_path(), "sandbox")
    env = {
        "PATH": path,
        "PYTHONPATH": os.pathsep.join([
            os.path.join(yatest.common.source_path(), "yt", "python"),
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
        yatest.common.source_path("yt/python/yt/wrapper/tests/test_user_statistics.py")
    ]

    pytest_runner.run(test_files, python_path="/usr/bin/python2.7", env=env)
