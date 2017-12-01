import os
import yatest.common

import library.python.testing.pytest_runner.runner as pytest_runner

PYTHON3 = "/usr/bin/python3.4"


def build_yt_py3():
    build_with_py3_dir = yatest.common.work_path("build_py3")
    os.makedirs(build_with_py3_dir)
    ya = yatest.common.source_path("ya")
    yatest.common.execute([
        PYTHON3, ya, "make",
        "--source-root", yatest.common.source_path(),
        "--results-root", build_with_py3_dir,
        "-DUSE_ARCADIA_PYTHON=no", "-DPYTHON_CONFIG=python3.4-config", "-DPYTHON_BIN=python3.4",
        "-C", "yt/19_2/yt/python/yson_shared",
        "-C", "yt/19_2/yt/python/driver_shared",
    ])
    return build_with_py3_dir


def run_pytest():
    build_dir = build_yt_py3()
    env = os.environ.copy()
    env = {"PYTHONPATH": os.path.pathsep.join([
        env.get("PYTHONPATH", ""),
        os.path.join(build_dir, "yt/19_2/yt/python/yson_shared"),
        os.path.join(build_dir, "yt/19_2/yt/python/driver_shared"),
    ])}
    pytest_runner.run(
        [
            yatest.common.source_path("yt/python/py3_tests/tests"),
        ],
        pytest_args=["--noconftest"],  # XXX remove when real YT tests are properly configured
        python_path=PYTHON3,
        env=env,
    )
