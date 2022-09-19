from yt_commands import authors

from yt.environment import arcadia_interop

import logging
import os
import signal
import subprocess

import pytest

root_logger = logging.getLogger()

signal.signal(signal.SIGTTOU, signal.SIG_IGN)


def get_gdb_context():
    yc = arcadia_interop.yatest_common
    return dict(
        gdbpath=yc.gdb_path(),
        gdbinit=yc.source_path("devtools/gdb/__init__.py"),
        gdbtest=yc.binary_path("yt/yt/tests/integration/yt_fibers_printer/gdbtest/gdbtest"),
    )


def copy_exe():
    gdb_context = get_gdb_context()
    try:
        subprocess.check_output([
            "cp",
            gdb_context["gdbtest"],
            os.path.join(arcadia_interop.yatest_common.output_path(), "executable")],
            stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        root_logger.exception("Failed to copy exe")


def gdb(*commands):
    gdb_context = get_gdb_context()
    cmd = [gdb_context["gdbpath"], "-nx", "-batch", "-ix", gdb_context["gdbinit"], "-ex", "set charset UTF-8"]
    for c in commands:
        cmd += ["-ex", c]
    cmd += [gdb_context["gdbtest"]]
    env = os.environ.copy()

    # NB: strings are not printed correctly in gdb overwise.
    env["LC_ALL"] = "en_US.UTF-8"
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    except subprocess.CalledProcessError as exc:
        root_logger.exception("gdb exited with code {}".format(exc.returncode))
        copy_exe()
        raise
    return out


def print_fibers():
    return gdb("b StopHere", "run", "print_yt_fibers")


@authors("shishmak")
def test_fibers_printer():
    if arcadia_interop.yatest_common is None:
        pytest.skip()

    actual_output = print_fibers().decode("UTF-8")
    with open(os.path.join(arcadia_interop.yatest_common.output_path(), "actual_output.txt"), "w") as output:
        output.write(actual_output)

    assert len(actual_output) > 0
    fibers_count = 0
    foo_count = 0
    bar_count = 0
    async_stop_count = 0
    for line in actual_output.split("\n"):
        if line.find("0x0000000000000000 in ?? ()") != -1:
            fibers_count += 1
        if line.find("Foo") != -1:
            foo_count += 1
        if line.find("Bar") != -1:
            bar_count += 1
        if line.find("AsyncStop") != -1:
            async_stop_count += 1
    assert fibers_count >= 2
    assert foo_count == 6
    assert bar_count == 5
    assert async_stop_count == 1
