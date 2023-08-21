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

    # NB: strings are not printed correctly in gdb otherwise.
    env["LC_ALL"] = "en_US.UTF-8"
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    except subprocess.CalledProcessError as exc:
        root_logger.exception("gdb exited with code {}\n{}".format(exc.returncode, exc.output.decode("UTF-8")))
        copy_exe()
        raise
    return out


def print_fibers(command):
    actual_output = gdb("b StopHere", "r", command).decode("UTF-8")
    root_logger.info("Actual output\n{}\n".format(actual_output))
    assert len(actual_output) > 0
    return actual_output


def check_backtraces(actual_output):
    fiber_count = 0
    filtered_running_fibers = False
    foo_count = 0
    bar_count = 0
    async_stop_count = 0
    for line in actual_output.split("\n"):
        if line.find("0x0000000000000000 in ?? ()") != -1:
            fiber_count += 1
        if line.find("Filtered out 1 running fiber(s)"):
            filtered_running_fibers = True
        if line.find("Foo") != -1:
            foo_count += 1
        if line.find("Bar") != -1:
            bar_count += 1
        if line.find("AsyncStop") != -1:
            async_stop_count += 1
    assert fiber_count > 0
    assert filtered_running_fibers
    assert foo_count == 6
    assert bar_count == 5
    assert async_stop_count > 0


def check_tags(actual_output):
    tags = None
    logging_tag = None
    for line in actual_output.split("\n"):
        if line.find("Tags: ") != -1:
            assert tags is None
            tags = line
        if line.find("Logging tag: ") != -1:
            assert logging_tag is None
            logging_tag = line
    assert tags == "Tags: tag = value, tag0 = value0"
    assert logging_tag == "Logging tag: LoggingTag"


@authors("shishmak")
def test_print_yt_fibers():
    if arcadia_interop.yatest_common is None:
        pytest.skip()

    actual_output = print_fibers("print_yt_fibers")
    check_backtraces(actual_output)


@authors("shishmak")
def test_print_yt_fibers_with_tags():
    if arcadia_interop.yatest_common is None:
        pytest.skip()

    actual_output = print_fibers("print_yt_fibers_with_tags")
    check_backtraces(actual_output)
    check_tags(actual_output)
