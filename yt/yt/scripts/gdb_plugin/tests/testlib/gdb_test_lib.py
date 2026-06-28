"""Shared scaffolding for the gdb_plugin tests: fixture context, one-time
(cached) core generation, and the `analyze` helper."""

from yt.environment import arcadia_interop

import logging
import os
import signal
import subprocess

import pytest

root_logger = logging.getLogger()

signal.signal(signal.SIGTTOU, signal.SIG_IGN)


def get_context():
    yc = arcadia_interop.yatest_common
    return dict(
        gdbpath=yc.gdb_path(),
        # The entry point bootstraps the whole gdb/ plugin directory.
        plugin=yc.source_path("yt/yt/scripts/gdb_plugin/lib/__init__.py"),
        fixture=yc.binary_path("yt/yt/scripts/gdb_plugin/tests/fixture/fixture"),
        output=yc.output_path(),
    )


def run(cmd, check=True):
    env = os.environ.copy()
    # NB: Strings are not printed correctly in gdb otherwise.
    env["LC_ALL"] = "en_US.UTF-8"
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env).decode("UTF-8")
    except subprocess.CalledProcessError as exc:
        out = exc.output.decode("UTF-8")
        root_logger.exception("gdb exited with code {}\n{}".format(exc.returncode, out))
        # A command that raises GdbError makes gdb exit non-zero (e.g. yt-fiber-select
        # on a coredump); the caller passes check=False to inspect the message.
        if not check:
            return out
        raise


def generate_core(ctx):
    # The walker analyzes coredumps: its heap-zone discovery keys off the LOAD
    # segments a core carries (a live process exposes no such segments for the
    # anonymous heap). So freeze the program at the breakpoint and dump a core.
    core = os.path.join(ctx["output"], "fixture.core")
    run([
        ctx["gdbpath"], "-nx", "-batch",
        "-ex", "break StopHere",
        "-ex", "run",
        "-ex", "generate-core-file " + core,
        ctx["fixture"],
    ])
    return core


def analyze(ctx, core, command, check=True):
    out = run([
        ctx["gdbpath"], "-nx", "-batch",
        "-ix", ctx["plugin"],
        "-ex", "set charset UTF-8",
        "-ex", command,
        ctx["fixture"], core,
    ], check=check)
    root_logger.info("Command %r output\n%s\n", command, out)
    assert len(out) > 0
    return out


_core_cache = None


def get_core():
    """(ctx, core_path), generated once and cached for the whole test session.
    Skips the test when run outside the arcadia test harness."""
    global _core_cache
    if _core_cache is None:
        if arcadia_interop.yatest_common is None:
            pytest.skip()
        ctx = get_context()
        _core_cache = (ctx, generate_core(ctx))
    return _core_cache
