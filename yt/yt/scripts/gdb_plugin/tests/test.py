from yt_commands import authors

from yt.environment import arcadia_interop

import logging
import os
import re
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


def run(cmd):
    env = os.environ.copy()
    # NB: Strings are not printed correctly in gdb otherwise.
    env["LC_ALL"] = "en_US.UTF-8"
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env).decode("UTF-8")
    except subprocess.CalledProcessError as exc:
        root_logger.exception("gdb exited with code {}\n{}".format(exc.returncode, exc.output.decode("UTF-8")))
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


def analyze(ctx, core, command):
    out = run([
        ctx["gdbpath"], "-nx", "-batch",
        "-ix", ctx["plugin"],
        "-ex", "set charset UTF-8",
        "-ex", command,
        ctx["fixture"], core,
    ])
    root_logger.info("Command %r output\n%s\n", command, out)
    assert len(out) > 0
    return out


@pytest.fixture(scope="module")
def core():
    if arcadia_interop.yatest_common is None:
        pytest.skip()
    ctx = get_context()
    return ctx, generate_core(ctx)


@authors("babenko")
def test_obj(core):
    ctx, path = core
    out = analyze(ctx, path, "yt-rc-obj GdbCycleHeadAddress")
    # Type resolved from the vptr, counter located via the address-salted signature.
    assert "TGdbCycleHead" in out
    assert re.search(r"alive\s+yes", out)


@authors("babenko")
def test_cycle(core):
    ctx, path = core
    out = analyze(ctx, path, "yt-rc-cycle GdbCycleHeadAddress")
    # head -> tail -> head must close as a cycle.
    assert "CYCLE" in out
    assert "holds back into the start object" in out


@authors("babenko")
def test_backref(core):
    ctx, path = core
    out = analyze(ctx, path, "yt-rc-backref GdbCycleHeadAddress")
    # The tail object holds the head via its intrusive-ptr member.
    assert "TGdbCycleTail" in out


@authors("babenko")
def test_find(core):
    ctx, path = core
    out = analyze(ctx, path, "yt-rc-find TGdbLiveSolo")
    assert "TGdbLiveSolo" in out


@authors("babenko")
def test_fiber_unwind(core):
    ctx, path = core
    # The object is pinned only by a parked fiber's stack -> off-heap attribution
    # must fire and the fiber must unwind to a clean, recognizable chain. This
    # exercises the rbp-walk path in debug builds (frame pointers) and the stack
    # scan in release builds (frame pointers omitted).
    out = analyze(ctx, path, "yt-rc-backref GdbFiberHeldAddress")
    assert "parked fiber" in out
    assert "WaitUntilSet" in out
    assert "FiberTrampoline" in out


@authors("babenko")
def test_thread_unwind(core):
    ctx, path = core
    # The object is pinned only by a running thread's stack (a main() local) ->
    # attributed to that thread with its native backtrace.
    out = analyze(ctx, path, "yt-rc-backref GdbThreadHeldAddress")
    assert "running thread" in out
    assert "StopHere" in out


@authors("babenko")
def test_final_type(core):
    ctx, path = core
    # New<T> for a final, non-TRefCounted type lays the counter before the object
    # (no virtual-base cast). The walker must still identify it. (Counts/liveness
    # come from the signature, exercised in debug / signature-enabled builds; the
    # signature mechanism itself is unit-tested in intrusive_ptr_ut.cpp.)
    out = analyze(ctx, path, "yt-rc-obj GdbFinalAddress")
    assert "TGdbFinalThing" in out


@authors("babenko")
def test_atomic_intrusive_ptr(core):
    ctx, path = core
    # The object is held via TAtomicIntrusivePtr, which packs a local refcount
    # into the pointer's top bits -- the holder must still be found (low-48-bit
    # match) and attributed to its container.
    out = analyze(ctx, path, "yt-rc-backref GdbAtomicHeldAddress")
    assert "TGdbAtomicHolder" in out


@authors("babenko")
def test_cycle_root(core):
    ctx, path = core
    # Tracing an object with no live heap holder bottoms out at a ROOT and is
    # attributed off-heap (here: a running thread's stack).
    out = analyze(ctx, path, "yt-rc-cycle GdbThreadHeldAddress")
    assert "ROOT" in out
    assert "running thread" in out


@authors("babenko")
def test_find_none(core):
    ctx, path = core
    out = analyze(ctx, path, "yt-rc-find ThisTypeDoesNotExistAnywhere")
    assert "none found" in out


@authors("babenko")
def test_weak_holder_classified(core):
    ctx, path = core
    # The child holds the parent via a TWeakPtr member. The holder must be
    # classified 'weak' (from the container's real field type), not a candidate
    # strong ref -- otherwise a weak back-edge would fabricate a retention cycle.
    out = analyze(ctx, path, "yt-rc-backref GdbWeakParentAddress")
    assert re.search(r"weak\b.*TGdbWeakChild", out)


@authors("babenko")
def test_weak_edge_not_a_cycle(core):
    ctx, path = core
    # Parent --strong--> Child --weak--> Parent. The weak edge must NOT close a
    # cycle: tracing the child bottoms out at a ROOT, never a CYCLE.
    out = analyze(ctx, path, "yt-rc-cycle GdbWeakChildAddress")
    assert "ROOT" in out
    assert "CYCLE" not in out


@authors("babenko")
def test_virtual_inheritance(core):
    ctx, path = core
    # TRefCounted is a shared *virtual* base of the diamond, so the counter sits
    # at a runtime vbase offset. Resolution must find it both from the
    # most-derived pointer and from an interior pointer to the virtual base.
    out_derived = analyze(ctx, path, "yt-rc-obj GdbDiamondAddress")
    out_base = analyze(ctx, path, "yt-rc-obj GdbDiamondBaseAddress")
    for out in (out_derived, out_base):
        assert "TGdbDiamond" in out
        assert re.search(r"alive\s+yes", out)
    # Both pointers must resolve to the very same counter.
    m1 = re.search(r"counter\s+(0x[0-9a-f]+)", out_derived)
    m2 = re.search(r"counter\s+(0x[0-9a-f]+)", out_base)
    assert m1 and m2 and m1.group(1) == m2.group(1)
