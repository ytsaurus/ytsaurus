import re

from yt_commands import authors

from gdb_test_lib import get_core, analyze, assert_contains


def _fixture_fiber_index(ctx, path):
    """Enumeration index of the fixture's parked WaitFor fiber, discovered by its
    user frame in the roster.

    The index is just an arbitrary position in the fiber registry -- other fibers
    (the main fiber, idle pooled ones) may precede it and it isn't deterministic,
    so the tests must never hard-code it. We find the fiber whose roster row shows
    the fixture's own frame (SetupGdbRefCountFixtures)."""
    out = analyze(ctx, path, "yt-fiber-list")
    for line in out.splitlines():
        if "SetupGdbRefCountFixtures" in line:
            m = re.search(r"#(\d+)", line)
            if m:
                return int(m.group(1))
    raise AssertionError("fixture's parked fiber not found in roster:\n" + out)


@authors("babenko")
def test_fiber_list():
    ctx, path = get_core()
    # The fixture parks one fiber in WaitFor; the roster must list it with the
    # user-code leaf frame (the fixture lambda), not the scheduler plumbing.
    out = analyze(ctx, path, "yt-fiber-list")
    assert_contains(out, "parked fiber", "SetupGdbRefCountFixtures")


@authors("babenko")
def test_fiber_bt():
    ctx, path = get_core()
    # The full backtrace unwinds to the recognizable WaitUntilSet / FiberTrampoline
    # chain (rbp-walk in debug, stack scan in release).
    index = _fixture_fiber_index(ctx, path)
    out = analyze(ctx, path, "yt-fiber-bt %d" % index)
    assert_contains(out, "WaitUntilSet", "FiberTrampoline")


@authors("babenko")
def test_fiber_locals_on_core():
    ctx, path = get_core()
    # The parked fiber's lambda keeps a local `held` (TIntrusivePtr) alive on its
    # stack. Inspecting locals must work on a coredump -- the CFI seed reads them
    # without modifying registers. Find the lambda's frame in the backtrace, then
    # dump its locals and confirm `held` shows up.
    index = _fixture_fiber_index(ctx, path)
    bt = analyze(ctx, path, "yt-fiber-bt %d" % index)
    frame = None
    for line in bt.splitlines():
        if "SetupGdbRefCountFixtures" in line:
            m = re.search(r"#(\d+)", line)
            if m:
                frame = int(m.group(1))
                break
    assert frame is not None, bt
    out = analyze(ctx, path, "yt-fiber-locals %d %d" % (index, frame))
    assert_contains(out, "locals:", "held")


@authors("babenko")
def test_fiber_select_on_core():
    ctx, path = get_core()
    # Switching the register context needs a live inferior; on a coredump the
    # command must fail with a clear message rather than a raw gdb error. The
    # raised GdbError makes gdb exit non-zero, so capture it with check=False.
    index = _fixture_fiber_index(ctx, path)
    out = analyze(ctx, path, "yt-fiber-select %d" % index, check=False)
    assert_contains(out, "coredump")
