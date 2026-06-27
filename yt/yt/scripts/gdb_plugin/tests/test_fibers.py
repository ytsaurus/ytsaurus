from yt_commands import authors

from gdb_test_lib import get_core, analyze


@authors("babenko")
def test_fiber_list():
    ctx, path = get_core()
    # The fixture parks one fiber in WaitFor; the roster must list it with the
    # user-code leaf frame (the fixture lambda), not the scheduler plumbing.
    out = analyze(ctx, path, "yt-fiber-list")
    assert "parked fiber" in out
    assert "SetupGdbRefCountFixtures" in out


@authors("babenko")
def test_fiber_bt():
    ctx, path = get_core()
    # The full backtrace unwinds to the recognizable WaitUntilSet / FiberTrampoline
    # chain (rbp-walk in debug, stack scan in release).
    out = analyze(ctx, path, "yt-fiber-bt 0")
    assert "WaitUntilSet" in out
    assert "FiberTrampoline" in out


@authors("babenko")
def test_fiber_select_on_core():
    ctx, path = get_core()
    # Switching the register context needs a live inferior; on a coredump the
    # command must fail with a clear message rather than a raw gdb error.
    out = analyze(ctx, path, "yt-fiber-select 0")
    assert "coredump" in out
