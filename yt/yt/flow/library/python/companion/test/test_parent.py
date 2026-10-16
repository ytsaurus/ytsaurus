"""Tests for the companion parent supervisor."""

import os
import signal
import time

import pytest

from yt.yt.flow.library.python.companion.parent import (
    DEFAULT_STOP_TIMEOUT_SECONDS,
    DEFAULT_SUPERVISE_INTERVAL_SECONDS,
    CrashLoopError,
    CompanionProcessSupervisor,
    _ForkedChild,
    _drain_child,
    _make_stop_handler,
)
from yt.yt.flow.library.python.companion.server import DEFAULT_DRAIN_TIMEOUT_SECONDS


class FakeChild:
    def __init__(self, idx, exits_on_stop=True):
        self.idx = idx
        self.alive = True
        self.stopped = False
        self.killed = False
        self._exits_on_stop = exits_on_stop

    def is_alive(self):
        return self.alive

    def stop(self):
        self.stopped = True
        if self._exits_on_stop:
            self.alive = False

    def kill(self):
        self.killed = True
        self.alive = False


class FakeClock:
    """Manually advanced monotonic clock for deterministic backoff tests."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_start_spawns_n_children():
    spawned_indices = []

    def spawn(idx):
        spawned_indices.append(idx)
        return FakeChild(idx)

    supervisor = CompanionProcessSupervisor(n=3, spawn_child=spawn)
    supervisor.start()

    assert spawned_indices == [0, 1, 2]
    assert len(supervisor.children) == 3


def test_reap_and_restart_replaces_dead_child():
    spawn_call_count = {"n": 0}

    def spawn(idx):
        spawn_call_count["n"] += 1
        return FakeChild(idx)

    supervisor = CompanionProcessSupervisor(n=2, spawn_child=spawn)
    supervisor.start()
    assert spawn_call_count["n"] == 2

    # Mark child 0 dead.
    supervisor.children[0].alive = False
    supervisor.reap_and_restart()

    assert spawn_call_count["n"] == 3
    assert all(c.is_alive() for c in supervisor.children)


def test_restart_is_delayed_by_backoff():
    """A dead child is not re-forked until its per-slot backoff window elapses."""
    spawn_count = {"n": 0}

    def spawn(idx):
        spawn_count["n"] += 1
        return FakeChild(idx)

    clock = FakeClock()
    supervisor = CompanionProcessSupervisor(n=1, spawn_child=spawn, backoff_base=1.0, clock=clock)
    supervisor.start()
    assert spawn_count["n"] == 1

    # Child dies; first restart happens immediately and arms a 1s backoff.
    supervisor.children[0].alive = False
    supervisor.reap_and_restart()
    assert spawn_count["n"] == 2

    # Dies again before the backoff elapses -> no restart yet.
    supervisor.children[0].alive = False
    clock.advance(0.4)
    supervisor.reap_and_restart()
    assert spawn_count["n"] == 2

    # After the backoff window the slot is re-forked.
    clock.advance(1.0)
    supervisor.reap_and_restart()
    assert spawn_count["n"] == 3


def test_crash_loop_gives_up():
    """Too many restarts of one slot in the window raises CrashLoopError."""

    def spawn(idx):
        return FakeChild(idx)

    clock = FakeClock()
    supervisor = CompanionProcessSupervisor(
        n=1,
        spawn_child=spawn,
        backoff_base=0.0,
        crash_loop_threshold=3,
        crash_loop_window=60.0,
        clock=clock,
    )
    supervisor.start()

    with pytest.raises(CrashLoopError):
        for _ in range(3):
            supervisor.children[0].alive = False
            supervisor.reap_and_restart()


def test_crash_loop_gives_up_with_default_parameters():
    """The give-up ceiling must be reachable with the DEFAULT backoff/threshold/window
    constants: a child that dies instantly after every restart, supervised at the
    production poll cadence, eventually raises CrashLoopError (the backoff gaps between
    threshold restarts must fit inside the window)."""

    def spawn(idx):
        return FakeChild(idx)

    clock = FakeClock()
    supervisor = CompanionProcessSupervisor(n=1, spawn_child=spawn, clock=clock)
    supervisor.start()

    with pytest.raises(CrashLoopError):
        # 200 polls of 5s = 1000s of simulated time — far beyond the crash-loop window,
        # so a supervisor that never trips fails the test instead of looping forever.
        for _ in range(200):
            supervisor.children[0].alive = False
            supervisor.reap_and_restart()
            clock.advance(DEFAULT_SUPERVISE_INTERVAL_SECONDS)


def test_backoff_and_crash_loop_are_per_slot():
    """Restart bookkeeping is isolated per slot: one crash-looping slot neither delays
    a healthy slot's restart nor contributes to its crash-loop ceiling."""
    spawned = []

    def spawn(idx):
        spawned.append(idx)
        return FakeChild(idx)

    clock = FakeClock()
    supervisor = CompanionProcessSupervisor(
        n=2,
        spawn_child=spawn,
        backoff_base=1.0,
        crash_loop_threshold=3,
        crash_loop_window=60.0,
        clock=clock,
    )
    supervisor.start()
    assert spawned == [0, 1]

    # Two slot-1 crashes (one below the ceiling) arm slot 1's backoff.
    for _ in range(2):
        supervisor.children[1].alive = False
        clock.advance(5.0)
        supervisor.reap_and_restart()
    assert spawned == [0, 1, 1, 1]

    # Slot 0's first crash restarts immediately, unaffected by slot 1's backoff.
    supervisor.children[0].alive = False
    supervisor.reap_and_restart()
    assert spawned == [0, 1, 1, 1, 0]

    # Slot 1's third crash alone trips its ceiling — slot 0's crash did not count.
    supervisor.children[1].alive = False
    clock.advance(5.0)
    with pytest.raises(CrashLoopError):
        supervisor.reap_and_restart()


def test_crash_loop_window_resets():
    """Crashes spread beyond the window do not trip the give-up ceiling."""

    def spawn(idx):
        return FakeChild(idx)

    clock = FakeClock()
    supervisor = CompanionProcessSupervisor(
        n=1,
        spawn_child=spawn,
        backoff_base=0.0,
        crash_loop_threshold=3,
        crash_loop_window=10.0,
        clock=clock,
    )
    supervisor.start()

    # Five crashes, each well outside the 10s window — never trips the ceiling.
    for _ in range(5):
        supervisor.children[0].alive = False
        supervisor.reap_and_restart()
        clock.advance(20.0)


def test_stop_propagates_to_children():
    def spawn(idx):
        return FakeChild(idx)

    supervisor = CompanionProcessSupervisor(n=2, spawn_child=spawn)
    supervisor.start()
    supervisor.stop()

    assert all(c.stopped for c in supervisor.children)
    assert not any(c.killed for c in supervisor.children)
    assert not any(c.is_alive() for c in supervisor.children)


def test_stop_kills_children_that_ignore_sigterm():
    """A child left running would keep holding the companion port."""

    def spawn(idx):
        return FakeChild(idx, exits_on_stop=False)

    supervisor = CompanionProcessSupervisor(n=2, spawn_child=spawn)
    supervisor.start()
    supervisor.stop(timeout=0.05)

    assert all(c.stopped for c in supervisor.children)
    assert all(c.killed for c in supervisor.children)
    assert not any(c.is_alive() for c in supervisor.children)


def test_requires_positive_n():
    with pytest.raises(ValueError):
        CompanionProcessSupervisor(n=0, spawn_child=lambda idx: FakeChild(idx))


def test_stop_reaps_a_real_child_that_ignores_sigterm():
    """A real child is needed here: the fakes exit synchronously and would hide
    the kill/reap race."""
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the forked child
        try:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(1)
        finally:
            os._exit(0)

    supervisor = CompanionProcessSupervisor(n=1, spawn_child=lambda idx: _ForkedChild(pid))
    supervisor.start()
    supervisor.stop(timeout=0.5)

    assert not supervisor.children[0].is_alive()
    # The child must be reaped, not merely signalled: waitpid on a reaped pid
    # raises instead of blocking.
    with pytest.raises(ChildProcessError):
        os.waitpid(pid, 0)


class _DrainRecorder:
    """Records the drain sequencing of a stopping child."""

    def __init__(self, fail_shutdown=None):
        self.calls = []
        self._fail_shutdown = fail_shutdown

    def stop(self, grace):
        self.calls.append(("stop", grace))

        class _Event:
            def wait(inner):
                self.calls.append(("wait",))

        return _Event()

    def shutdown(self):
        self.calls.append(("shutdown",))
        if self._fail_shutdown is not None:
            raise self._fail_shutdown


def test_drain_child_releases_resources_after_the_server_drains():
    recorder = _DrainRecorder()

    _drain_child(recorder, recorder)

    # The resources go only after in-flight RPCs finished: batches still
    # holding leases must complete against usable instances. The grace is the
    # drain slice, not the whole stop budget: the rest funds the unload hooks,
    # which would otherwise be killed instead of run.
    assert recorder.calls == [("stop", DEFAULT_DRAIN_TIMEOUT_SECONDS), ("wait",), ("shutdown",)]
    assert DEFAULT_DRAIN_TIMEOUT_SECONDS < DEFAULT_STOP_TIMEOUT_SECONDS


def test_drain_child_survives_a_failing_shutdown():
    recorder = _DrainRecorder(fail_shutdown=RuntimeError("hook failed"))

    # A failing unload hook must not turn a graceful child exit into a crash.
    _drain_child(recorder, recorder)

    assert recorder.calls == [("stop", DEFAULT_DRAIN_TIMEOUT_SECONDS), ("wait",), ("shutdown",)]


def test_drain_child_survives_a_shutdown_escaping_as_base_exception():
    recorder = _DrainRecorder(fail_shutdown=BaseException("hook escaped"))  # noqa: TRY002

    # ResourceStore.shutdown re-raises BaseException by design; escaping here
    # would skip the child's exit and report a graceful stop as a crash.
    _drain_child(recorder, recorder)

    assert recorder.calls == [("stop", DEFAULT_DRAIN_TIMEOUT_SECONDS), ("wait",), ("shutdown",)]


def test_second_stop_signal_does_not_re_enter_the_drain():
    recorder = _DrainRecorder()
    exits = []
    handler = _make_stop_handler(recorder, recorder, exit_process=exits.append)

    handler(signal.SIGTERM, None)
    handler(signal.SIGTERM, None)

    # The second signal must not restart the drain: re-entering would exit the process
    # while the first invocation still had unload hooks to run.
    assert recorder.calls == [("stop", DEFAULT_DRAIN_TIMEOUT_SECONDS), ("wait",), ("shutdown",)]
    assert exits == [0]
