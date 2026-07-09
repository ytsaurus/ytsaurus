"""Tests for the companion parent supervisor."""

import pytest

from yt.yt.flow.library.python.companion.parent import (
    DEFAULT_SUPERVISE_INTERVAL_SECONDS,
    CrashLoopError,
    CompanionProcessSupervisor,
)


class FakeChild:
    def __init__(self, idx):
        self.idx = idx
        self.alive = True
        self.stopped = False

    def is_alive(self):
        return self.alive

    def stop(self):
        self.stopped = True


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


def test_requires_positive_n():
    with pytest.raises(ValueError):
        CompanionProcessSupervisor(n=0, spawn_child=lambda idx: FakeChild(idx))
