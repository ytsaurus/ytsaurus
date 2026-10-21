import os
import signal
import threading
import time
from datetime import datetime, timedelta
from unittest import mock

import pytest

from yt.wrapper.client import YtClient
import yt.wrapper.transaction as transaction_module
from yt.wrapper.transaction import PingTransaction
from yt.testlib import authors


def _wait_for_child(pid, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return status, False
        time.sleep(0.01)

    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    _, status = os.waitpid(pid, 0)
    return status, True


@pytest.fixture
def pinger_factory(monkeypatch):
    ping = mock.Mock()
    monkeypatch.setattr(transaction_module, "ping_transaction", ping)
    pingers = []

    def make_pinger(config_patch=None, ping_period=1000, ping_timeout=100, interrupt_on_failed=False):
        client_config = {
            "ping_failed_mode": "pass",
            "transaction_sleep_period": 50,
        }
        client_config.update(config_patch or {})
        client = YtClient(config=client_config)
        pinger = PingTransaction(
            "test-transaction",
            ping_period,
            ping_timeout,
            interrupt_on_failed=interrupt_on_failed,
            client=client,
        )
        pingers.append(pinger)
        return pinger

    yield make_pinger, ping

    for pinger in pingers:
        pinger.stop()
        if pinger.ident is not None:
            pinger.join(5)
            assert not pinger.is_alive()


@authors("denvr")  # @damirzain
def test_stop_wakes_long_idle_wait(pinger_factory):
    make_pinger, ping = pinger_factory
    rpc_entered = threading.Event()
    ping.side_effect = lambda *_args, **_kwargs: rpc_entered.set()
    pinger = make_pinger(
        {"transaction_sleep_period": 3000},
        ping_period=15000,
        ping_timeout=100,
    )
    pinger.start()
    stopper = threading.Thread(target=pinger.stop, daemon=True)
    try:
        assert rpc_entered.wait(1.0)
        stopper.start()
        stopper.join(timeout=1.0)
        assert not stopper.is_alive()
        assert not pinger.is_alive()
    finally:
        if stopper.ident is not None:
            stopper.join(timeout=4.0)


@authors("denvr")  # @damirzain
def test_ping_cadence_and_rpc_parameters_are_unchanged(pinger_factory, monkeypatch):
    make_pinger, ping = pinger_factory
    base_time = datetime(2020, 1, 1)
    clock = [base_time]
    calls = []

    def advance(seconds):
        clock[0] += timedelta(seconds=seconds)
        if len(calls) >= 2:
            pinger.is_running = False

    def record_ping(transaction, timeout, client):
        calls.append((transaction, timeout, client, (clock[0] - base_time).total_seconds()))

    monkeypatch.setattr(transaction_module, "datetime", mock.Mock(now=lambda: clock[0]))
    pinger = make_pinger(ping_period=300, ping_timeout=137)
    pinger._stop_event.wait = mock.Mock(side_effect=advance)
    ping.side_effect = record_ping

    pinger.run()

    assert ping.call_args_list == [
        mock.call("test-transaction", timeout=137, client=pinger._client),
        mock.call("test-transaction", timeout=137, client=pinger._client),
    ]
    assert calls[0][3] == pytest.approx(0.0)
    assert calls[1][3] == pytest.approx(0.3)


@authors("denvr")  # @damirzain
def test_stop_does_not_interrupt_in_flight_rpc(pinger_factory):
    make_pinger, ping = pinger_factory
    rpc_entered = threading.Event()
    release_rpc = threading.Event()

    def blocked_ping(*_args, **_kwargs):
        rpc_entered.set()
        release_rpc.wait(2.0)

    ping.side_effect = blocked_ping
    pinger = make_pinger(ping_period=3000, ping_timeout=271)
    pinger.start()
    stopper = threading.Thread(target=pinger.stop, daemon=True)
    try:
        assert rpc_entered.wait(1.0)
        stopper.start()
        stopper.join(timeout=0.05)
        assert stopper.is_alive(), "stop must wait for the in-flight RPC"
        release_rpc.set()
        stopper.join(timeout=1.0)
        assert not stopper.is_alive()
        ping.assert_called_once_with("test-transaction", timeout=271, client=pinger._client)
        assert not pinger.is_alive()
    finally:
        release_rpc.set()
        if stopper.ident is not None:
            stopper.join(timeout=2.0)


@authors("denvr")  # @damirzain
def test_never_started_and_repeated_stop_are_safe(pinger_factory):
    make_pinger, _ = pinger_factory
    pinger = make_pinger()

    pinger.stop()
    pinger.stop()

    assert not pinger.is_running
    assert not pinger.is_alive()
    assert not pinger._stop_event.is_set()


@authors("denvr")  # @damirzain
def test_stop_uses_independent_join_timeouts(pinger_factory):
    make_pinger, _ = pinger_factory
    first = make_pinger(ping_period=1000, ping_timeout=271)
    second = make_pinger(ping_period=400, ping_timeout=83)

    first.is_alive = mock.Mock(side_effect=[True, False])
    first.join = mock.Mock()
    second.is_alive = mock.Mock(side_effect=[True, False])
    second.join = mock.Mock()

    first.stop()
    assert not first.is_running
    assert second.is_running
    second.join.assert_not_called()
    first.join.assert_called_once_with(pytest.approx(0.371))

    second.stop()
    assert not second.is_running
    second.join.assert_called_once_with(pytest.approx(0.183))


@authors("denvr")  # @damirzain
def test_callback_self_stop_keeps_existing_join_error(pinger_factory):
    make_pinger, ping = pinger_factory
    callback_errors = []

    def callback():
        try:
            pinger.stop()
        except RuntimeError as error:
            callback_errors.append(error)

    ping.side_effect = RuntimeError("ping failure")
    pinger = make_pinger(
        {"ping_failed_mode": "call_function", "ping_failed_function": callback},
        interrupt_on_failed=True,
    )
    pinger.start()
    pinger.join(timeout=1.0)

    assert not pinger.is_alive()
    assert pinger.failed
    assert not pinger.is_running
    assert len(callback_errors) == 1
    assert "current thread" in str(callback_errors[0])


@authors("denvr")  # @damirzain
def test_partial_initialization_preserves_original_exception(pinger_factory):
    make_pinger, _ = pinger_factory

    with pytest.raises(transaction_module.YtError, match="Incorrect ping failed mode"):
        make_pinger({"ping_failed_mode": "invalid-mode"})


@authors("denvr")  # @damirzain
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_fork_child_stop_skips_inherited_event_condition_lock(pinger_factory):
    make_pinger, ping = pinger_factory
    first_ping = threading.Event()
    ping.side_effect = lambda *_args, **_kwargs: first_ping.set()
    pinger = make_pinger(ping_period=1000, ping_timeout=100)
    pinger.start()
    assert first_ping.wait(1.0)
    child_pid = None
    try:
        with pinger._stop_event._cond:
            child_pid = os.fork()
            if child_pid == 0:
                try:
                    pinger.stop()
                    os._exit(0)
                except BaseException:
                    os._exit(3)

        status, timed_out = _wait_for_child(child_pid, timeout=2.0)
        assert not timed_out, "fork child stop hung on the inherited Event Condition"
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    finally:
        if child_pid is not None:
            try:
                _wait_for_child(child_pid, timeout=0)
            except ChildProcessError:
                pass
