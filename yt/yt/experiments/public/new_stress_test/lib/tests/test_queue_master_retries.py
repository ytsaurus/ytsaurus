from lib import test_queue_and_hunk_storage as stress

import pytest
import yt.wrapper as yt
from yt.wrapper import native_driver, retries
from yt.wrapper.default_config import get_default_config

import copy
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock


@pytest.fixture
def clients():
    config = get_default_config()
    config["enable_token"] = False
    config["apply_remote_patch_at_start"] = None
    original_config = copy.deepcopy(config)
    tablet = stress.create_tablet_client(config)
    master = stress.create_master_client(tablet.config)
    assert config == original_config
    return master, tablet


def test_master_config_does_not_extend_tablet_timeouts(clients):
    master, tablet = clients
    assert tablet.config["driver_config"] == {"enable_retries": True}
    assert tablet.config["transaction_timeout"] == 30000
    assert tablet.config["dynamic_table_retries"]["total_timeout"] == 180000

    driver = master.config["driver_config"]
    assert driver["rpc_timeout"] == 300000
    retry_config = driver["retrying_channel"]
    assert retry_config["retry_timeout"] == 300000
    # The default RPC backoff is three seconds; the attempt cap must allow five minutes.
    assert (retry_config["retry_attempts"] - 1) * 3000 >= 300000
    lifetime = master.config["transaction_timeout"]
    assert lifetime - lifetime / 3 > 300000


@pytest.fixture
def failing_rpc(monkeypatch):
    state = SimpleNamespace(seconds=0, recover_at=270, error_code=3, requests=[])

    class Clock:
        @staticmethod
        def now():
            return datetime(2026, 1, 1) + timedelta(seconds=state.seconds)

    def sleep(seconds):
        state.seconds += seconds

    def execute(request):
        state.requests.append(copy.deepcopy(request.parameters))
        ok = state.seconds >= state.recover_at
        return SimpleNamespace(
            wait=lambda: None,
            is_ok=lambda: ok,
            error=lambda: {"code": state.error_code, "message": "Injected failure"},
        )

    class Request:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.id = "test-request"

    driver = Mock()
    driver.execute.side_effect = execute
    driver.get_command_descriptors.return_value = {
        command: SimpleNamespace(
            is_heavy=lambda: False,
            is_volatile=command != "get",
            input_type=lambda: b"Null",
            output_type=lambda: b"Null",
        ) for command in ("get", "set")
    }
    monkeypatch.setattr(native_driver, "get_driver_instance", lambda client: driver)
    monkeypatch.setattr(native_driver, "driver_bindings", SimpleNamespace(Request=Request))
    monkeypatch.setattr(retries, "datetime", Clock)
    monkeypatch.setattr(retries.time, "sleep", sleep)
    return state


@pytest.mark.parametrize("command, error_code", [
    ("get", 3),   # Transport failure on a read.
    ("set", 218),  # MasterDisconnected on a mutation.
    ("set", 712),  # MasterCommunicationFailed on a mutation.
])
def test_master_requests_recover_after_long_outage(clients, failing_rpc, command, error_code):
    master, _ = clients
    failing_rpc.error_code = error_code
    native_driver.make_request(command, {}, client=master)
    assert 270 <= failing_rpc.seconds <= 300
    assert len(failing_rpc.requests) > 10
    # Reissue the same mutation, even if the master applied it before the response was lost.
    if command != "get":
        assert len({params["mutation_id"] for params in failing_rpc.requests}) == 1
        assert not failing_rpc.requests[0]["retry"]
        assert all(params["retry"] for params in failing_rpc.requests[1:])


def test_master_retries_are_bounded(clients, failing_rpc):
    master, _ = clients
    failing_rpc.recover_at = 301
    with pytest.raises(yt.YtError, match="Injected failure"):
        native_driver.make_request("get", {}, client=master)
    # The next exponential backoff may exceed the remaining budget.
    assert 120 < failing_rpc.seconds <= 300


def test_master_requests_do_not_retry_permanent_errors(clients, failing_rpc):
    master, _ = clients
    failing_rpc.error_code = 42
    with pytest.raises(yt.YtError, match="Injected failure"):
        native_driver.make_request("set", {}, client=master)
    assert len(failing_rpc.requests) == 1


def test_tablet_client_does_not_get_master_retry_budget(clients, failing_rpc):
    _, tablet = clients
    with pytest.raises(yt.YtError, match="Injected failure"):
        native_driver.make_request("get", {}, client=tablet)
    assert failing_rpc.seconds <= 120


def test_client_routing_and_polling_deadlines(clients, monkeypatch):
    master, tablet = clients
    master.get = Mock(return_value=[{"state": "mounted"}])
    tablet.select_rows = Mock(return_value=[])
    tablet.get_tablet_infos = Mock(return_value={"tablets": [{"total_row_count": 0}]})
    tablet.trim_rows = Mock()
    monkeypatch.setattr(stress, "master_client", master)
    monkeypatch.setattr(stress, "tablet_client", tablet)
    # Fail on implicit requests; execute predicates to check their client selection too.
    monkeypatch.setattr(stress, "yt", None)
    wait = Mock(side_effect=lambda predicate, **kwargs: predicate())
    monkeypatch.setattr(stress, "wait", wait)

    queue = stress.Queue("//test", "queue", 1)
    queue._select_data_rows(0, 0, 1)
    tablet.select_rows.assert_called_once()
    queue.trim(0, 0)
    tablet.trim_rows.assert_called_once_with(queue.path, 0, 0)

    stress.wait_for_tablet_state(queue.path, [0], "mounted")
    master.get.assert_called_once_with(f"{queue.path}/@tablets")
    assert wait.call_args.kwargs["timeout"] == 540
    queue._wait_for_written_rows(queue.path, [0])
    tablet.get_tablet_infos.assert_called_once_with(queue.path, [0])
    assert wait.call_args.kwargs["timeout"] == 240
