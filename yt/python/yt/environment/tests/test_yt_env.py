import pytest

from yt.common import YtError, YtResponseError, date_string_to_datetime
from yt.environment.yt_env import YTInstance

# Large on purpose: a stall before the first loop check leaves the condition unpolled.
SUCCESS_WAIT_TIME = 60
TIMEOUT_WAIT_TIME = 2


class _StderrCollector:
    def __init__(self):
        self.processed = []

    def _process_stderrs(self, name, number=None):
        self.processed.append(name)


class _ScriptedCondition:
    def __init__(self, results):
        assert results, "at least one result is required"
        self._remaining = list(results)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        # Last result repeats: the poll count is set by the wall clock, not by the script.
        return self._remaining.pop(0) if len(self._remaining) > 1 else self._remaining[0]


def run_wait_for(results, max_wait_time, stderr_collector=None):
    condition = _ScriptedCondition(results)
    YTInstance._wait_for(
        stderr_collector if stderr_collector is not None else _StderrCollector(),
        condition,
        "scheduler",
        max_wait_time=max_wait_time,
        sleep_quantum=0.001,
    )
    return condition


class TestWaitFor:
    def test_returns_when_condition_holds(self):
        assert run_wait_for([True], SUCCESS_WAIT_TIME).calls == 1

    def test_returns_when_condition_holds_after_failures(self):
        assert run_wait_for([(False, "not yet"), True], SUCCESS_WAIT_TIME).calls == 2

    def test_reports_message_of_failing_check(self):
        with pytest.raises(YtError) as exc_info:
            run_wait_for([(False, "0 schedulers registered in cypress, expected 1")], TIMEOUT_WAIT_TIME)
        assert [error.message for error in exc_info.value.inner_errors] == [
            "0 schedulers registered in cypress, expected 1"
        ]

    def test_reports_message_of_last_failing_check(self):
        with pytest.raises(YtError) as exc_info:
            run_wait_for(
                [(False, "No active scheduler found"), (False, "Nodes are not online at scheduler")],
                TIMEOUT_WAIT_TIME,
            )
        assert [error.message for error in exc_info.value.inner_errors] == ["Nodes are not online at scheduler"]

    def test_attaches_error_object_of_failing_check_as_is(self):
        error = YtResponseError({"message": "Orchid connection refused", "code": 105})
        with pytest.raises(YtError) as exc_info:
            run_wait_for([(False, error)], TIMEOUT_WAIT_TIME)
        assert exc_info.value.inner_errors == [error]

    @pytest.mark.parametrize("result", [False, None, (), (False,), (False, "reason", "extra"), (True, "reason")])
    def test_illegal_result_is_rejected(self, result):
        with pytest.raises(YtError, match="must return True"):
            run_wait_for([result], SUCCESS_WAIT_TIME)

    def test_illegal_result_is_rejected_on_the_poll_that_returns_it(self):
        condition = _ScriptedCondition([(False, "No active scheduler found"), False])
        with pytest.raises(YtError, match="must return True"):
            YTInstance._wait_for(
                _StderrCollector(),
                condition,
                "scheduler",
                max_wait_time=SUCCESS_WAIT_TIME,
                sleep_quantum=0.001,
            )
        assert condition.calls == 2

    def test_dumps_stderrs_on_timeout(self):
        collector = _StderrCollector()
        with pytest.raises(YtError):
            run_wait_for([(False, "not ready")], TIMEOUT_WAIT_TIME, stderr_collector=collector)
        assert collector.processed == ["scheduler"]


TX_ID = "1-2-3-4"
# Ordered on purpose: STALE < MULTI_START_TIME < CURRENT.
STALE = "2026-08-12T07:05:00.000000Z"
MULTI_START_TIME = date_string_to_datetime("2026-08-12T07:10:13.725000Z")
CURRENT = "2026-08-12T07:10:14.833744Z"


class _FakeClient:
    def __init__(self, tx_start_time):
        self._values = {
            "//sys/scheduler/lock/@locks/0/transaction_id": TX_ID,
            f"#{TX_ID}/@start_time": tx_start_time,
        }
        self.aborted = []

    def get(self, path):
        return self._values[path]

    def abort_transaction(self, tx_id):
        self.aborted.append(tx_id)


class _FakeYtConfig:
    def __init__(self, enable_multidaemon):
        self.enable_multidaemon = enable_multidaemon


class _LockRemover:
    """Drives YTInstance._remove_scheduler_lock with everything it touches faked out."""

    _scheduler_lock_transaction_is_stale = YTInstance._scheduler_lock_transaction_is_stale

    def __init__(self, enable_multidaemon, tx_start_time, multi_start_time=MULTI_START_TIME):
        self.yt_config = _FakeYtConfig(enable_multidaemon)
        self._multi_start_time = multi_start_time
        self.client = _FakeClient(tx_start_time)

    def _create_cluster_client(self):
        return self.client

    def run(self):
        YTInstance._remove_scheduler_lock(self)
        return self.client.aborted


class TestRemoveSchedulerLock:
    def test_multidaemon_aborts_a_transaction_from_a_previous_run(self):
        assert _LockRemover(True, STALE).run() == [TX_ID]

    def test_multidaemon_keeps_the_transaction_of_the_running_scheduler(self):
        assert _LockRemover(True, CURRENT).run() == []

    def test_without_multidaemon_the_lock_is_always_stale(self):
        assert _LockRemover(False, CURRENT).run() == [TX_ID]
        assert _LockRemover(False, STALE).run() == [TX_ID]

    def test_multidaemon_without_a_known_start_time_falls_back_to_aborting(self):
        assert _LockRemover(True, CURRENT, multi_start_time=None).run() == [TX_ID]

    def test_unparseable_start_time_falls_back_to_aborting(self):
        assert _LockRemover(True, "not a timestamp").run() == [TX_ID]
