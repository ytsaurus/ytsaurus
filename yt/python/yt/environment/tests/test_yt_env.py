import pytest

from yt.common import YtError, YtResponseError
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
