from typing import Any


class MockResult:
    def __init__(self, result: Any | None = None, error: Any | None = None, raw: bool = False):
        assert result is not None or error is not None
        if result is not None:
            assert error is None
        if error is not None:
            assert result is None
        self._result = result
        self._error = error
        self.raw = raw

    def is_ok(self):
        return self._error is None

    def get_result(self) -> Any:
        assert self._result is not None
        return self._result

    def get_error(self) -> Any:
        assert self._error is not None
        return self._error


class MockOperation:
    class MockState:
        def __init__(self, name: str):
            self.name = name

        def __repr__(self):
            return self.name

        def __str__(self):
            return self.name

        def is_finished(self):
            return self.name in ("aborted", "completed", "failed")

        def is_unsuccessfully_finished(self):
            return self.name in ("aborted", "failed")

    def __init__(self, state: str):
        self._state = self.MockState(state)

    def get_state(self) -> MockState:
        return self._state
