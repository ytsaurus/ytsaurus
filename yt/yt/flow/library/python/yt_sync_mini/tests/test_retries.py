"""Tests for the bounded resolve retries shared by the creation helpers.

An object created moments earlier is not yet resolvable on every master cell,
so an operation touching it fails with ``No such object <id>`` until the cells
catch up. No YT access.
"""

import pytest

from yt.wrapper.errors import YtResponseError

from yt.yt.flow.library.python.yt_sync_mini import yt_sync_mini

# ``NYTree::EErrorCode::ResolveError``, what the master returns for a dead object id.
RESOLVE_ERROR_CODE = 500

# Any non-resolve code; ``NTabletClient::EErrorCode::NoSuchTablet``.
OTHER_ERROR_CODE = 1701


def _response_error(code):
    return YtResponseError({"code": code, "message": "No such object 1-2-3-4"})


class _Action:
    """Raises the given errors on successive calls, then returns ``"done"``."""

    def __init__(self, errors):
        self._errors = list(errors)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self._errors:
            raise self._errors.pop(0)
        return "done"


class _Client:
    """Client stub whose every method delegates to one shared action."""

    def __init__(self, action):
        self._action = action

    def create(self, *args, **kwargs):
        return self._action()

    def mount_table(self, *args, **kwargs):
        return self._action()

    def register_queue_consumer(self, *args, **kwargs):
        return self._action()


@pytest.fixture
def sleeps(monkeypatch):
    """Record the backoff instead of sleeping, without touching the shared
    ``time`` module: only the name ``time`` inside yt_sync_mini is rebound."""
    recorded = []

    class _Time:
        @staticmethod
        def sleep(seconds):
            recorded.append(seconds)

    monkeypatch.setattr(yt_sync_mini, "time", _Time)
    return recorded


def test_resolve_error_is_retried(sleeps):
    action = _Action([_response_error(RESOLVE_ERROR_CODE)] * 2)

    assert yt_sync_mini._retry_on_resolve_error(action, "create table //tmp/t") == "done"

    assert action.calls == 3
    assert sleeps == [yt_sync_mini.RETRY_INTERVAL] * 2


def test_retries_give_up_at_the_bound(sleeps):
    attempts = yt_sync_mini.RETRY_ATTEMPTS
    action = _Action([_response_error(RESOLVE_ERROR_CODE)] * attempts)

    with pytest.raises(YtResponseError):
        yt_sync_mini._retry_on_resolve_error(action, "create table //tmp/t")

    assert action.calls == attempts
    assert sleeps == [yt_sync_mini.RETRY_INTERVAL] * (attempts - 1)


def test_other_error_is_not_retried(sleeps):
    action = _Action([_response_error(OTHER_ERROR_CODE)])

    with pytest.raises(YtResponseError):
        yt_sync_mini._retry_on_resolve_error(action, "create table //tmp/t")

    assert action.calls == 1
    assert sleeps == []


def test_create_table_retries(sleeps):
    """``create_table`` creates and mounts, so it retries twice over."""
    action = _Action([_response_error(RESOLVE_ERROR_CODE)] * 2)

    yt_sync_mini.create_table(_Client(action), "//tmp/queue", yt_sync_mini.CONSUMER_SCHEMA)

    assert action.calls == 4
    assert sleeps == [yt_sync_mini.RETRY_INTERVAL] * 2


def test_register_consumer_retries(sleeps):
    action = _Action([_response_error(RESOLVE_ERROR_CODE)])

    yt_sync_mini.register_consumer(_Client(action), "//tmp/queue", "//tmp/consumer", vital=True)

    assert action.calls == 2
    assert sleeps == [yt_sync_mini.RETRY_INTERVAL]
