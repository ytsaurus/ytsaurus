"""Tests for the bounded resolve retries shared by the creation helpers.

An object created moments earlier is not yet resolvable on every master cell,
so an operation touching it fails with ``No such object <id>`` until the cells
catch up. No YT access.

``register_consumer`` writes only what the existing registrations lack, so its
tests live here too.
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


def _registration(vital):
    return {"queue_path": "//tmp/queue", "consumer_path": "//tmp/consumer", "vital": vital}


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

    def list_queue_consumer_registrations(self, queue_path, consumer_path):
        return []

    def register_queue_consumer(self, *args, **kwargs):
        return self._action()


class _RegistrationClient:
    """Client stub backed by a registration list, as the registration table is."""

    def __init__(self, registrations=(), errors=()):
        self.registrations = [dict(registration) for registration in registrations]
        self._errors = list(errors)
        self.register_calls = 0

    def list_queue_consumer_registrations(self, queue_path, consumer_path):
        return [
            registration
            for registration in self.registrations
            if registration["queue_path"] == queue_path and registration["consumer_path"] == consumer_path
        ]

    def register_queue_consumer(self, queue_path, consumer_path, vital):
        self.register_calls += 1
        if self._errors:
            raise self._errors.pop(0)
        self.registrations = [
            registration
            for registration in self.registrations
            if (registration["queue_path"], registration["consumer_path"]) != (queue_path, consumer_path)
        ]
        self.registrations.append({"queue_path": queue_path, "consumer_path": consumer_path, "vital": vital})


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


def test_missing_registration_is_created(sleeps):
    client = _RegistrationClient()

    yt_sync_mini.register_consumer(client, "//tmp/queue", "//tmp/consumer", vital=True)

    assert client.register_calls == 1
    assert client.registrations == [_registration(vital=True)]


def test_matching_registration_is_left_alone(sleeps):
    """A run against a live pipeline whose registration is in place writes nothing."""
    client = _RegistrationClient([_registration(vital=True)])

    yt_sync_mini.register_consumer(client, "//tmp/queue", "//tmp/consumer", vital=True)

    assert client.register_calls == 0


def test_registration_is_rewritten_when_vital_differs(sleeps):
    client = _RegistrationClient([_registration(vital=False)])

    yt_sync_mini.register_consumer(client, "//tmp/queue", "//tmp/consumer", vital=True)

    assert client.register_calls == 1
    assert client.registrations == [_registration(vital=True)]


def test_failed_registration_keeps_the_existing_one(sleeps):
    """Exhausting the retries must not leave a live pipeline unregistered; the
    stub has no ``unregister_queue_consumer`` at all, so any attempt to drop a
    registration fails the test."""
    attempts = yt_sync_mini.RETRY_ATTEMPTS
    client = _RegistrationClient(
        [_registration(vital=False)],
        errors=[_response_error(RESOLVE_ERROR_CODE)] * attempts,
    )

    with pytest.raises(YtResponseError):
        yt_sync_mini.register_consumer(client, "//tmp/queue", "//tmp/consumer", vital=True)

    assert client.register_calls == attempts
    assert client.registrations == [_registration(vital=False)]
