from yt_odin.odinserver.juggler_client import JugglerClient, JugglerError

import mock
import pytest

import contextlib
import json
import socket
import time


@contextlib.contextmanager
def set_juggler_params(timeout, retry_count):
    old_timeout = JugglerClient.HTTP_REQUEST_TIMEOUT
    old_retry_count = JugglerClient.HTTP_RETRY_COUNT
    JugglerClient.HTTP_REQUEST_TIMEOUT = timeout
    JugglerClient.HTTP_RETRY_COUNT = retry_count
    yield
    JugglerClient.HTTP_REQUEST_TIMEOUT = old_timeout
    JugglerClient.HTTP_RETRY_COUNT = old_retry_count


@mock.patch("yt_odin.odinserver.juggler_client.requests.post", autospec=True)
def test_juggler_client(post_mock):
    response_mock = mock.MagicMock()
    response_mock.content = json.dumps(dict(events=[
        dict(code=200, message="OK1"),
        dict(code=200, message="OK2")
    ]))
    post_mock.return_value = response_mock

    events = ["yohoho", "ololo"]
    client = JugglerClient(host="foo")
    client.push_events(events)

    assert post_mock.call_count == 1
    assert post_mock.call_args[1]["data"] == json.dumps(events)


@mock.patch("yt_odin.odinserver.juggler_client.requests.post", autospec=True)
def test_juggler_client_retries(post_mock):
    response_mock = mock.MagicMock()
    response_mock.content = json.dumps(dict(events=[
        dict(code=200, message="OK")
    ]))
    post_mock.side_effect = [socket.error] * 4 + [response_mock]

    start_time = time.time()
    events = ["So hard to post"]
    with set_juggler_params(timeout=1000, retry_count=5):
        client = JugglerClient(host="bar")
        client.push_events(events)
    assert time.time() - start_time >= 4.0

    assert post_mock.call_count == 5
    assert post_mock.call_args[1]["data"] == json.dumps(events)


@mock.patch("yt_odin.odinserver.juggler_client.requests.post", autospec=True)
def test_juggler_client_partial_fail(post_mock):
    response_mock = mock.MagicMock()
    response_mock.content = json.dumps(dict(events=[
        dict(code=200, message="OK1"),
        dict(code=400, message="FAIL"),
        dict(code=200, message="OK2"),
    ]))
    post_mock.return_value = response_mock

    events = ["first", "second", "third"]
    client = JugglerClient(host="bar")
    client.push_events(events)

    assert post_mock.call_count == 1
    assert post_mock.call_args[1]["data"] == json.dumps(events)


@mock.patch("yt_odin.odinserver.juggler_client.requests.post", autospec=True)
def test_juggler_client_total_fail(post_mock):
    response_mock = mock.MagicMock()
    response_mock.content = json.dumps(dict(events=[
        dict(code=400, message="FAIL1"),
        dict(code=400, message="FAIL2"),
    ]))
    post_mock.return_value = response_mock

    events = ["first", "second"]
    client = JugglerClient(host="bar")
    with pytest.raises(JugglerError):
        client.push_events(events)
