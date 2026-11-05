
from unittest.mock import Mock
from .conftest import authors
from .helpers import set_config_option

from yt.wrapper import auth_commands
from yt.wrapper import cli_impl
from yt.wrapper.errors import YtError
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message

import yt.wrapper as yt

import pytest
import string
import random


@pytest.fixture
def disable_current_user_cache(monkeypatch):
    monkeypatch.setattr(auth_commands, "get_option", lambda name, client=None: None)
    monkeypatch.setattr(auth_commands, "set_option", lambda *args, **kwargs: None)


@authors("asklit")
def test_get_current_user_returns_none_on_request_error_by_default(monkeypatch, disable_current_user_cache):
    authentication_error = YtError("Authentication failed", code=900)
    make_formatted_request = Mock(side_effect=authentication_error)
    get_user_name = Mock()
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    assert auth_commands.get_current_user() is None
    get_user_name.assert_not_called()


@authors("asklit")
def test_get_current_user_propagates_request_error(monkeypatch, disable_current_user_cache):
    authentication_error = YtError("Authentication failed", code=900)
    make_formatted_request = Mock(side_effect=authentication_error)
    get_user_name = Mock()
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    with pytest.raises(YtError) as error:
        auth_commands._get_current_user(raise_error=True)

    assert error.value is authentication_error
    make_formatted_request.assert_called_once_with(
        "get_current_user",
        params={},
        format=None,
        client=None)
    get_user_name.assert_not_called()


@authors("asklit")
def test_get_current_user_uses_auth_fallback(monkeypatch, disable_current_user_cache):
    unsupported_error = YtError("Command get_current_user is not supported")
    make_formatted_request = Mock(side_effect=unsupported_error)
    get_user_name = Mock(return_value="root")
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    assert auth_commands.get_current_user() == {"user": "root"}
    make_formatted_request.assert_called_once_with(
        "get_current_user",
        params={},
        format=None,
        client=None)
    get_user_name.assert_called_once_with(client=None)


@authors("asklit")
def test_get_current_user_passes_client(monkeypatch, disable_current_user_cache):
    client = object()
    current_user = {"user": "root"}
    make_formatted_request = Mock(return_value=current_user)
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)

    assert auth_commands.get_current_user(client=client) == current_user
    make_formatted_request.assert_called_once_with(
        "get_current_user",
        params={},
        format=None,
        client=client)


@authors("asklit")
def test_get_current_user_preserves_auth_fallback_empty_user(monkeypatch, disable_current_user_cache):
    unsupported_error = YtError("Command get_current_user is not supported")
    make_formatted_request = Mock(side_effect=unsupported_error)
    get_user_name = Mock(return_value=None)
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    assert auth_commands.get_current_user() == {"user": None}
    get_user_name.assert_called_once_with(client=None)


@authors("asklit")
def test_get_current_user_returns_none_on_auth_fallback_error_by_default(monkeypatch, disable_current_user_cache):
    unsupported_error = YtError("Command get_current_user is not supported")
    authentication_error = YtError("Authentication failed")
    make_formatted_request = Mock(side_effect=unsupported_error)
    get_user_name = Mock(side_effect=authentication_error)
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    assert auth_commands.get_current_user() is None
    get_user_name.assert_called_once_with(client=None)


@authors("asklit")
def test_get_current_user_propagates_auth_fallback_error(monkeypatch, disable_current_user_cache):
    unsupported_error = YtError("Command get_current_user is not supported")
    authentication_error = YtError("Authentication failed")
    make_formatted_request = Mock(side_effect=unsupported_error)
    get_user_name = Mock(side_effect=authentication_error)
    monkeypatch.setattr(auth_commands, "make_formatted_request", make_formatted_request)
    monkeypatch.setattr(auth_commands, "get_user_name", get_user_name)

    with pytest.raises(YtError) as error:
        auth_commands._get_current_user(raise_error=True)

    assert error.value is authentication_error
    make_formatted_request.assert_called_once_with(
        "get_current_user",
        params={},
        format=None,
        client=None)
    get_user_name.assert_called_once_with(client=None)


@authors("asklit")
def test_whoami_returns_current_user(monkeypatch):
    get_current_user = Mock(return_value={"user": "root"})
    monkeypatch.setattr(cli_impl, "_get_current_user", get_current_user)

    assert cli_impl._whoami() == "root"
    get_current_user.assert_called_once_with(client=None, raise_error=True)


@authors("asklit")
@pytest.mark.parametrize("current_user", [None, {"user": None}])
def test_whoami_reports_error_when_current_user_is_unavailable(monkeypatch, current_user):
    get_current_user = Mock(return_value=current_user)
    monkeypatch.setattr(cli_impl, "_get_current_user", get_current_user)

    with pytest.raises(YtError, match="Failed to get current user"):
        cli_impl._whoami()

    get_current_user.assert_called_once_with(client=None, raise_error=True)


@pytest.mark.usefixtures("yt_env_with_authentication")
class TestAuthCommands(object):
    @authors("pavel-bash")
    @add_failed_operation_stderrs_to_error_message
    def test_password_strength_validation(self):
        password = "".join(random.choice(string.ascii_letters) for i in range(11))
        yt.set_user_password("admin", password)
        with set_config_option("enable_password_strength_validation", True):
            with pytest.raises(ValueError):
                yt.set_user_password("admin", password)

        password = "".join(random.choice(string.ascii_letters) for i in range(12))
        yt.set_user_password("admin", password)

        password = "".join(random.choice(string.ascii_letters) for i in range(129))
        yt.set_user_password("admin", password)
        with set_config_option("enable_password_strength_validation", True):
            with pytest.raises(ValueError):
                yt.set_user_password("admin", password)

    @authors("denvr")
    def test_get_current_user(self):
        assert yt.get_current_user() == {"user": "root"}
