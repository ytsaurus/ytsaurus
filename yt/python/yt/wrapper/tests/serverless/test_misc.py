import yt.logger as yt_logger
import yt.wrapper as yt

from typing import get_type_hints
from unittest import mock

import pytest

from yt.testlib import authors


@authors("denvr")
def test_config_types():
    def _check_keys(type_object, config_object):
        type_hints = get_type_hints(type_object)
        for param_name, param_value in config_object.items():
            assert param_name in type_hints, "New config parameter should be described in default_config.DefaultConfigType"
            if isinstance(param_value, dict) and param_value:
                _check_keys(type_hints[param_name], param_value)

    _check_keys(yt.default_config.DefaultConfigType, yt.default_config.default_config)


@authors("asklit")
@pytest.mark.parametrize("token", [
    " token",
    "token ",
    "\ttoken",
    "token\t",
    "\rtoken",
    "token\r",
    "\ntoken",
    "token\n",
    " \t\r\n",
])
def test_token_with_leading_or_trailing_whitespace(token):
    with pytest.raises(
            yt.YtTokenError,
            match="Authentication token contains leading or trailing whitespace"):
        yt._get_token(token=token)


@authors("asklit")
def test_config_token_with_trailing_whitespace():
    client = yt.YtClient(config={"token": "token\n"})

    with pytest.raises(
            yt.YtTokenError,
            match="Authentication token contains leading or trailing whitespace"):
        yt._get_token(client=client)


@authors("asklit")
def test_token_with_inner_whitespace_uses_generic_error():
    with pytest.raises(yt.YtTokenError, match="You have an improper authentication token"):
        yt._get_token(token="token with space")


@authors("denvr")   # author: marydrobotun@gmail.com
def test_log_once():
    with mock.patch.object(yt_logger.LOGGER, "log") as logger_mock, \
            mock.patch.object(yt_logger, "MAX_BUFF_LEN", 5), \
            mock.patch.object(yt_logger, "BUFF_CLEANING_LEN", 2):
        yt_logger.log_once(30, 'test1')
        yt_logger.log_once(30, 'test1')
        yt_logger.log_once(30, 'test2')
        yt_logger.log_once(30, 'test3')
        yt_logger.log_once(30, 'test4')
        assert len(yt_logger.LOG_ONCE_BUFF) == 4
        yt_logger.log_once(30, 'test5')
        assert logger_mock.call_count == 5
        assert len(yt_logger.LOG_ONCE_BUFF) == 3, "clean"
        assert logger_mock.call_count == 5, "hit after clean"
        yt_logger.log_once(30, 'test5')
        assert logger_mock.call_count == 5, "hit after clean"
        yt_logger.log_once(30, 'test1')
        assert logger_mock.call_count == 6, "miss after clean"
