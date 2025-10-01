import yt.logger as yt_logger
import yt.wrapper as yt
from typing import get_type_hints

from yt.testlib import authors

from unittest.mock import Mock


@authors("denvr")
def test_config_types():
    def _check_keys(type_object, config_object):
        type_hints = get_type_hints(type_object)
        for param_name, param_value in config_object.items():
            assert param_name in type_hints, "New config parameter should be described in default_config.DefaultConfigType"
            if isinstance(param_value, dict) and param_value:
                _check_keys(type_hints[param_name], param_value)

    _check_keys(yt.default_config.DefaultConfigType, yt.default_config.default_config)


def test_log_once():
    tmp_log = yt_logger.LOGGER.log
    logger_mock = Mock()
    yt_logger.LOGGER.log = logger_mock
    yt_logger.log_once(30, 'test')
    yt_logger.log_once(30, 'test')
    yt_logger.log_once(30, 'test')
    assert logger_mock.call_count == 1
    yt_logger.LOGGER.log = tmp_log