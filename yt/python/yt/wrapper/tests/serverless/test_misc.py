import yt.logger as yt_logger
import yt.wrapper as yt
from yt.wrapper.spec_builder_helpers import BaseLayerDetector

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


@authors("pogorelov")
@pytest.mark.parametrize("codename,layer_path", [
    ("xenial", "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_latest"),
    ("bionic", "//porto_layers/base/bionic/porto_layer_search_ubuntu_bionic_app_latest"),
    ("focal", "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_latest"),
    ("jammy", "//porto_layers/base/jammy/porto_layer_search_ubuntu_jammy_app_latest"),
    ("precise", "//porto_layers/base/precise/porto_layer_search_ubuntu_precise_app_lastest.tar.gz"),
    ("trusty", "//porto_layers/base/trusty/porto_layer_search_ubuntu_trusty_app_lastest.tar.gz"),
    ("noble", "//porto_layers/base/noble/porto_layer_search_ubuntu_noble_app_lastest.tar.gz"),
])
def test_porto_base_layer_fallback(codename, layer_path):
    client = yt.YtClient()
    os_env = ("ubuntu", codename, None)
    with mock.patch.object(BaseLayerDetector, "_detect_os_env", return_value=os_env), \
            mock.patch.object(yt, "exists", side_effect=lambda path, **kwargs: path == layer_path):
        assert BaseLayerDetector._get_default_layer(client, "porto") == [layer_path]


@authors("pogorelov")
def test_porto_base_layer_missing_canonical_alias():
    client = yt.YtClient()
    legacy_path = "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz"
    os_env = ("ubuntu", "focal", None)
    with mock.patch.object(BaseLayerDetector, "_detect_os_env", return_value=os_env), \
            mock.patch.object(yt, "exists", side_effect=lambda path, **kwargs: path == legacy_path):
        assert BaseLayerDetector._get_default_layer(client, "porto") is None


@authors("pogorelov")
def test_porto_base_layer_registry_precedes_fallback():
    client = yt.YtClient()
    registry = {"porto": {"//custom/python-layer": {
        "os": "ubuntu",
        "tags": ["os_codename=focal", "python=3.11"],
        "base_layers": ["//custom/base-layer"],
    }}}
    os_env = ("ubuntu", "focal", (3, 11))
    with mock.patch.object(BaseLayerDetector, "_detect_os_env", return_value=os_env), \
            mock.patch.object(yt, "exists", return_value=True), \
            mock.patch.object(yt, "get", return_value=registry):
        assert BaseLayerDetector._get_default_layer(client, "porto") == [
            "//custom/python-layer",
            "//custom/base-layer",
        ]


@authors("pogorelov")
def test_porto_base_layer_keeps_explicit_spec():
    client = yt.YtClient(config={"operation_base_layer": "porto:auto"})
    spec = {"layer_paths": ["//custom/explicit-layer"]}
    assert BaseLayerDetector.guess_base_layers(spec, client) == {
        "layer_paths": ["//custom/explicit-layer"],
    }
