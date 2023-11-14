from yt_odin_checks.lib.check_runner import catch_all_errors
from yt_odin_checks.lib.check_runner.argument_manager import CheckArgumentManager

from yt_odin.logserver import UNAVAILABLE_STATE, FAILED_STATE

import yt.wrapper as yt

import logging
import pytest


def test_catchers():
    def inner_buggy_foo():
        raise yt.YtError("Huge mistake!")

    def buggu_foo():
        inner_buggy_foo()
    assert catch_all_errors(buggu_foo, logging)() == UNAVAILABLE_STATE

    def buggy_bar():
        raise KeyError
    assert catch_all_errors(buggy_bar, logging)() == FAILED_STATE


def test_argument_parser():
    stdin_args = dict(
        options={},
        yt_client_params={},
        secrets={},
    )
    manager = CheckArgumentManager(logging, stdin_args)

    def good(options, yt_client, logger, states):
        pass

    def good2(yt_client, options):
        pass

    def good3(secrets):
        pass

    def unknown_argument(yt_client, logger, you_touch_my_tralala):
        pass

    def varargs(logger, states, *args):
        pass

    def kwargs(states, yt_client, **kwargs):
        pass

    def default_values(logger, states, yt_client, options=None):
        pass

    assert set(manager.make_arguments(good).keys()) == set(["options", "yt_client", "logger", "states"])
    assert set(manager.make_arguments(good2).keys()) == set(["yt_client", "options"])
    assert set(manager.make_arguments(good3).keys()) == set(["secrets"])

    for check_function in [unknown_argument, varargs, kwargs, default_values]:
        with pytest.raises(TypeError):
            manager.make_arguments(check_function)
