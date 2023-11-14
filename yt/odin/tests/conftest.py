from yt.environment.arcadia_interop import yatest_common

import os

pytest_plugins = [
    "yt_odin.test_helpers",
]


def get_tests_location():
    if yatest_common is None:
        return os.path.abspath(os.path.dirname(__file__))
    else:
        return yatest_common.source_path("yt/odin/tests")


def get_tests_checks_path():
    return os.path.join(get_tests_location(), "data/checks")
