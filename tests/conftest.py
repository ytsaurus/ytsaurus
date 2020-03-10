import os

try:
    from yp.tests.helpers.conftest import *  # noqa
except ImportError:
    from .helpers.conftest import *  # noqa


if yatest_common is None:  # noqa
    pytest_plugins = "yt.test_runner.plugin"

if "AUTH" in os.environ:
    try:
        from yp.tests.helpers.conftest_auth import *  # noqa
    except ImportError:
        pass
