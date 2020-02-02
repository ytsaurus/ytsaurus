import os

try:
    from yp.tests.helpers.conftest import *
except ImportError:
    from .helpers.conftest import *


if yatest_common is None:
    pytest_plugins = "yt.test_runner.plugin"

if "AUTH" in os.environ:
    try:
        from yp.tests.helpers.conftest_auth import *
    except ImportError:
        pass
