import os

try:
    from yp.tests.helpers.conftest import *
except ImportError:
    from .helpers.conftest import *

if "AUTH" in os.environ:
    try:
        from yp.tests.helpers.conftest_auth import *
    except ImportError:
        pass
