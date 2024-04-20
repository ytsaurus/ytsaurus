from yt.testlib import authors  # noqa

import yt.yson
import yt.ypath
import yt.wrapper

import os


@authors("denvr")
def test_fork():
    if os.environ["TEST_NAME"] == "py3test":
        assert yt.yson.__file__ == 'yt/python/yt/yson/__init__.py'
        assert yt.ypath.__file__ == 'yt/python/yt/ypath/__init__.py'
        assert yt.wrapper.__file__ == 'yt/python/yt/wrapper/__init__.py'
    elif os.environ["TEST_NAME"] == "pytest":
        assert yt.yson.__file__ == 'yt/python_py2/yt/yson/__init__.py'
        assert yt.ypath.__file__ == 'yt/python_py2/yt/ypath/__init__.py'
        assert yt.wrapper.__file__ == 'yt/python_py2/yt/wrapper/__init__.py'
    else:
        assert False
