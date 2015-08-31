import sys
import os
import pytest

sys.path.insert(0, os.path.abspath('../../python'))
sys.path.append(os.path.abspath('.'))

from yt_shell_test import ShellFile

def pytest_runtest_setup(item):
    multicell_marker = item.keywords.get("multicell", None)
    if multicell_marker is not None and os.environ.get("ENABLE_YT_MULTICELL_TESTS") != "1":
        pytest.skip("Multicell tests require explicit ENABLE_YT_MULTICELL_TESTS=1")
