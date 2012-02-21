import sys
import os

sys.path.append(os.path.abspath('.'))
from yt_shell_test import ShellTest

import pytest

def pytest_collect_file(path, parent):
    if path.ext == ".sh" and path.basename.startswith("test"):
        name = path.basename[:-3] # remove extension
        return ShellTest(path, name, parent)
