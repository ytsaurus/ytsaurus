import sys
import os

sys.path.append(os.path.abspath('.'))
from yt_shell_test import ShellFile

import pytest

##################################################################

def pytest_collect_file(path, parent):
    if path.ext == ".sh" and path.basename.startswith("test"):
        return ShellFile(path, parent)
