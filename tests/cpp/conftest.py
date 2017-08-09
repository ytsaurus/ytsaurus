import sys
import os

sys.path.insert(0, os.path.abspath('../../tests/integration'))
sys.path.insert(0, os.path.abspath('../../python'))
sys.path.append(os.path.abspath('.'))

from yt_shell_test import CppFile


def pytest_collect_file(parent, path):
    if path.ext == '.cpp' and path.basename.startswith('test'):
        return CppFile(path, parent)

def pytest_addoption(parser):
    parser.addoption("--gtest_filter", action="store", default=None, help="gtest filter option")
