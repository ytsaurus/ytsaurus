# Adopted from the MyPy project
# https://github.com/python/mypy/blob/3dd2bdf50c086deaf85e879e9b7f0b524fe4f974/conftest.py

import os.path

pytest_plugins = [
    'mypy.test.data',
]


def pytest_configure(config):
    mypy_source_root = os.path.dirname(os.path.abspath(__file__))
    if os.getcwd() != mypy_source_root:
        os.chdir(mypy_source_root)
