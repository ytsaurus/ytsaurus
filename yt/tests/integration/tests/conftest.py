import sys
import os

sys.path.insert(0, os.path.abspath('../../../python'))
sys.path.append(os.path.abspath('.'))

pytest_plugins = "yt.tests_runner.plugin"

def pytest_runtest_makereport(item, call, __multicall__):
    rep = __multicall__.execute()
    if hasattr(item, "cls") and hasattr(item.cls, "Env"):
        rep.environment_path = item.cls.Env.path
    return rep
