import sys
if sys.version_info[0] < 3:
    pytest_plugins = "yt.tests_runner.plugin"
