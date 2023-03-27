import conftest_lib.conftest

pytest_plugins = [
    "queries.environment",
] + conftest_lib.conftest.pytest_plugins
