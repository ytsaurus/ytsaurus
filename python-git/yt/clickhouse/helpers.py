import os.path

# This helper is used both in Python API tests and integration tests.
# Path may be overridden via env var like YTSERVER_CLICKHOUSE_PATH=/path/to/bin.
def get_host_paths(arcadia_interop, bins):
    result = {}
    if arcadia_interop.yatest_common is None:
        test_dir = os.path.join(os.path.dirname(__file__), "../../../yt/tests/integration/tests")
        from distutils.spawn import find_executable
        get_host_path = find_executable
        result["test-dir"] = test_dir
    else:
        # YT_ROOT is exported in corresponding test ya.make.
        yt_root = os.environ.get("YT_ROOT")
        if yt_root is not None:
            test_dir = os.environ.get("YT_ROOT") + "/yt/tests/integration/tests"
            test_dir = arcadia_interop.yatest_common.source_path(test_dir)
            result["test-dir"] = test_dir
        get_host_path = arcadia_interop.yatest_common.binary_path

    if "test-dir" in result:
        assert os.path.exists(result["test-dir"])

    for bin in bins:
        result[bin] = os.environ.get(bin.replace("-", "_").upper() + "_PATH") or get_host_path(bin)

    return result
