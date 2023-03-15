import yatest.common


TEST_BIN = yatest.common.binary_path("yt/cpp/roren/library/save_load_wrapper/interprocess_test/test/test")


def test_interprocess_saveload():
    filepath = yatest.common.output_path() + "/test"
    yatest.common.execute([TEST_BIN, "write", filepath], wait=True)
    execution = yatest.common.execute([TEST_BIN, "read", filepath], wait=True)
    result = execution.stdout.decode()
    assert result == "101\n202\n303\n"
