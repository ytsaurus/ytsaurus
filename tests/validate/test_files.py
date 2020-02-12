import os

import yatest.common


def is_test_file(filename):
    return filename.startswith("test_") and filename.endswith(".py")


def list_yamake_test(ya_make):
    tests = set()
    for line in open(yatest.common.source_path(ya_make)):
        line = line.strip().split("/")[-1]
        if is_test_file(line):
            tests.add(line)
    return tests


def test_all_files_added_to_ya_make():
    test_dir = yatest.common.source_path("yt/tests/integration/tests")
    test_files = set([f for f in os.listdir(test_dir) if is_test_file(f)])

    tests = list_yamake_test("yt/tests/integration/tests/ya.make")
    large_tests = list_yamake_test("yt/tests/integration/large/ya.make")
    kvm_tests = list_yamake_test("yt/tests/integration/large/ya.make")
    clickhouse_tests = list_yamake_test("yt/tests/clickhouse/ya.make")

    assert test_files == tests | large_tests | kvm_tests | clickhouse_tests
