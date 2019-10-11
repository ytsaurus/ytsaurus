import yt.test_runner as test_runner

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from collections import defaultdict
import re
import sys
import pytest

pytest_plugins = "yt.test_runner.plugin"

if yatest_common is None:
    @pytest.fixture(scope="session", autouse=True)
    def interpreter():
        pass

    def pytest_generate_tests(metafunc):
        metafunc.parametrize("interpreter", ["{0}.{1}".format(*sys.version_info[:2])])

def pytest_configure(config):
    def scheduling_func(test_items, process_count):
        if sys.version_info[0] == 2:
            DEFAULT_SPLIT_FACTOR = 5
        else:
            DEFAULT_SPLIT_FACTOR = 3
        TESTS_SPLIT_FACTOR = {
            "TestYtBinary": 1,
            "TestMapreduceBinary": 1,
        }

        NO_API_SPLIT_TESTS = set([
            "TestTransferManager",
            "TestYtBinary",
        ])

        suites = defaultdict(list)
        for index, test in enumerate(test_items):
            match = re.search(r"\[([a-zA-Z0-9.-]+)\]$", test.name)

            suite_name = ""
            test_class_name = ""
            try:
                test_class_name = test.cls.__name__
            except AttributeError:
                pass

            if match and test_class_name not in NO_API_SPLIT_TESTS:
                # py.test uses "-" as delimiter when
                # it writes parameters for test.
                parameters = match.group(1).split("-")
                for param in parameters:
                    if param in ["v3", "v4", "rpc", "native", "native_v4"]:
                        suite_name = param
                        break

            split_process_id = hash(test.name) % TESTS_SPLIT_FACTOR.get(test_class_name, DEFAULT_SPLIT_FACTOR)
            suite_name = test_class_name + suite_name + str(split_process_id)

            suites[suite_name].append(index)

        return test_runner.split_test_suites(suites, process_count)

    test_runner.set_scheduling_func(scheduling_func)
