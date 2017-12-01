import yt.tests_runner as tests_runner

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from collections import defaultdict
import re
import sys

pytest_plugins = "yt.tests_runner.plugin"

if yatest_common is None:
    def pytest_generate_tests(metafunc):
        metafunc.parametrize("interpreter", ["{0}.{1}".format(*sys.version_info[:2])], indirect=True)

def pytest_configure(config):
    def scheduling_func(test_items, process_count):
        HEAVY_TESTS_SPLIT_FACTOR = {
            "TestOperations": 3,
            "TestYamrMode": 2,
            "TestTableCommands": 1,
            "TestTransferManager": 1,
            "TestYtBinary": 1
        }

        NO_API_SPLIT_TESTS = set([
            "TestTransferManager",
            "TestYtBinary"
        ])

        suites = defaultdict(list)
        for index, test in enumerate(test_items):
            match = re.search(r"\[([a-zA-Z0-9.-]+)\]$", test.name)

            suite_name = ""
            test_class_name = None
            try:
                test_class_name = test.cls.__name__
            except AttributeError:
                pass

            if match and test_class_name not in NO_API_SPLIT_TESTS:
                # py.test uses "-" as delimiter when
                # it writes parameters for test.
                parameters = match.group(1).split("-")
                for param in parameters:
                    if param in ["v3", "native"]:
                        suite_name = param
                        break

            if test_class_name in HEAVY_TESTS_SPLIT_FACTOR:
                split_process_id = hash(test.name) % HEAVY_TESTS_SPLIT_FACTOR[test_class_name]
                suite_name = test_class_name + suite_name + str(split_process_id)

            suites[suite_name].append(index)

        return tests_runner.split_test_suites(suites, process_count)

    tests_runner.set_scheduling_func(scheduling_func)
