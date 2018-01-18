from __future__ import print_function

from .helpers import (PYTHONPATH, TEST_DIR, set_config_option, get_tests_sandbox, check,
                      get_test_file_path, build_python_egg, get_python, dumps_yt_config)

from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
import yt.subprocess_wrapper as subprocess

# Necessary for tests.
try:
    import yt.wrapper.tests.test_module
    has_test_module = True
except ImportError:
    has_test_module = False

import yt.wrapper as yt

import tempfile
import pytest
import sys
import os

@pytest.mark.usefixtures("yt_env")
class TestOperationsPickling(object):
    @add_failed_operation_stderrs_to_error_message
    def test_pickling(self):
        def foo(rec):
            import my_test_module
            assert my_test_module.TEST == 1
            yield rec

        with tempfile.NamedTemporaryFile(mode="w",
                                         suffix=".py",
                                         dir=get_tests_sandbox(),
                                         prefix="test_pickling",
                                         delete=False) as f:
            f.write("TEST = 1")

        with set_config_option("pickling/additional_files_to_archive", [(f.name, "my_test_module.py")]):
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1}])
            yt.run_map(foo, table, table)

    @add_failed_operation_stderrs_to_error_message
    def test_module_filter(self):
        assert has_test_module

        def mapper_test_module(row):
            import yt.wrapper.tests.test_module
            assert yt.wrapper.tests.test_module

            yield row

        def mapper_no_test_module(row):
            try:
                import yt.wrapper.tests.test_module
                assert yt.wrapper.tests.test_module
                print("NOT OK", file=sys.stderr)
                raise Exception()
            except ImportError:
                print("OK", file=sys.stderr)

            yield row

        table = TEST_DIR + "/table"

        yt.write_table(table, [{"x": 1}, {"y": 2}])

        filter = lambda module: hasattr(module, "__file__") and not "test_module" in module.__file__
        filter_string = 'lambda module: hasattr(module, "__file__") and not "test_module" in module.__file__'

        yt.run_map(mapper_test_module, table, table)
        check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

        with set_config_option("pickling/module_filter", filter):
            yt.run_map(mapper_no_test_module, table, table)
        check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

        with set_config_option("pickling/module_filter", filter_string):
            yt.run_map(mapper_no_test_module, table, table)
        check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

    @pytest.mark.usefixtures("test_dynamic_library")
    @add_failed_operation_stderrs_to_error_message
    def test_modules_compatibility_filter(self, test_dynamic_library):
        libs_dir, so_file = test_dynamic_library
        def check_platforms_are_different(rec):
            assert "_shared" in os.environ["LD_LIBRARY_PATH"]
            for root, dirs, files in os.walk("."):
                if so_file in files:
                    assert False, "Dependency {0} is collected".format(so_file)
            yield rec

        def check_platforms_are_same(rec):
            assert "_shared" in os.environ["LD_LIBRARY_PATH"]
            for root, dirs, files in os.walk("."):
                if so_file in files:
                    break
            else:
                assert False, "Dependency {0} is not collected".format(so_file)
            yield rec

        old_platform = sys.platform
        sys.platform = "linux_test_platform"

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}])

        old_ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = os.pathsep.join([old_ld_library_path, libs_dir])
        try:
            with set_config_option("pickling/dynamic_libraries/enable_auto_collection", True):
                with set_config_option("pickling/enable_modules_compatibility_filter", True):
                    yt.run_map(check_platforms_are_different, table, TEST_DIR + "/out")
                yt.run_map(check_platforms_are_same, table, TEST_DIR + "/out")

                sys.platform = old_platform
                with set_config_option("pickling/enable_modules_compatibility_filter", True):
                    yt.run_map(check_platforms_are_same, table, TEST_DIR + "/out")
        finally:
            if old_ld_library_path:
                os.environ["LD_LIBRARY_PATH"] = old_ld_library_path
            sys.platform = old_platform

    @add_failed_operation_stderrs_to_error_message
    def test_eggs_file_usage_from_operation(self, yt_env):
        script = """\
from __future__ import print_function

import yt.wrapper as yt
from module_in_egg import hello_provider

def mapper(rec):
    yield {{"x": hello_provider.get_message()}}

if __name__ == "__main__":
    yt.config["pickling"]["enable_tmpfs_archive"] = False
    print(yt.run_map(mapper, "{1}", "{2}", sync=False).id)
"""
        yt.write_table(TEST_DIR + "/table", [{"x": 1, "y": 1}])

        dir_ = yt_env.env.path
        with tempfile.NamedTemporaryFile("w", dir=dir_, prefix="mapper", delete=False) as f:
            mapper = script.format(yt.config["proxy"]["url"],
                                   TEST_DIR + "/table",
                                   TEST_DIR + "/other_table")
            f.write(mapper)

        module_egg = build_python_egg(get_test_file_path("yt_test_module"), temp_dir=dir_)

        env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.pathsep.join([module_egg, os.environ["PYTHONPATH"]])
        }

        operation_id = subprocess.check_output([get_python(), f.name], env=env).strip()

        op = yt.Operation("map", operation_id)
        op.wait()
        assert list(yt.read_table(TEST_DIR + "/other_table")) == [{"x": "hello"}]

