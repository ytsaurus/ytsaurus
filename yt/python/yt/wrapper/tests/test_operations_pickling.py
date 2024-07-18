from __future__ import print_function

from .conftest import authors
from .helpers import (TEST_DIR, set_config_option, get_tests_sandbox, get_test_file_path, get_test_files_dir_path,
                      build_python_egg, get_python, dumps_yt_config, run_python_script_with_check, get_operation_path,
                      check_rows_equality)

from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.spec_builders import VanillaSpecBuilder, MapSpecBuilder
from yt.wrapper import TypedJob
from yt.wrapper.schema import yt_dataclass
import yt.subprocess_wrapper as subprocess

# Necessary for tests.
try:
    import yt.wrapper.tests.test_module
    has_test_module = True
except ImportError:
    has_test_module = False

import yt.wrapper as yt

from flaky import flaky
import pytest

import tempfile
import sys
import time
import os
from pickle import PicklingError
from dataclasses import make_dataclass
from typing import Iterable


@pytest.mark.usefixtures("yt_env_with_increased_memory")
class TestOperationsPickling(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        yt.config["is_local_mode"] = False
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ["PYTHONPATH"]
        }

    @authors("asaitgalin")
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

    @authors("ignat")
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
        check_rows_equality(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

        with set_config_option("pickling/module_filter", filter):
            yt.run_map(mapper_no_test_module, table, table)
        check_rows_equality(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

        with set_config_option("pickling/module_filter", filter_string):
            yt.run_map(mapper_no_test_module, table, table)
        check_rows_equality(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

    @authors("ignat")
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
                    yt.run_map(check_platforms_are_different, table, TEST_DIR + "/out", format=yt.JsonFormat())
                yt.run_map(check_platforms_are_same, table, TEST_DIR + "/out", format=yt.JsonFormat())

                sys.platform = old_platform
                with set_config_option("pickling/enable_modules_compatibility_filter", True):
                    yt.run_map(check_platforms_are_same, table, TEST_DIR + "/out", format=yt.JsonFormat())
        finally:
            if old_ld_library_path:
                os.environ["LD_LIBRARY_PATH"] = old_ld_library_path
            sys.platform = old_platform

    @authors("ignat")
    @add_failed_operation_stderrs_to_error_message
    def test_eggs_file_usage_from_operation(self, yt_env_with_increased_memory):
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

        dir_ = yt_env_with_increased_memory.env.path
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

        op = yt.Operation(operation_id, "map")
        op.wait()
        assert list(yt.read_table(TEST_DIR + "/other_table")) == [{"x": "hello"}]

    @authors("ignat")
    @pytest.mark.usefixtures("test_dynamic_library")
    @add_failed_operation_stderrs_to_error_message
    def test_enable_dynamic_libraries_collection(self, test_dynamic_library):
        libs_dir, so_file = test_dynamic_library
        def mapper(rec):
            assert "_shared" in os.environ["LD_LIBRARY_PATH"]
            for root, dirs, files in os.walk("."):
                if so_file in files:
                    break
            else:
                assert False, "Dependency {0} not collected".format(so_file)
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}])

        old_ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = os.pathsep.join([old_ld_library_path, libs_dir])
        try:
            with set_config_option("pickling/dynamic_libraries/enable_auto_collection", True):
                 with set_config_option("pickling/dynamic_libraries/library_filter",
                                        lambda lib: not lib.startswith("/lib")):
                    yt.run_map(mapper, table, TEST_DIR + "/out")
        finally:
            if old_ld_library_path:
                os.environ["LD_LIBRARY_PATH"] = old_ld_library_path

    @authors("ignat")
    def test_disable_yt_accesses_from_job(self, yt_env_with_increased_memory):
        if yt.config["backend"] == "native":
            pytest.skip()

        first_script = """\
from __future__ import print_function

import sys

import yt.wrapper as yt


def mapper(rec):
    yield rec

yt.config["proxy"]["url"] = "{0}"
yt.config["pickling"]["enable_tmpfs_archive"] = False
yt.config["pickling"]["python_binary"] = sys.executable
print(yt.run_map(mapper, "{1}", "{2}", sync=False).id)
"""
        second_script = """\
from __future__ import print_function

import sys

import yt.wrapper as yt


def mapper(rec):
    yt.get("//@")
    yield rec

if __name__ == "__main__":
    yt.config["proxy"]["url"] = "{0}"
    yt.config["pickling"]["enable_tmpfs_archive"] = False
    yt.config["pickling"]["python_binary"] = sys.executable
    print(yt.run_map(mapper, "{1}", "{2}", sync=False).id)
"""
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        dir_ = yt_env_with_increased_memory.env.path
        for script in [first_script, second_script]:
            with tempfile.NamedTemporaryFile("w", dir=dir_, prefix="mapper", delete=False) as f:
                mapper = script.format(yt.config["proxy"]["url"],
                                       table,
                                       TEST_DIR + "/other_table")
                f.write(mapper)

            env = dict([(key, value) for key, value in os.environ.items() if key.startswith("PYTHON")])
            op_id = subprocess.check_output([get_python(), f.name], env=env).strip()
            op_path = get_operation_path(op_id)
            while not yt.exists(op_path) \
                    or yt.get(op_path + "/@state") not in ["aborted", "failed", "completed"]:
                time.sleep(0.2)
            assert yt.get(op_path + "/@state") == "failed"

            job_infos = yt.list_jobs(op_id, with_stderr=True, job_state="failed")["jobs"]
            assert len(job_infos) == 1
            for job_info in job_infos:
                assert b"Did you forget to surround" in yt.get_job_stderr(op_id, job_info["id"]).read()

    @authors("ignat")
    def test_python_operations_pickling(self, yt_env_with_increased_memory):
        test_script = """\
from __future__ import print_function
import yt.wrapper as yt

import yt.yson as yson
import sys

{mapper_code}

if __name__ == "__main__":
    stdin = sys.stdin
    if sys.version_info[0] >= 3:
        stdin = sys.stdin.buffer

    yt.update_config(yson.load(stdin, always_create_attributes=False))
    yt.config["pickling"]["enable_tmpfs_archive"] = False
    print(yt.run_map({mapper}, "{source_table}", "{destination_table}", format="json").id)
"""

        methods_pickling_test = ("""\
class C(object):
    {decorator}
    def do({self}):
        return 0

    def __call__(self, rec):
        self.do()
        yield rec
""", "C()")

        metaclass_pickling_test = ("""\


from abc import ABCMeta, abstractmethod


class AbstractClass(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass


class DoSomething(AbstractClass):
    def __init__(self):
        pass

    def do_something(self, rec):
        if "x" in rec:
            rec["x"] = int(rec["x"]) + 1
        return rec


class MapperWithMetaclass(object):
    def __init__(self):
        self.some_external_code = DoSomething()

    def map(self, rec):
        yield self.some_external_code.do_something(rec)
""", 'MapperWithMetaclass().map')

        simple_pickling_test = ("""\
def mapper(rec):
    yield rec
""", "mapper")

        methods_call_order_test = ("""\
class Mapper(object):
    def __init__(self):
        self.x = 42

    def start(self):
        self.x = 666

    def finish(self):
        self.x = 100500

    def __call__(self, rec):
        rec["x"] = self.x
        self.x += 1
        yield rec
""", "Mapper()")

        def _format_script(script, **kwargs):
            kwargs.update(dict(zip(("mapper_code", "mapper"), script)))
            return test_script.format(**kwargs)

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"x": 2}, {"x": 3}])
        run_python_script_with_check(
            yt_env_with_increased_memory,
            _format_script(simple_pickling_test, source_table=table, destination_table=table))
        check_rows_equality(yt.read_table(table), [{"x": 1}, {"x": 2}, {"x": 3}])

        yt.write_table(table, [{"x": 1}, {"x": 2}, {"x": 3}])
        for decorator, self_ in [("", "self"), ("@staticmethod", ""), ("@classmethod", "cls")]:
            yt.write_table(table, [{"x": 1}, {"y": 2}])
            script = (methods_pickling_test[0].format(decorator=decorator, self=self_),
                      methods_pickling_test[1])
            run_python_script_with_check(
                yt_env_with_increased_memory,
                _format_script(script, source_table=table, destination_table=table))
            check_rows_equality(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        run_python_script_with_check(
            yt_env_with_increased_memory,
            _format_script(metaclass_pickling_test, source_table=table, destination_table=table))
        check_rows_equality(yt.read_table(table), [{"x": 2}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 1}, {"x": 2}, {"x": 3}])
        run_python_script_with_check(
            yt_env_with_increased_memory,
            _format_script(methods_call_order_test, source_table=table, destination_table=table))
        assert list(yt.read_table(table)) == [{"x": 666}, {"x": 667}, {"x": 668}]

    @authors("ignat")
    def test_python_job_preparation_time(self):
        def mapper(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])
        op = yt.run_map(mapper, table, table, format=None, sync=False)
        op.wait()
        assert sorted(list(op.get_job_statistics()["custom"])) == ["python_job_preparation_time"]
        check_rows_equality(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

    @authors("ignat")
    def test_relative_imports_with_run_module(self, yt_env_with_increased_memory):
        yt.write_table("//tmp/input_table", [{"value": 0}])
        subprocess.check_call([sys.executable, "-m", "test_rel_import_module.run"],
                               cwd=get_test_files_dir_path(), env=self.env)
        check_rows_equality(yt.read_table("//tmp/output_table"), [{"value": 0, "constant": 10}])

    @authors("ignat")
    def test_local_file_attributes(self):
        def command(row):
            pass

        vanilla_spec = VanillaSpecBuilder()\
            .begin_task("sample")\
                .command("cat")\
                .job_count(1)\
                .file_paths(yt.LocalFile(get_test_file_path("capitalize_b.py"), attributes={"bypass_artifacts_cache": True}))\
            .end_task()

        result_spec = vanilla_spec.build()
        assert result_spec["tasks"]["sample"]["file_paths"][0].attributes == {"bypass_artifacts_cache": True, "file_name": "capitalize_b.py", "executable": True}

        try:
            yt.config["pickling"]["modules_bypass_artifacts_cache"] = True

            input_table = TEST_DIR + "/input"
            output_table = TEST_DIR + "/output"

            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                .command(command) \
                .end_mapper() \
                .input_table_paths(input_table) \
                .output_table_paths(output_table)

            result_spec = spec_builder.build()
            modules_file_count = 0
            for file_path in result_spec["mapper"]["file_paths"]:
                if "modules" in str(file_path.attributes["file_name"]):
                    modules_file_count += 1
                    assert file_path.attributes["bypass_artifacts_cache"]
            assert modules_file_count >= 1
        finally:
            yt.config["pickling"]["modules_bypass_artifacts_cache"] = None

    # Remove flaky after YT-10347.
    @authors("ignat")
    @flaky(max_runs=3)
    @add_failed_operation_stderrs_to_error_message
    def DISABLED_test_python_operations_and_file_cache(self):
        def func(row):
            yield row

        input = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input, [{"x": 1}, {"y": 2}])

        # Some strange things are happen.
        # Sometimes in the first iteration some modules occurred to be unimported (like yt_env.pyc).
        # So we only tests that regularly operation files are the same in sequential runs.
        failures = 0
        for i in range(5):
            yt.run_map(func, input, output)
            files_in_cache = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            assert len(files_in_cache) > 0

            yt.run_map(func, input, output)
            files_in_cache_again = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            if sorted(files_in_cache) != sorted(files_in_cache_again):
                failures += 1

        assert failures <= 2

        failures = 0
        for _ in range(3):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command(func) \
                    .format("json") \
                .end_mapper() \
                .input_table_paths(input) \
                .output_table_paths(output)
            yt.run_operation(spec_builder)
            files_in_cache = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            assert len(files_in_cache) > 0

            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command(func) \
                    .format("json") \
                .end_mapper() \
                .input_table_paths(input) \
                .output_table_paths(output)
            yt.run_operation(spec_builder)
            files_in_cache_again = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            if sorted(files_in_cache) != sorted(files_in_cache_again):
                failures += 1

        assert failures <= 1

    @authors("dmifedorov")
    def test_set_custom_pickler_params(self):
        @yt_dataclass
        class OutputRow:
            str: str

        dynamic_dataclass = yt_dataclass(make_dataclass("Row", fields=[("str", str)]))
        rows = [dynamic_dataclass(str="a")]

        yt.write_table_structured("//tmp/test_dynamic_dataclass_pickling", dynamic_dataclass, rows)

        class Mapper(TypedJob):
            def __call__(self, row: dynamic_dataclass) -> Iterable[OutputRow]:
                yield OutputRow(str=row.str)

        with pytest.raises(PicklingError):
            yt.run_map(Mapper(), "//tmp/test_dynamic_dataclass_pickling", "//tmp/test_dynamic_dataclass_pickling")

        with set_config_option("pickling/pickler_kwargs", [{"key": "byref", "value": False}]):
            yt.run_map(Mapper(), "//tmp/test_dynamic_dataclass_pickling", "//tmp/test_dynamic_dataclass_pickling")
