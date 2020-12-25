from yt_commands import (raises_yt_error, create, FormatDisabled, write_table, authors, create_user)

import yt.yson

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import tempfile


class _TestProxyFormatConfigBase:
    __metaclass__ = ABCMeta

    FORMAT_CONFIG = {
        "yamr": {
            "enable": False,
            "user_overrides": {
                "good_user": {
                    "enable": True,
                },
                "bad_user": {
                    "enable": False,
                },
            },
        },
        "yamred_dsv": {
            "enable": True,
            "user_overrides": {
                "good_user": {
                    "enable": True,
                },
                "bad_user": {
                    "enable": False,
                },
            },
        },
        "yson": {
            "default_attributes": {
                "format": "text",
            },
            "user_overrides": {
                "binary_user": {
                    "default_attributes": {
                        "format": "binary",
                    },
                },
                "text_user": {
                    "default_attributes": {
                        "format": "text",
                    },
                },
                "pretty_user": {
                    "default_attributes": {
                        "format": "pretty",
                    },
                },
                "empty_user": {
                    "default_attributes": {},
                },
            },
        },
    }

    TABLE_CONTENT_TWO_COLUMNS = [
        {"key": "1", "value": "bar"},
        {"key": "2", "value": "baz"},
        {"key": "3", "value": "qux"},
    ]
    TABLE_CONTENT_ONE_COLUMN = [
        {"key": "1"},
        {"key": "2"},
        {"key": "3"},
    ]
    YAMR = yt.yson.to_yson_type("yamr")
    YAMRED_DSV = yt.yson.to_yson_type("yamred_dsv", attributes={
        "columns": ["key", "value"],
        "key_column_names": ["key"],
    })
    YSON = yt.yson.to_yson_type("yson")
    BINARY_YSON = yt.yson.to_yson_type("yson", attributes={"format": "binary"})
    TEXT_YSON = yt.yson.to_yson_type("yson", attributes={"format": "text"})
    PRETTY_YSON = yt.yson.to_yson_type("yson", attributes={"format": "pretty"})

    @abstractmethod
    def _do_run_operation(self, op_type, spec, user, use_start_op):
        return

    @abstractmethod
    def _test_format_enable(self, format, user, enable):
        return

    @staticmethod
    def _parse_format(format, data):
        format_name = str(format)
        if format_name == "yson":
            return list(yt.yson.loads(data, yson_type="list_fragment"))
        elif format_name in ("yamr", "yamred_dsv"):
            result = []
            for line in data.strip().split("\n"):
                k, v = line.strip().split()
                if format_name == "yamred_dsv":
                    assert v.startswith("value=")
                    v = v[len("value="):]
                result.append({"key": k, "value": v})
            return result
        else:
            assert False

    @staticmethod
    def _write_format(format, rows):
        format_name = str(format)
        if format_name == "yson":
            return yt.yson.dumps(rows, yson_type="list_fragment")
        elif format_name == "yamr":
            return "\n".join("\t".join([r["key"], r["value"]]) for r in rows)
        elif format_name == "yamred_dsv":
            return "\n".join("{}\tvalue={}".format(r["key"], r["value"]) for r in rows)
        else:
            assert False

    def _get_context_manager(self, enable):
        if enable:
            @contextmanager
            def manager():
                yield
        else:
            def manager():
                return raises_yt_error(FormatDisabled)
        return manager

    def _run_operation(self, op_type, spec, user, use_start_op=True, content=[]):
        schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
        ]
        create("table", "//tmp/t_in", force=True, attributes={"schema": schema})
        write_table("//tmp/t_in", content)
        create("table", "//tmp/t_out", force=True)
        spec["input_table_paths"] = ["//tmp/t_in"]
        spec["output_table_paths"] = ["//tmp/t_out"]
        op = self._do_run_operation(op_type, spec, user, use_start_op)
        op.track()

    def _test_format_enable_operations(self, format, user, enable):
        map_spec = {
            "mapper": {
                "command": "sleep 0",
                "output_format": format,
            },
        }
        reduce_spec = {
            "reduce_by": ["key"],
            "reducer": {
                "command": "sleep 0",
                "input_format": format,
            }
        }
        map_reduce_spec = {
            "reduce_by": ["key"],
            "sort_by": ["key"],
            "mapper": {
                "command": "sleep 0",
                "input_format": format,
            },
            "reducer": {
                "command": "sleep 0",
                "format": format,
            }
        }
        vanilla_spec = {
            "tasks": {
                "task1": {
                    "command": "sleep 0",
                    "output_format": format,
                    "job_count": 1,
                },
            },
        }
        file_path = yt.yson.to_yson_type("//tmp/file", attributes={"format": format})
        create("table", file_path, force=True)
        write_table(file_path, self.TABLE_CONTENT_TWO_COLUMNS)
        vanilla_spec_with_file = {
            "tasks": {
                "task1": {
                    "command": "sleep 0",
                    "job_count": 1,
                    "file_paths": [file_path],
                },
            },
        }

        manager = self._get_context_manager(enable)
        with manager():
            self._run_operation("map", map_spec, user)
        with manager():
            self._run_operation("map", map_spec, user, use_start_op=False)
        with manager():
            self._run_operation("reduce", reduce_spec, user)
        with manager():
            self._run_operation("join_reduce", reduce_spec, user)
        with manager():
            self._run_operation("map_reduce", map_reduce_spec, user)
        with manager():
            self._run_operation("vanilla", vanilla_spec, user)
        with manager():
            self._run_operation("vanilla", vanilla_spec_with_file, user)

    def _test_format_defaults_operations(self, format, user, content, expected_content):
        with tempfile.NamedTemporaryFile() as op_input:
            spec = {
                "mapper": {
                    "command": "cat > {}".format(op_input.name),
                    "input_format": format,
                },
            }
            self._run_operation("map", spec, user, content=content)
            assert op_input.read() == expected_content

        with tempfile.NamedTemporaryFile() as file_from_spec:
            file_path = yt.yson.to_yson_type("//tmp/file_table", attributes={"format": format})
            create("table", file_path, force=True)
            write_table(file_path, content)
            spec = {
                "tasks": {
                    "task1": {
                        "command": "cat ./file_table > {}".format(file_from_spec.name),
                        "job_count": 1,
                        "file_paths": [file_path],
                    },
                },
            }
            self._run_operation("vanilla", spec, user)
            actual_content = file_from_spec.read()
            print(actual_content)
            assert actual_content == expected_content

    @authors("levysotsky")
    def test_format_enable(self):
        for user in ["some_user", "good_user", "bad_user"]:
            create_user(user)

        self._test_format_enable(self.YAMR, "some_user", False)
        self._test_format_enable(self.YAMR, "root", True)
        self._test_format_enable(self.YAMR, "good_user", True)
        self._test_format_enable(self.YAMR, "bad_user", False)

        self._test_format_enable(self.YAMRED_DSV, "some_user", True)
        self._test_format_enable(self.YAMRED_DSV, "root", True)
        self._test_format_enable(self.YAMRED_DSV, "good_user", True)
        self._test_format_enable(self.YAMRED_DSV, "bad_user", False)

        self._test_format_enable(self.YSON, "some_user", True)
        self._test_format_enable(self.YSON, "root", True)
        self._test_format_enable(self.YSON, "good_user", True)
        self._test_format_enable(self.YSON, "bad_user", True)

    @authors("levysotsky")
    def test_format_defaults(self):
        for user in ["binary_user", "text_user", "pretty_user", "default_user", "empty_user"]:
            create_user(user)

        content = self.TABLE_CONTENT_ONE_COLUMN

        def check(format, user, expected_format):
            self._test_format_defaults(format, user, content, expected_format)

        check(self.YSON, "binary_user", self.BINARY_YSON)
        check(self.TEXT_YSON, "binary_user", self.TEXT_YSON)
        check(self.BINARY_YSON, "binary_user", self.BINARY_YSON)

        check(self.YSON, "text_user", self.TEXT_YSON)
        check(self.TEXT_YSON, "text_user", self.TEXT_YSON)
        check(self.BINARY_YSON, "text_user", self.BINARY_YSON)

        check(self.YSON, "pretty_user", self.PRETTY_YSON)
        check(self.TEXT_YSON, "pretty_user", self.TEXT_YSON)
        check(self.BINARY_YSON, "pretty_user", self.BINARY_YSON)

        check(self.YSON, "default_user", self.TEXT_YSON)
        check(self.TEXT_YSON, "default_user", self.TEXT_YSON)
        check(self.BINARY_YSON, "default_user", self.BINARY_YSON)

        check(self.YSON, "empty_user", self.BINARY_YSON)
        check(self.TEXT_YSON, "empty_user", self.TEXT_YSON)
        check(self.BINARY_YSON, "empty_user", self.BINARY_YSON)
