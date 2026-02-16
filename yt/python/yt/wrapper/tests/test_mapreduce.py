from .conftest import authors
from .helpers import (get_tests_sandbox, wait,
                      get_environment_for_binary_test)
from .helpers_cli import YtCli

from yt.testlib.test_environment import YtTestEnvironment
from yt.common import makedirp

import yt.wrapper as yt

import pytest

from flaky import flaky

import os
import uuid
import subprocess
import json
import stat
import time


@pytest.fixture(params=[False, True])
def yt_cli(request: pytest.FixtureRequest, yt_env_for_yamr: YtTestEnvironment):
    env = get_environment_for_binary_test(yt_env_for_yamr)
    env["YT_PREFIX"] = "//home/wrapper_tests/"

    if request.param:
        env["ENABLE_SCHEMA"] = "1"
        env["YT_CONFIG_PATCHES"] = "{yamr_mode={create_schema_on_tables=%true}};"+env["YT_CONFIG_PATCHES"]

    sandbox_root: str = get_tests_sandbox()  # type: ignore
    test_name = request.node.name
    sandbox_dir = os.path.join(sandbox_root, f"TestYtBinary_{test_name}_" + uuid.uuid4().hex[:8])
    makedirp(sandbox_dir)

    with open(os.path.join(sandbox_dir, "table_file"), "w") as file:
        file.write("4\t5\t6\n1\t2\t3")

    with open(os.path.join(sandbox_dir, "big_file"), "w") as file:
        for i in range(1, 11):
            for j in range(1, 11):
                file.write(f"{i}\tA\t{j}\n")
            file.write(f"{i}\tX\tX\n")

    yt.remove("//home/wrapper_tests", recursive=True, force=True)

    replace = {
        "yt": [env["PYTHON_BINARY"], env["YT_CLI_PATH"]],
        "mapreduce-yt": [env["PYTHON_BINARY"], env["MAPREDUCE_YT_CLI_PATH"]]
    }
    yield YtCli(env, sandbox_dir, replace)


def read_file_as_string(file_path: str, cwd: str | None = None):
    """if cwd is not specified, file_path is expected to be an absolute path"""
    path = os.path.join(cwd, file_path) if cwd else file_path
    with open(path, "r") as file:
        return "".join(file.readlines())


def lines_count(content: str | bytes):
    if isinstance(content, bytes):
        text = content.decode("utf-8")
    elif isinstance(content, str):
        text = content
    else:
        raise TypeError
    text = text.strip()
    if text == "":
        return 0
    return len(text.split("\n"))


@flaky(max_runs=3)
@pytest.mark.timeout(1800)
@pytest.mark.usefixtures("yt_env_for_yamr")
class TestYamrModeMapreduceBinary(object):
    @authors("ilyaibraev")
    def test_base_functionality(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-list"])
        yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "//unexisting/path"])
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignats/temp"])
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-move", "-src", "ignat/temp", "-dst", "ignat/other_table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/other_table", "-dst", "ignat/temp"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/other_table", "-dst", "ignat/other_table"])

        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/temp"])

        yt_cli.check_output(["mapreduce-yt", "-sort", "-src", "ignat/other_table", "-dst", "ignat/other_table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table"]) == b"1\t2\t3\n4\t5\t6\n"
        yt_cli.check_output(["mapreduce-yt", "-sort", "ignat/other_table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table"]) == b"1\t2\t3\n4\t5\t6\n"
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/other_table", "-lowerkey", "3"]) == b"4\t5\t6\n"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "ignat/other_table", "-dst", "ignat/mapped", "-ytspec", '{"job_count": 10}'])

        stdout = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/mapped"])
        assert lines_count(stdout) == 2
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "ignat/other_table", "-src", "ignat/mapped", "-dstappend", "ignat/temp"])
        yt_cli.check_output(["mapreduce-yt", "-orderedmap", "cat", "-src", "ignat/other_table", "-src", "ignat/mapped", "-dst", "ignat/temp", "-append"])
        stdout = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp"])
        assert lines_count(stdout) == 8

        yt_cli.check_output(["mapreduce-yt", "-reduce", "cat", "-src", "ignat/other_table", "-dst", "ignat/temp"])
        stdout = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp"])
        assert lines_count(stdout) == 2
        yt_cli.env["MR_TABLE_PREFIX"] = "ignat/"
        yt_cli.check_output(["mapreduce-yt", "-reduce", "cat", "-src", "other_table", "-dst", "temp2"])
        del yt_cli.env["MR_TABLE_PREFIX"]
        stdout = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp2"])
        assert lines_count(stdout) == 2

        yt_cli.check_output(["mapreduce-yt", "-hash-reduce", "cat", "-src", "ignat/other_table", "-dst", "ignat/temp"])
        stdout = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp"])
        assert lines_count(stdout) == 2

    @authors("ilyaibraev")
    def test_copy_move(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/table"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/unexisting_table", "-dstappend", "ignat/table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-move", "-src", "ignat/unexisting_table", "-dstappend", "ignat/table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/table"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/unexisting_table", "-dst", "ignat/table"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/table"]) == b""

    @authors("ilyaibraev")
    def test_list(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/test_dir/table1"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/test_dir/table2"], stdin=read_file_as_string("table_file", yt_cli.cwd))

        yt_cli.check_output(["yt", "create", "table", "ignat/test_dir/table3"])

        yt_cli.env["YT_IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST"] = "1"
        yt_cli.env["YT_USE_YAMR_STYLE_PREFIX"] = "1"

        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir"]) == b"ignat/test_dir/table1\nignat/test_dir/table2\n"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir/"]) == b"ignat/test_dir/table1\nignat/test_dir/table2\n"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir/tab"]) == b"ignat/test_dir/table1\nignat/test_dir/table2\n"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir/table1"]) == b"ignat/test_dir/table1\n"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-exact", "ignat/test_dir/table1"]) == b"ignat/test_dir/table1\n"
        yt_cli.env["MR_TABLE_PREFIX"] = "ignat/"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-exact", "test_dir/table1"]) == b"test_dir/table1\n"
        del yt_cli.env["MR_TABLE_PREFIX"]
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-exact", "ignat/test_dir/table"]) == b""
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-exact", "ignat/test_dir"]) == b""
        completed_process = yt_cli.run(["mapreduce-yt", "-list", "-exact", "ignat/test_dir/", "-prefix", "ignat"])
        assert completed_process.returncode == 1
        assert b"prefix and exact options can not be specified simultaneously with enabled option use_yamr_style_prefix" in completed_process.stderr
        stdout = yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir/table", "-jsonoutput"])
        assert json.loads(stdout)[0]["name"] == "ignat/test_dir/table1"
        stdout = yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir/table", "-jsonoutput"])
        assert json.loads(stdout)[1]["name"] == "ignat/test_dir/table2"
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-exact", "ignat/test_dir/table", "-jsonoutput"]) == b"[]\n"

        del yt_cli.env["YT_IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST"]

        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", "ignat/test_dir"]) == b"ignat/test_dir/table1\nignat/test_dir/table2\nignat/test_dir/table3\n"

        del yt_cli.env["YT_USE_YAMR_STYLE_PREFIX"]

        yt_prefix = yt_cli.env["YT_PREFIX"]
        assert yt_cli.check_output(["mapreduce-yt", "-list", "-prefix", f"{yt_prefix}ignat/test_dir/"]) == b"table1\ntable2\ntable3\n"

    @authors("ilyaibraev")
    def test_codec(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))

        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/temp"])
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/temp", "-codec", "zlib_9", "-replicationfactor", "5"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/temp/@replication_factor"]) == b"5\n"

    @authors("ilyaibraev")
    def test_many_output_tables(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/temp", "-codec", "zlib_9", "-replicationfactor", "5"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        script_content = """#!/usr/bin/env python3
import os
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for descr in [3, 4, 5]:
        os.write(descr, '{0}\\\t{0}\\\t{0}\\\n'.format(descr).encode("utf-8"))
"""
        script_file_path = os.path.join(yt_cli.cwd, "many_output_mapreduce.py")
        with open(script_file_path, "w") as file:
            file.write(script_content)
        os.chmod(script_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        yt_cli.check_output(["mapreduce-yt", "-map", "./many_output_mapreduce.py", "-src", "ignat/temp", "-dst", "ignat/out1",
                             "-dst", "ignat/out2", "-dst", "ignat/out3", "-file", "many_output_mapreduce.py"])

        for i in range(1, 4):
            descr = i + 2
            assert yt_cli.check_output(["mapreduce-yt", "-read", f"ignat/out{i}"]) == f"{descr}\\\t{descr}\\\t{descr}\n".encode("utf-8")

    @authors("ilyaibraev")
    def test_chunksize(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/temp", "-chunksize", "1"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-get", "ignat/temp/@"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/temp/@chunk_count"]) == b"2\n"

    @authors("ilyaibraev")
    def test_mapreduce(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("big_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-src", "ignat/temp", "-dst", "ignat/reduced", "-map", "grep \"A\"", "-reduce", "awk '{a[$1]+=$3} END {for (i in a) {print i\"\t\t\"a[i]}}'"])
        assert lines_count(yt_cli.check_output(["mapreduce-yt", "-subkey", "-read", "ignat/reduced"])) == 10

    @authors("ilyaibraev")
    def test_input_output_format(self, yt_cli: YtCli):
        if "ENABLE_SCHEMA" in yt_cli.env:
            return
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        script_content = """#!/usr/bixn/env python3
import sys
if __name__ == '__main__':
    for line in sys.stdin:
        pass
    for i in [0, 1]:
        sys.stdout.write('k={0}\\ts={0}\\tv={0}\\n'.format(i))
"""
        script_path = os.path.join(yt_cli.cwd, "reformat.py")

        with open(script_path, "w") as file:
            file.write(script_content)
        os.chmod(script_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-outputformat", "dsv", "-map", "python3 reformat.py", "-file", "reformat.py", "-src", "ignat/temp", "-dst", "ignat/reformatted"])
        stdout = yt_cli.check_output(["mapreduce-yt", "-dsv", "-read", "ignat/reformatted"])
        for i in [0, 1]:
            for f in ["k", "s", "v"]:
                assert f"{f}={i}".encode("utf-8") in stdout

        yt_cli.check_output(["mapreduce-yt", "-format", "yson", "-write", "ignat/table"], stdin="{k=1;v=2}")
        assert yt_cli.check_output(["mapreduce-yt", "-format", "dsv", "-read", "ignat/table"]) == b"k=1\tv=2\n"

    @authors("ilyaibraev")
    def test_transactions(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        tx = yt_cli.check_output(["mapreduce-yt", "-starttx"]).decode("utf-8").strip()
        yt_cli.check_output(["mapreduce-yt", "-renewtx", tx])
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp", "-append", "-tx", tx], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-set", "ignat/temp/@my_attr", "-value", "10", "-tx", tx])

        completed_process = yt_cli.run(["mapreduce-yt", "-get", "ignat/temp/@my_attr"])
        assert completed_process.returncode == 1
        assert b"Attribute \"my_attr\" is not found" in completed_process.stderr
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp"]) == b'4\t6\n1\t3\n'
        yt_cli.check_output(["mapreduce-yt", "-committx", tx])

        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/temp/@my_attr"]) == b"10\n"
        assert lines_count(yt_cli.check_output(["mapreduce-yt", "-read", "ignat/temp"])) == 4

    @authors("ilyaibraev")
    def test_range_map(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-map", "awk \'{sum+=$1+$2} END {print \"\t\"sum}\'", "-src", "ignat/temp{key,value}", "-dst", "ignat/sum"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/sum"]) == b"\t14\n"

    @authors("ilyaibraev")
    def test_uploaded_files(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))

        script_content = """#!/usr/bin/env python3
import sys
if __name__ == '__main__':
    for line in sys.stdin:
        pass
    for i in [0, 1, 2, 3, 4]:
        sys.stdout.write('{0}\\t{1}\\t{2}\\n'.format(i, i * i, i * i * i))
"""
        script_path = os.path.join(yt_cli.cwd, "my_mapper.py")
        with open(script_path, "w") as file:
            file.write(script_content)
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/dir/mapper.py"])

        assert yt_cli.check_output(["mapreduce-yt", "-listfiles"]) == b""
        initial_number_of_files = 0

        yt_cli.check_output(["mapreduce-yt", "-upload", "ignat/dir/mapper.py", "-executable"], stdin=read_file_as_string(script_path))
        yt_cli.check_output(["mapreduce-yt", "-upload", "ignat/dir/mapper.py", "-executable"], stdin=read_file_as_string(script_path))

        assert yt_cli.check_output(["mapreduce-yt", "-download", "ignat/dir/mapper.py"]).decode("utf-8") == script_content

        final_number_of_lines = lines_count(yt_cli.check_output(["mapreduce-yt", "-listfiles"]))
        assert initial_number_of_files + 1 == final_number_of_lines

        yt_cli.check_output(["mapreduce-yt", "-subkey", "-map", "./mapper.py", "-ytfile", "ignat/dir/mapper.py", "-src", "ignat/temp", "-dst", "ignat/mapped"])
        assert lines_count(yt_cli.check_output(["mapreduce-yt", "-subkey", "-read", "ignat/mapped"])) == 5

    @authors("ilyaibraev")
    def test_ignore_positional_arguments(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-list", "", "123"])

    @authors("ilyaibraev")
    def test_stderr(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/temp"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        completed_process = yt_cli.run(["mapreduce-yt", "-subkey", "-map", "cat &>2 && exit(1)", "-src", "ignat/temp", "-dst", "ignat/tmp"])
        assert completed_process.returncode == 1

    @authors("ilyaibraev")
    def test_spec(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/input"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-map", "cat >/dev/null; echo -e \"${YT_OPERATION_ID}\t\"",
                             "-ytspec", "{\"opt1\": 10, \"opt2\": {\"$attributes\": {\"my_attr\": \"ignat\"}, \"$value\": 0.5}}",
                             "-src", "ignat/input", "-dst", "ignat/output"])

        op_id = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]).decode("utf-8").strip()
        op_dir = op_id[-2:]
        op_path = f"//sys/operations/{op_dir}/{op_id}"
        assert yt_cli.check_output(["mapreduce-yt", "-get", f"{op_path}/@spec/opt1"]) == b"10\n"
        stdout = yt_cli.check_output(["mapreduce-yt", "-get", f"{op_path}/@spec/opt2"])
        assert json.loads(stdout)["$value"] == 0.5

        yt_cli.env["YT_SPEC"] = "{\"opt3\": \"hello\", \"opt4\": {\"$attributes\": {}, \"$value\": null}}"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat >/dev/null; echo -e \"${YT_OPERATION_ID}\t\"", "-src", "ignat/input", "-dst", "ignat/output"])

        op_id = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]).decode("utf-8").strip()
        op_dir = op_id[-2:]
        op_path = f"//sys/operations/{op_dir}/{op_id}"
        assert yt_cli.check_output(["mapreduce-yt", "-get", f"{op_path}/@spec/opt3"]) == b"\"hello\"\n"
        assert yt_cli.check_output(["mapreduce-yt", "-get", f"{op_path}/@spec/opt4"]) == b"null\n"
        yt_cli.env["YT_USE_YAMR_DEFAULTS"] = "1"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat >/dev/null; echo -e \"${YT_OPERATION_ID}\t\"",
                             "-ytspec", "{\"mapper\": {\"memory_limit\": 1234567890}}", "-src", "ignat/input", "-dst", "ignat/output"])

        op_id = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]).decode("utf-8").strip()
        op_dir = op_id[-2:]
        op_path = f"//sys/operations/{op_dir}/{op_id}"
        assert yt_cli.check_output(["mapreduce-yt", "-get", f"{op_path}/@spec/mapper/memory_limit"]) == b"1234567890\n"

    @authors("ilyaibraev")
    def test_smart_format(self, yt_cli: YtCli):
        if "ENABLE_SCHEMA" in yt_cli.env:
            return
        format = """{
    "$value":"yamred_dsv",
    "$attributes":{
        "key_column_names":["x","y"],
        "subkey_column_names":["subkey"],
        "has_subkey":"true"
    }
}"""
        same_format_with_other_repr = """{
    "$value":"yamred_dsv",
    "$attributes":{
        "key_column_names":["x","y"],
        "subkey_column_names":["subkey"],
        "has_subkey":"true"
    }
}"""
        yt_cli.env["YT_SMART_FORMAT"] = "1"
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/smart_x"])
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/smart_z"])
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/smart_w"])

        yt_cli.check_output(["mapreduce-yt", "-set", "ignat/smart_x/@_format", "-value", format])
        yt_cli.check_output(["mapreduce-yt", "-set", "ignat/smart_z/@_format", "-value", format])
        yt_cli.check_output(["mapreduce-yt", "-set", "ignat/smart_w/@_format", "-value", same_format_with_other_repr])
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/smart_x"], stdin="1 2\t\tz=10\n")
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/smart_w"], stdin="3 4\t\tz=20\n")
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x"]) == b"1 2\tz=10\n"
        assert yt_cli.check_output(["mapreduce-yt", "-lenval", "-read", "ignat/smart_x"]) == b"\x03\x00\x00\x001 2\x04\x00\x00\x00z=10"
        assert yt_cli.check_output(["mapreduce-yt", "-subkey", "-read", "ignat/smart_x"]) == b"1 2\t\tz=10\n"
        ranged_table = "ignat/smart_x{x,z}"
        completed_process = yt_cli.run(["mapreduce-yt", "-read", ranged_table])
        assert completed_process.returncode == 1
        assert b"Key column \"y\" is missing" in completed_process.stderr
        assert yt_cli.check_output(["mapreduce-yt", "-read", ranged_table, "-dsv"]) == b"x=1\tz=10\n"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "ignat/smart_x", "-src", "ignat/smart_w", "-dst", "ignat/output"])
        yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output", "-dsv"])
        yt_cli.check_output(["mapreduce-yt", "-sort", "-src", "ignat/output", "-dst", "ignat/output"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output{key}", "-dsv"]) == b"key=1 2\nkey=3 4\n"
        del yt_cli.env["YT_SMART_FORMAT"]
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/smart_y"], stdin="1 2\t\tz=10\n")
        yt_cli.check_output(["mapreduce-yt", "-smartformat", "-map", "cat", "-src", "ignat/smart_y", "-src", "fake", "-dst", "ignat/smart_x"])
        assert yt_cli.check_output(["mapreduce-yt", "-smartformat", "-read", "ignat/smart_x"]) == b"1 2\tz=10\n"
        completed_process = yt_cli.run(["mapreduce-yt", "-smartformat", "-map", "cat", "-src", "ignat/smart_y", "-src", "fake", "-dst", "ignat/smart_x", "-dst", "ignat/some_table"])
        assert completed_process.returncode == 1
        assert b"Tables have different attribute _format:" in completed_process.stderr
        yt_cli.check_output(["mapreduce-yt", "-smartformat", "-map", "cat", "-src", "ignat/smart_y", "-src", "fake", "-dst", "ignat/smart_x", "-dst", "ignat/smart_z", "-outputformat", "yamr"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x"]) == b"1 2\tz=10\n"
        completed_process = yt_cli.run(["mapreduce-yt", "-smartformat", "-read", "ignat/smart_x"])
        assert completed_process.returncode == 1
        assert b"yt.common.YtResponseError: Key column \"x\" is missing" in completed_process.stderr
        yt_cli.env["YT_SMART_FORMAT"] = "1"
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/smart_x"], stdin="1 2\t\tz=10\n")
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/smart_x", "-dst", "ignat/smart_z"])
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "ignat/smart_x", "-src", "ignat/smart_z", "-dst", "ignat/smart_z"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_z"]) == b"1 2\tz=10\n1 2\tz=10\n"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-reduce", "cat", "-src", "ignat/smart_x", "-dst", "ignat/smart_y"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_y"]) == b"1 2\tz=10\n"
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/smart_x", "-append"], stdin="1 1\t\n")
        yt_cli.check_output(["mapreduce-yt", "-sort", "-src", "ignat/smart_x", "-dst", "ignat/smart_x"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x"]) == b"1 1\t\n1 2\tz=10\n"
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/smart_x", "-append"], stdin="1 2\t\tz=1\n")
        yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x", "-dsv"])
        yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/smart_x"]) == b"1 1\t\n1 2\tz=10\n1 2\tz=1\n"
        yt_cli.check_output(["mapreduce-yt", "-reduce", "tr '=' ' ' | awk '{sum+=$4} END {print sum \"\t\"}'", "-src", "ignat/smart_x", "-dst", "ignat/output", "-jobcount", "2"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]) == b"11\t\n"

    @authors("ilyaibraev")
    def test_drop(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-subkey", "-write", "ignat/xxx/yyy/zzz"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/xxx/yyy/zzz"])
        completed_process = yt_cli.run(["mapreduce-yt", "-get", "ignat/xxx"])
        assert completed_process.returncode == 1
        assert b"Error resolving path //home/wrapper_tests/ignat/xxx" in completed_process.stderr

    @authors("ilyaibraev")
    def test_create_table(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/empty_table"])
        yt_cli.check_output(["mapreduce-yt", "-set", "ignat/empty_table/@xxx", "-value", "\"my_value\""])
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/empty_table", "-append"], stdin="x\ty\n")
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/empty_table/@xxx"]) == b"\"my_value\"\n"

    @authors("ilyaibraev")
    def test_do_not_delete_empty_table(self, yt_cli: YtCli):
        yt_cli.env["YT_DELETE_EMPTY_TABLES"] = "0"
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/empty_table"])
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/empty_table"])
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/empty_table"], stdin=subprocess.DEVNULL)
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/empty_table/@row_count"]) == b"0\n"
        del yt_cli.env["YT_DELETE_EMPTY_TABLES"]

    @authors("ilyaibraev")
    def test_sortby_reduceby(self, yt_cli: YtCli):
        if "ENABLE_SCHEMA" in yt_cli.env:
            return
        script_reducer_content = """#!/usr/bin/env python3
import sys
from itertools import groupby, starmap

def parse(line):
    d = dict(x.split('=') for x in line.split())
    return (d['c3'], d['c2'])

def aggregate(key, recs):
    recs = list(recs)
    for i in range(len(recs) - 1):
        assert recs[i][1] <= recs[i + 1][1]
    return key, sum(map(lambda x: int(x[1]), recs))

if __name__ == '__main__':
    recs = map(parse, sys.stdin.readlines())
    for key, num in starmap(aggregate, groupby(recs, lambda rec: rec[0])):
        print('c3=%s  c2=%d' % (key, num))
"""
        script_reducer_path = os.path.join(yt_cli.cwd, "my_reducer.py")
        with open(script_reducer_path, "w") as file:
            file.write(script_reducer_content)
        script_order_content = """#!/usr/bin/env python3
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print('\t'.join(k + '=' + v for k, v in sorted(x.split('=') for x in line.split())))
"""
        script_order_path = os.path.join(yt_cli.cwd, "order.py")
        with open(script_order_path, "w") as file:
            file.write(script_order_content)
        stdin = "c1=1\tc2=2\tc3=z\n"\
                "c1=1\tc2=3\tc3=x\n"\
                "c1=2\tc2=2\tc3=x\n"
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "ignat/test_table"], stdin=stdin)
        yt_cli.check_output(["mapreduce-yt", "-sort", "-src", "ignat/test_table", "-dst", "ignat/sorted_table", "-sortby", "c3", "-sortby", "c2"])
        yt_cli.check_output(["mapreduce-yt", "-reduce", "./my_reducer.py", "-src", "ignat/sorted_table{c3,c2}", "-dst", "ignat/reduced_table", "-reduceby", "c3", "-file", "my_reducer.py", "-dsv"])
        stdin_local = "c3=x\tc2=5\nc3=z\tc2=2\n"
        stdin_mr_yt = yt_cli.check_output(["mapreduce-yt", "-read", "ignat/reduced_table", "-dsv"]).decode("utf-8")
        os.chmod(script_order_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        assert yt_cli.check_output(["./order.py"], stdin=stdin_mr_yt) == yt_cli.check_output(["./order.py"], stdin=stdin_local)
        os.remove(script_order_path)
        os.remove(script_reducer_path)
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "<sorted_by=[a]>ignat/test_table"], stdin="a=1\tb=2\na=1\tb=1\n")
        yt_cli.check_output(["mapreduce-yt", "-reduce", "cat", "-src", "ignat/test_table", "-dst", "ignat/reduced_table", "-reduceby", "a", "-dsv"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/reduced_table", "-dsv"]) == b"a=1\tb=2\na=1\tb=1\n"
        yt_cli.check_output(["mapreduce-yt", "-reduce", "cat", "-src", "ignat/test_table", "-dst", "ignat/reduced_table", "-reduceby", "a", "-sortby", "a", "-sortby", "b", "-dsv"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/reduced_table", "-dsv"]) == b"a=1\tb=1\na=1\tb=2\n"

        yt_cli.check_output(["mapreduce-yt", "-reduce", "cat", "-src", "ignat/test_table", "-dst", "ignat/reduced_table", "-reduceby", "b", "-sortby", "b", "-dsv"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/reduced_table", "-dsv"]) == b"b=1\ta=1\nb=2\ta=1\n"

    @authors("ilyaibraev")
    def test_empty_destination(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/empty_table"], stdin=subprocess.DEVNULL)

    @authors("ilyaibraev")
    def test_dsv_reduce(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "ignat/empty_table"])
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-reduce", "cat", "-reduceby", "x", "-src", "ignat/empty_table", "-dst", "ignat/empty_table"])

    @authors("ilyaibraev")
    def test_slow_write(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/some_table"])
        process = yt_cli.run_in_background(["mapreduce-yt", "-write", "ignat/some_table", "-timeout", "2000"])
        time.sleep(1)
        stdout, stderr = process.communicate(b"a\tb\n")
        assert process.returncode == 0
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b"a\tb\n"
        process = yt_cli.run_in_background(["mapreduce-yt", "-write", "ignat/some_table", "-timeout", "1000"])
        time.sleep(5)
        stdout, stderr = process.communicate(b"a\tb\n")
        assert process.returncode == 0
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b"a\tb\n"

    @authors("ilyaibraev")
    def test_many_dst_write(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "-dst", "ignat/A", "-dst", "ignat/B"], stdin="a\tb\n1\nc\td\n0\ne\tf")
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/A"]) == b"a\tb\ne\tf\n"
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/B"]) == b"c\td\n"

    @authors("ilyaibraev")
    def test_dstsorted(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-writesorted", "ignat/some_table"], stdin="x\t10\ny\t15\n")
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/some_table/@sorted"]) == b"true\n"
        yt_cli.check_output(["mapreduce-yt", "-reduce", "grep x", "-src", "ignat/some_table", "-dstsorted", "ignat/some_table"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/some_table/@sorted"]) == b"true\n"
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/some_table"], stdin="x\t10\ny\t15")
        yt_cli.check_output(["mapreduce-yt", "-reduce", "grep x", "-src", "ignat/some_table", "-dstsorted", "ignat/some_table"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/some_table/@sorted"]) == b"true\n"
        completed_process = yt_cli.run(["mapreduce-yt", "-map", "cat", "-src", "ignat/some_table", "-dstsorted", "ignat/some_table", "-format", "dsv"])
        assert completed_process.returncode == 1
        assert b"Output format must be yamr or yamred_dsv if dstsorted is non-trivial" in completed_process.stderr

    @authors("ilyaibraev")
    def test_custom_fs_rs(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-fs", " ", "-write", "ignat/some_table"], stdin="x y z")
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b"x\ty z\n"

    @authors("ilyaibraev")
    def test_write_with_tx(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/some_table"])
        tx = yt_cli.check_output(["mapreduce-yt", "-starttx"]).decode().strip()
        process = yt_cli.run_in_background(["mapreduce-yt", "-write", "ignat/some_table", "-tx", tx])
        time.sleep(1)
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b""
        time.sleep(1)
        process.communicate(b"a\tb")
        time.sleep(1)
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b""
        yt_cli.check_output(["mapreduce-yt", "-committx", tx])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b"a\tb\n"

    @authors("ilyaibraev")
    def test_table_file(self, yt_cli: YtCli):
        if "ENABLE_SCHEMA" in yt_cli.env:
            return
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "ignat/input"], stdin="x=0\n")
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "ignat/dictionary"], stdin="field=10\n")
        yt_cli.check_output(["mapreduce-yt", "-map", "cat >/dev/null; cat dictionary", "-dsv", "-src", "ignat/input", "-dst", "ignat/output", "-ytfile", "<format=dsv>ignat/dictionary"])
        assert yt_cli.check_output(["mapreduce-yt", "-dsv", "-read", "ignat/output"]) == b"field=10\n"

    @authors("ilyaibraev")
    def test_unexisting_input_tables(self, yt_cli: YtCli):
        if "ENABLE_SCHEMA" in yt_cli.env:
            return
        yt_cli.check_output(["mapreduce-yt", "-dsv", "-write", "ignat/output"], stdin="x=0\n")
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "ignat/unexisting1", "-src", "ignat/unexisting2", "-dst", "ignat/output"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]) == b""

    @authors("ilyaibraev")
    def test_copy_files(self, yt_cli: YtCli):
        test_file_content = "MY CONTENT\n"
        yt_cli.check_output(["mapreduce-yt", "-upload", "ignat/my_file"], stdin=test_file_content)
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/my_file", "-dst", "ignat/other_file"])
        assert yt_cli.check_output(["mapreduce-yt", "-download", "ignat/other_file"]) == test_file_content.encode("utf-8")

    @authors("ilyaibraev")
    def test_write_lenval(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-lenval", "-write", "ignat/lenval_table"], stdin="\x01\x00\x00\x00a\x01\x00\x00\x00b")
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/lenval_table"]) == b"a\tb\n"

    @authors("ilyaibraev")
    def test_force_drop(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/some_table", "-force"])
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/some_table"])
        process = yt_cli.run_in_background(["mapreduce-yt", "-append", "-write", "ignat/some_table"])
        time.sleep(2)
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/some_table", "-force"])
        time.sleep(2)
        stdout, stderr = process.communicate(input="x\ty".encode("utf-8"))
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/some_table"]) == b""
        process.kill()

    @authors("ilyaibraev")
    def test_parallel_dstappend(self, yt_cli: YtCli):
        def run_op():
            return yt_cli.run_in_background(["timeout", "25", *yt_cli.replace["mapreduce-yt"], "-map", "cat", "-src", "ignat/table", "-dstappend", "ignat/output_table"])

        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/table"], stdin="x\t10")
        sync_path = os.path.join(yt_cli.cwd, "sync_file")
        with open(sync_path, "w") as _:
            pass
        yt_cli.check_output(["mapreduce-yt", "-createtable", "ignat/output_table"])
        process_op_1 = run_op()
        process_op_2 = run_op()

        wait(lambda: process_op_1.poll() is not None and process_op_2.poll() is not None, timeout=30)
        assert process_op_1.returncode == 0 and process_op_2.returncode == 0
        assert lines_count(yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output_table"])) == 2

    @authors("ilyaibraev")
    def test_many_to_many_copy_move(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/in1"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/in2"], stdin=read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-move", "-src", "ignat/in1", "-dst", "ignat/out1", "-src", "ignat/in2", "-dst", "ignat/out2"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/in1"]) == b""
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/in2"]) == b""
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/out1"]) == b"4\t5\t6\n1\t2\t3\n"
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/out2"]) == b"4\t5\t6\n1\t2\t3\n"
        yt_cli.check_output(["mapreduce-yt", "-copy", "-src", "ignat/out1", "-dst", "ignat/in1", "-src", "ignat/out2", "-dst", "ignat/in2"])
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/in1"]) == b"4\t5\t6\n1\t2\t3\n"
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/in2"]) == b"4\t5\t6\n1\t2\t3\n"

    @authors("ilyaibraev")
    def test_missing_prefix(self, yt_cli: YtCli):
        if "YT_PREFIX" in yt_cli.env:
            del yt_cli.env["YT_PREFIX"]
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/@key"]) == b"\"tmp\"\n"

    @authors("ilyaibraev")
    def test_table_record_index(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-writesorted", "ignat/tableA"], stdin="a\t1")
        yt_cli.check_output(["mapreduce-yt", "-writesorted", "ignat/tableB"], stdin="b\t2")
        tempfile = os.path.join(yt_cli.cwd, "test_mapreduce_binary")
        with open(tempfile, "w") as _:
            pass
        os.chmod(tempfile, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        yt_cli.check_output(["mapreduce-yt", "-reduce", f"cat > {tempfile}", "-src", "ignat/tableA", "-src", "ignat/tableB", "-dst", "ignat/dst", "-tablerecordindex"])
        assert read_file_as_string(tempfile) == "0\n0\na\t1\n1\n0\nb\t2\n"

    @authors("ilyaibraev")
    def test_opts(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-write", "tmp/input"], read_file_as_string("table_file", yt_cli.cwd))
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "tmp/input", "-dst", "tmp/output"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/output/@chunk_count"]) == b"1\n"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "tmp/input", "-dst", "tmp/output", "-opt", "jobcount=2"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/output/@chunk_count"]) == b"2\n"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "tmp/input", "-dst", "tmp/output", "-opt", "cpu.intensive.mode=1"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/output/@chunk_count"]) == b"2\n"
        yt_cli.env["MR_OPT"] = "cpu.intensive.mode=1"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "tmp/input", "-dst", "tmp/output"])
        del yt_cli.env["MR_OPT"]
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/output/@chunk_count"]) == b"2\n"
        yt_cli.env["MR_OPT"] = "cpu.intensive.mode=1"
        yt_cli.check_output(["mapreduce-yt", "-map", "cat", "-src", "tmp/input", "-dst", "tmp/output", "-opt", "cpu.intensive.mode=0"])
        del yt_cli.env["MR_OPT"]
        assert yt_cli.check_output(["mapreduce-yt", "-get", "tmp/output/@chunk_count"]) == b"1\n"

    @authors("ilyaibraev")
    def test_defrag(self, yt_cli: YtCli):
        yt_cli.check_output(["mapreduce-yt", "-writesorted", "ignat/input"], stdin="a\t1\n")
        yt_cli.check_output(["mapreduce-yt", "-append", "-writesorted", "ignat/input"], stdin="b\t2\n")
        for defrag in ["", "full"]:
            if defrag:
                yt_cli.check_output(["mapreduce-yt", "-defrag", defrag, "-src", "ignat/input", "-dst", "ignat/output"])
            else:
                yt_cli.check_output(["mapreduce-yt", "-defrag", "-src", "ignat/input", "-dst", "ignat/output"])

            assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/output"]) == b"a\t1\nb\t2\n"
            assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/output/@sorted"]) == b"true\n"
            assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/output/@chunk_count"]) == b"1\n"

    @authors("ilyaibraev")
    def test_archive_and_transform(self, yt_cli: YtCli):
        tempfile = os.path.join(yt_cli.cwd, "test_mapreduce_binary_config")
        with open(tempfile, "w") as file:
            file.write("{transform_options={desired_chunk_size=10000000}}\n")
        yt_cli.env["YT_CONFIG_PATH"] = tempfile
        yt_cli.check_output(["mapreduce-yt", "-write", "ignat/input"], stdin="a\t1\nb\t2")
        yt_cli.check_output(["mapreduce-yt", "-archive", "ignat/input", "-erasurecodec", "none"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/input/@compression_codec"]) == b"\"zlib_9\"\n"
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/input/@erasure_codec"]) == b"\"none\"\n"
        yt_cli.check_output(["mapreduce-yt", "-unarchive", "ignat/input"])
        yt_cli.check_output(["mapreduce-yt", "-transform", "-src", "ignat/input", "-dst", "ignat/output", "-codec", "zlib_6"])
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/output/@compression_codec"]) == b"\"zlib_6\"\n"
        assert yt_cli.check_output(["mapreduce-yt", "-get", "ignat/output/@erasure_codec"]) == b"\"none\"\n"

    @authors("ilyaibraev")
    def test_create_tables_under_transaction_option(self, yt_cli: YtCli):
        echo_script_content = """#!/usr/bin/env python3
from time import sleep
import sys

if __name__ == '__main__':
    print("a\tb", file=sys.stdout)
    sleep(10.0)
    print("x\ty", file=sys.stdout)
"""
        echo_script_path = os.path.join(yt_cli.cwd, "echo.py")
        with open(echo_script_path, "w") as file:
            file.write(echo_script_content)
        os.chmod(echo_script_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/xxx_some_table", "-force"])
        assert yt_cli.check_output(["mapreduce-yt", "-exists", "ignat/xxx_some_table"]) == b"false\n"
        echo_process = yt_cli.run_in_background([echo_script_path])
        process = yt_cli.run_in_background(["mapreduce-yt", "-append", "-write", "ignat/xxx_some_table"], stdin=echo_process.stdout)
        time.sleep(5)
        assert yt_cli.check_output(["mapreduce-yt", "-exists", "ignat/xxx_some_table"]) == b"true\n"
        time.sleep(7)
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/xxx_some_table"]) == b"a\tb\nx\ty\n"
        yt_cli.env["YT_CONFIG_PATCHES"] = "{yamr_mode={create_tables_outside_of_transaction=%false}};" + yt_cli.env["YT_CONFIG_PATCHES"]
        yt_cli.check_output(["mapreduce-yt", "-drop", "ignat/xxx_some_table2", "-force"])
        assert yt_cli.check_output(["mapreduce-yt", "-exists", "ignat/xxx_some_table2"]) == b"false\n"
        echo_process = yt_cli.run_in_background([echo_script_path])
        process.kill()
        process = yt_cli.run_in_background(["mapreduce-yt", "-append", "-write", "ignat/xxx_some_table2"], stdin=echo_process.stdout)
        time.sleep(5)
        assert yt_cli.check_output(["mapreduce-yt", "-exists", "ignat/xxx_some_table2"]) == b"false\n"
        time.sleep(7)
        assert yt_cli.check_output(["mapreduce-yt", "-read", "ignat/xxx_some_table2"]) == b"a\tb\nx\ty\n"
        process.kill()
