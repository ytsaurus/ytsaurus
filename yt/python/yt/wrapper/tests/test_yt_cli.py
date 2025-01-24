from __future__ import print_function

import stat

from .conftest import authors
from .helpers import get_tests_sandbox, wait, get_environment_for_binary_test
from .helpers_cli import YtCli

from yt import yson

from yt.testlib.test_environment import YtTestEnvironment

from yt.common import makedirp

import yt.wrapper as yt


from flaky import flaky

import os
import pytest
import random
import string
import time
import uuid


@pytest.fixture()
def yt_cli(request: pytest.FixtureRequest, yt_env_job_archive: YtTestEnvironment):
    yt.create("map_node", "//home/wrapper_test", ignore_existing=True, recursive=True)
    env = get_environment_for_binary_test(yt_env_job_archive)
    env["FALSE"] = "%false"
    env["TRUE"] = "%true"

    sandbox_root: str = get_tests_sandbox()  # type: ignore

    test_name = request.node.name
    sandbox_dir = os.path.join(sandbox_root, f"TestYtBinary_{test_name}_" + uuid.uuid4().hex[:8])
    makedirp(sandbox_dir)
    replace = {
        "yt": [env["PYTHON_BINARY"], env["YT_CLI_PATH"]]
    }
    yield YtCli(env, sandbox_dir, replace)

    yt.remove("//home/wrapper_test", force=True, recursive=True)
    if "script.sh" in os.listdir("."):
        os.remove("script.sh")


def read_file_as_string(file_path: str, cwd: str | None = None):
    """if cwd is not specified, file_path is expected to be an absolute path"""
    path = os.path.join(cwd, file_path) if cwd else file_path
    with open(path, "r") as file:
        return "".join(file.readlines())


@pytest.mark.timeout(1200)
@flaky(max_runs=3)
@pytest.mark.usefixtures("yt_env_job_archive")
class TestYtBinary(object):
    @authors("ilyaibraev")
    def test_cypress_commands(self, yt_cli: YtCli):
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test"]) == b""
        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--name", "xxx"]) == b""

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/folder", "{}"])
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test/folder"]) == b""
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test"]) == b"folder\n"
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test", "--read-from", "cache"]) == b"folder\n"
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test", "--format", "json"]) == b"[\"folder\"]"
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test", "--format", "<format=text>yson"]) == b"{\"folder\"={};}\n"

        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--name", "xxx"]) == b""
        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--name", "folder"]) == b"//home/wrapper_test/folder\n"
        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--name", "folder", "--read-from", "cache"]) == b"//home/wrapper_test/folder\n"
        yt_cli.env["YT_PREFIX"] = "//home/"
        assert yt_cli.check_output(["yt", "find", "wrapper_test", "--name", "folder"]) == b"//home/wrapper_test/folder\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/folder/@attr", "<a=b>c"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/folder/@attr", "--format", "<format=text>yson"]) == b"<\"a\"=\"b\";>\"c\"\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/folder/@attr", "{\"attr\": 10}", "--format", "json"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/folder/@attr", "--format", "json"]) == b"{\"attr\":10}\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/other_folder/my_dir", "{}", "--recursive", "--force"])
        assert yt_cli.check_output(["yt", "exists", "//home/wrapper_test/other_folder/my_dir"]) == b"true\n"

        yt_cli.check_output(["yt", "create", "file", "//home/wrapper_test/file_with_attrs", "--attributes", "{testattr=1;other=2}", "--ignore-existing"])
        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--attribute-filter", "testattr=1"]) == b"//home/wrapper_test/file_with_attrs\n"
        assert yt_cli.check_output(["yt", "find", "//home/wrapper_test", "--attribute-filter", "attr=1"]) == b""

    @authors("ilyaibraev")
    def test_create_account(self, yt_cli: YtCli):

        yt_cli.check_output(["yt", "create-account", "parent"])
        assert yt_cli.check_output(["yt", "exists", "//sys/account_tree/parent"]) == b"true\n"
        yt_cli.check_output(["yt", "create-account", "parent", "-i"])

        assert yt_cli.check_output(["yt", "get", "//sys/account_tree/parent/@resource_limits/node_count"]) == b"0\n"
        yt_cli.check_output(["yt", "set", "//sys/account_tree/parent/@resource_limits/node_count", "10"])
        yt_cli.check_output(["yt", "create-account", "--parent-name", "parent", "--name", "child", "--resource-limits", "{node_count=10}", "--allow-children-limit-overcommit"])
        assert yt_cli.check_output(["yt", "exists", "//sys/account_tree/parent/child"]) == b"true\n"

        assert yt_cli.check_output(["yt", "get", "//sys/account_tree/parent/child/@resource_limits/node_count"]) == b"10\n"
        assert yt_cli.check_output(["yt", "get", "//sys/account_tree/parent/child/@allow_children_limit_overcommit"]) == b"%true\n"

    @authors("ilyaibraev")
    def test_create_pool(self, yt_cli: YtCli):

        yt_cli.check_output(["yt", "create-pool", "test"])
        assert yt_cli.check_output(["yt", "exists", "//sys/pool_trees/default/test"]) == b"true\n"
        yt_cli.check_output(["yt", "create-pool", "test", "-i"])

        yt_cli.check_output(["yt", "create", "scheduler_pool_tree", "--attributes", "{name=yggdrasil}"])

        yt_cli.check_output(["yt", "create-pool", "parent", "yggdrasil", "--resource-limits", "{cpu=10; memory=1000000000}", "--min-share-resources", "{cpu=5}"])
        assert yt_cli.check_output(["yt", "exists", "//sys/pool_trees/yggdrasil/parent"]) == b"true\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/@resource_limits/cpu"]) == b"10\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/@resource_limits/memory"]) == b"1000000000\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/@min_share_resources/cpu"]) == b"5\n"

        yt_cli.check_output(
            [
                "yt", "create-pool", "--parent-name", "parent", "--pool-tree", "yggdrasil", "--name", "fair-share-child", "--weight",
                "3.14", "--create-ephemeral-subpools", "--max-operation-count", "10", "--max-running-operation-count", "5", "--attributes", "{attr=value}"
            ]
        )
        assert yt_cli.check_output(["yt", "exists", "//sys/pool_trees/yggdrasil/parent/fair-share-child"]) == b"true\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fair-share-child/@weight"]) == b"3.14\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fair-share-child/@max_operation_count"]) == b"10\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fair-share-child/@max_running_operation_count"]) == b"5\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fair-share-child/@attr"]) == b"\"value\"\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fair-share-child/@create_ephemeral_subpools"]) == b"%true\n"

        yt_cli.check_output(
            [
                "yt", "create-pool", "fifo-child", "--parent-name", "parent", "--pool-tree", "yggdrasil", "--mode",
                "fifo", "--fifo-sort-parameters", "[pending_job_count]", "--ephemeral-subpool-config", "{max_operation_count=10}", "--forbid-immediate-operations"
            ]
        )

        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fifo-child/@mode"]) == b"\"fifo\"\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fifo-child/@fifo_sort_parameters/0"]) == b"\"pending_job_count\"\n"
        assert yt_cli.check_output(["yt", "exists", "//sys/pool_trees/yggdrasil/parent/fifo-child/@fifo_sort_parameters/1"]) == b"false\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fifo-child/@forbid_immediate_operations"]) == b"%true\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/yggdrasil/parent/fifo-child/@ephemeral_subpool_config/max_operation_count"]) == b"10\n"

        yt_cli.check_output(["yt", "remove", "//sys/pool_trees/yggdrasil", "--force", "--recursive"])

    @authors("ilyaibraev")
    def test_list_long_format(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "list", "-l", "//home"])
        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/folder_with_symlinks/test_table", "--recursive"])
        yt_cli.check_output(["yt", "link", "//home/wrapper_test/folder_with_symlinks/test_table", "//home/wrapper_test/folder_with_symlinks/valid_link"])
        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table_to_delete"])
        yt_cli.check_output(["yt", "link", "//home/wrapper_test/table_to_delete", "//home/wrapper_test/folder_with_symlinks/invalid_link"])
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/table_to_delete"])
        yt_cli.check_output(["yt", "list", "-l", "//home/wrapper_test/folder_with_symlinks"])

    @authors("ilyaibraev")
    def test_concatenate(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "write-file", "//home/wrapper_test/file_a"], stdin="Hello\n")
        yt_cli.check_output(["yt", "write-file", "//home/wrapper_test/file_b"], stdin="World\n")
        yt_cli.check_output(["yt", "concatenate", "--src", "//home/wrapper_test/file_a", "--src", "//home/wrapper_test/file_b", "--dst", "//home/wrapper_test/output_file"])
        assert yt_cli.check_output(["yt", "read-file", "//home/wrapper_test/output_file"]) == b"Hello\nWorld\n"

    @authors("ilyaibraev")
    def test_table_commands(self, yt_cli: YtCli):

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/test_table"])
        assert yt_cli.check_output(["yt", "read", "//home/wrapper_test/test_table", "--format", "dsv"]) == b""

        yt_cli.check_output(["yt", "write", "//home/wrapper_test/test_table", "--format", "dsv"], stdin="value=y\nvalue=x\n")
        assert yt_cli.check_output(["yt", "read", "//home/wrapper_test/test_table", "--format", "dsv"]) == b"value=y\nvalue=x\n"

    @authors("ilyaibraev")
    def test_file_commands(self, yt_cli: YtCli):
        script_content = "grep x"
        script_file_path = os.path.join(yt_cli.cwd, "script")
        with open(script_file_path, "w") as script_file:
            script_file.write(script_content)
        os.chmod(script_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        yt_cli.check_output(["yt", "upload", "//home/wrapper_test/script", "--executable"], stdin=script_content)
        assert yt_cli.check_output(["yt", "download", "//home/wrapper_test/script"]) == b"grep x"
        yt_cli.check_output(["yt", "write", "//home/wrapper_test/input_table", "--format", "dsv"], stdin="value=y\nvalue=x\n")
        yt_cli.check_output(["yt", "map", "./script", "--src", "//home/wrapper_test/input_table", "--dst", "//home/wrapper_test/output_table",
                            "--file", "//home/wrapper_test/script", "--format", "dsv"])
        assert yt_cli.check_output(["yt", "read", "//home/wrapper_test/output_table", "--format", "dsv"]) == b"value=x\n"
        yt_cli.check_output(["yt", "map", "./script", "--src", "//home/wrapper_test/input_table", "--dst", "//home/wrapper_test/output_table", "--local-file", "script", "--format", "dsv"])
        assert yt_cli.check_output(["yt", "read", "//home/wrapper_test/output_table", "--format", "dsv"]) == b"value=x\n"
        os.remove(script_file_path)

    @authors("ilyaibraev")
    def test_copy_move_link(self, yt_cli: YtCli):

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table"])
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test"]) == b"table\n"

        yt_cli.check_output(["yt", "copy", "//home/wrapper_test/table", "//home/wrapper_test/other_table"])
        assert sorted(yt_cli.check_output(["yt", "list", "//home/wrapper_test"]).decode("utf-8").strip().split("\n")) == ["other_table", "table"]

        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/table"])
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test"]) == b"other_table\n"

        yt_cli.check_output(["yt", "move", "//home/wrapper_test/other_table", "//home/wrapper_test/table"])
        assert yt_cli.check_output(["yt", "list", "//home/wrapper_test"]) == b"table\n"

        yt_cli.check_output(["yt", "link", "//home/wrapper_test/table", "//home/wrapper_test/other_table"])
        assert sorted(yt_cli.check_output(["yt", "list", "//home/wrapper_test"]).decode("utf-8").strip().split("\n")) == ["other_table", "table"]

        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/table"])

        completed_process = yt_cli.run(["yt", "read", "//home/wrapper_test/other_table", "--format", "dsv"])
        assert completed_process.returncode == 1
        assert b"yt.wrapper.errors.YtResolveError: Error resolving path //home/wrapper_test/other_table/@" in completed_process.stderr

        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "create", "account", "--attributes", "{name=test}"])
        yt_cli.check_output(["yt", "set", "//sys/accounts/test/@resource_limits/master_memory/total", "1000000"])
        yt_cli.check_output(["yt", "set", "//sys/accounts/test/@resource_limits/master_memory/chunk_host", "1000000"])
        yt_cli.check_output(["yt", "set", "//sys/accounts/test/@resource_limits/node_count", "1000"])
        yt_cli.check_output(["yt", "set", "//sys/accounts/test/@resource_limits/chunk_count", "100000"])
        yt_cli.check_output(["yt", "set", "//sys/accounts/test/@resource_limits/disk_space_per_medium/default", "1000000000"])
        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table", "--attributes", "{account=test}"])

        yt_cli.check_output(["yt", "copy", "//home/wrapper_test/table", "//home/wrapper_test/other_table"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@account"]) == b"\"sys\"\n"
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "copy", "//home/wrapper_test/table", "//home/wrapper_test/other_table", "--preserve-account"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@account"]) == b"\"test\"\n"
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "move", "//home/wrapper_test/table", "//home/wrapper_test/other_table", "--preserve-account"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@account"]) == b"\"test\"\n"
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table", "--attributes", "{account=test}"])
        yt_cli.check_output(["yt", "move", "//home/wrapper_test/table", "//home/wrapper_test/other_table"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@account"]) == b"\"sys\"\n"
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table", "--attributes", "{expiration_time=\"2050-01-01T12:00:00.000000Z\"}"])
        yt_cli.check_output(["yt", "move", "//home/wrapper_test/table", "//home/wrapper_test/other_table", "--preserve-expiration-time"])
        assert yt_cli.check_output(["yt", "exists", "//home/wrapper_test/other_table/@expiration_time"]) == b"true\n"
        yt_cli.check_output(["yt", "remove", "//home/wrapper_test/other_table"])

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/table", "--attributes", "{expiration_time=\"2050-01-01T12:00:00.000000Z\"}"])
        yt_cli.check_output(["yt", "copy", "//home/wrapper_test/table", "//home/wrapper_test/other_table"])
        assert yt_cli.check_output(["yt", "exists", "//home/wrapper_test/other_table/@expiration_time"]) == b"false\n"

    @authors("ilyaibraev")
    def test_merge_erase(self, yt_cli: YtCli):
        tables = []
        for i in range(1, 4):
            yt_cli.check_output(["yt", "write", f"//home/wrapper_test/table{i}", "--format", "dsv"], stdin=f"value={i}\n")
            tables.append(f"//home/wrapper_test/table{i}")
        yt_cli.check_output(["yt", "merge", "--src", *tables, "--dst", "//home/wrapper_test/merge"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/merge/@row_count"]) == b"3\n"

        yt_cli.check_output(["yt", "erase", "//home/wrapper_test/merge[#1:#2]"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/merge/@row_count"]) == b"2\n"

        yt_cli.check_output(["yt", "merge", "--src", "//home/wrapper_test/merge", "--src", "//home/wrapper_test/merge", "--dst", "//home/wrapper_test/merge"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/merge/@row_count"]) == b"4\n"

    @authors("ilyaibraev")
    def test_map_reduce(self, yt_cli: YtCli):
        yt_cli.env["YT_TABULAR_DATA_FORMAT"] = "dsv"
        yt_cli.check_output(["yt", "write", "//home/wrapper_test/input_table"], stdin="value=1\nvalue=2\n")
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/input_table/@row_count"]) == b"2\n"

        yt_cli.check_output(["yt", "map-reduce", "--mapper", "cat", "--reducer", "grep 2",
                             "--src", "//home/wrapper_test/input_table", "--dst", "//home/wrapper_test/input_table", "--reduce-by", "value"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/input_table/@row_count"]) == b"1\n"
        del yt_cli.env["YT_TABULAR_DATA_FORMAT"]

    @authors("ilyaibraev")
    def test_users(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "create", "user", "--attribute", "{name=test_user}"])
        yt_cli.check_output(["yt", "create", "group", "--attribute", "{name=test_group}"])

        assert yt_cli.check_output(["yt", "get", "//sys/groups/test_group/@members", "--format", "<format=text>yson"]) == b"[]\n"

        yt_cli.check_output(["yt", "add-member", "test_user", "test_group"])
        assert yt_cli.check_output(["yt", "get", "//sys/groups/test_group/@members", "--format", "<format=text>yson"]) == b"[\"test_user\";]\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/@acl/end", "{action=allow;subjects=[test_group];permissions=[write]}"])
        stdout = yt_cli.check_output(["yt", "check-permission", "test_user", "write", "//home/wrapper_test"])
        assert b"allow" in stdout

        yt_cli.check_output(["yt", "remove-member", "test_user", "test_group"])
        assert yt_cli.check_output(["yt", "get", "//sys/groups/test_group/@members", "--format", "<format=text>yson"]) == b"[]\n"

        yt_cli.check_output(["yt", "remove", "//sys/users/test_user"])

    @authors("ilyaibraev")
    def test_concurrent_upload_in_operation(self, yt_cli: YtCli):
        script_file_path = os.path.join(yt_cli.cwd, "script.sh")
        with open(script_file_path, "w") as script_file:
            script_file.write("cat")
        os.chmod(script_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        yt_cli.check_output(["yt", "write", "//home/wrapper_test/table", "--format", "dsv"], stdin="x=y\n")

        out1_process = yt_cli.run_in_background(["yt", "map", "cat", "--src", "//home/wrapper_test/table", "--dst", "//home/wrapper_test/out1", "--format", "dsv", "--local-file", "script.sh"])
        out2_process = yt_cli.run_in_background(["yt", "map", "cat", "--src", "//home/wrapper_test/table", "--dst", "//home/wrapper_test/out2", "--format", "dsv", "--local-file", "script.sh"])

        wait(lambda: all(
            yt_cli.check_output(["yt", "exists", f"//home/wrapper_test/out{out_index}"]) == b"true\n"
            and yt_cli.check_output(["yt", "read", f"//home/wrapper_test/out{out_index}", "--format", "dsv"]) == b"x=y\n"
            for out_index in range(1, 3)
        ), timeout=30)
        out1_process.kill()
        out2_process.kill()

    @authors("ilyaibraev")
    def test_sorted_by(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "write", "<sorted-by=[x]>//home/wrapper_test/table", "--format", "dsv"], stdin="x=y\n")
        yt_cli.check_output(["yt", "write", "<sorted_by=[x]>//home/wrapper_test/table", "--format", "dsv"], stdin="x=z\n")
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/table/@sorted"]) == b"%true\n"

    @authors("ilyaibraev")
    def test_transactions(self, yt_cli: YtCli):
        tx_id = yt_cli.check_output(["yt", "start-tx"]).decode("utf-8").strip()
        yt_cli.check_output(["yt", "abort-tx", tx_id])

        tx_id = yt_cli.check_output(["yt", "start-tx"]).decode("utf-8").strip()
        yt_cli.check_output(["yt", "commit-tx", tx_id])

    @authors("ilyaibraev")
    def test_hybrid_arguments(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/hybrid_test"])

        yt_cli.check_output(["yt", "copy", "//home/wrapper_test/hybrid_test", "--destination-path", "//home/wrapper_test/hybrid_copy"])
        assert yt_cli.check_output(["yt", "exists", "--path", "//home/wrapper_test/hybrid_copy"]) == b"true\n"

        yt_cli.check_output(["yt", "copy", "--destination-path", "//home/wrapper_test/hybrid_copy2", "--source-path", "//home/wrapper_test/hybrid_copy"])
        assert yt_cli.check_output(["yt", "exists", "--path", "//home/wrapper_test/hybrid_copy2"]) == b"true\n"

        yt_cli.check_output(["yt", "move", "//home/wrapper_test/hybrid_test", "--destination-path", "//home/wrapper_test/hybrid_moved"])
        assert yt_cli.check_output(["yt", "exists", "//home/wrapper_test/hybrid_moved"]) == b"true\n"

        yt_cli.check_output(["yt", "move", "--destination-path", "//home/wrapper_test/hybrid_test", "--source-path", "//home/wrapper_test/hybrid_moved"])
        assert yt_cli.check_output(["yt", "exists", "//home/wrapper_test/hybrid_test"]) == b"true\n"

        yt_cli.check_output(["yt", "link", "--link-path", "//home/wrapper_test/hybrid_link", "--target-path", "//home/wrapper_test/hybrid_test"])

        yt_cli.check_output(["yt", "remove", "--path", "//home/wrapper_test/hybrid_test"])
        completed_process = yt_cli.run(["yt", "read", "//home/wrapper_test/hybrid_link", "--format", "dsv"])
        assert completed_process.returncode == 1
        assert b"yt.wrapper.errors.YtResolveError: Error resolving path //home/wrapper_test/hybrid_link/@" in completed_process.stderr

        yt_cli.check_output(["yt", "create", "map_node", "//home/wrapper_test/test_dir"])
        yt_cli.check_output(["yt", "create", "--type", "map_node", "--path", "//home/wrapper_test/test_dir2"])
        assert yt_cli.check_output(["yt", "list", "--path", "//home/wrapper_test/test_dir"]) == b""
        assert yt_cli.check_output(["yt", "find", "--path", "//home/wrapper_test/test_dir", "--type", "file"]) == b""

        yt_cli.check_output(["yt", "write", "--table", "//home/wrapper_test/yamr_table", "--format", "yamr"], stdin="a\tb\n")

        assert yt_cli.check_output(["yt", "read", "--table", "//home/wrapper_test/yamr_table", "--format", "yamr"]) == b"a\tb\n"

        yt_cli.check_output(["yt", "write-file", "--destination", "//home/wrapper_test/test_file"], stdin="abcdef")
        assert yt_cli.check_output(["yt", "read-file", "--path", "//home/wrapper_test/test_file"]) == b"abcdef"

        tx_id = yt_cli.check_output(["yt", "start-tx", "--timeout", "10000"]).decode("utf-8").strip()
        yt_cli.check_output(["yt", "lock", "--path", "//home/wrapper_test/test_file", "--tx", tx_id])
        completed_process = yt_cli.run(["yt", "remove", "--path", "//home/wrapper_test/test_file"])
        assert completed_process.returncode == 1
        exception_text = b"yt.wrapper.errors.YtCypressTransactionLockConflict: " \
                         b"Cannot take \"exclusive\" lock for node //home/wrapper_test/test_file since \"exclusive\" lock is taken by concurrent transaction"
        assert exception_text in completed_process.stderr

        yt_cli.check_output(["yt", "ping-tx", "--transaction", tx_id])

        yt_cli.check_output(["yt", "abort-tx", "--transaction", tx_id])

        yt_cli.check_output(["yt", "check-permission", "root", "write", "//home/wrapper_test"])
        yt_cli.check_output(["yt", "check-permission", "--user", "root", "--permission", "write", "--path", "//home/wrapper_test"])

        yt_cli.check_output(["yt", "set", "--path", "//home/wrapper_test/value", "--value", "def"])
        assert yt_cli.check_output(["yt", "get", "--path", "//home/wrapper_test/value"]) == b"\"def\"\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/value", "abc"])
        assert yt_cli.check_output(["yt", "get", "--path", "//home/wrapper_test/value"]) == b"\"abc\"\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/value"], stdin="with_pipe")
        assert yt_cli.check_output(["yt", "get", "--path", "//home/wrapper_test/value"]) == b"\"with_pipe\"\n"

    @authors("ilyaibraev")
    def test_async_operations(self, yt_cli: YtCli):
        yt_cli.env["YT_TABULAR_DATA_FORMAT"] = "dsv"
        yt_cli.check_output(["yt", "write", "//home/wrapper_test/input_table"], stdin="x=1\n")

        map_op = yt_cli.check_output(["yt", "map", "tr 1 2", "--src", "//home/wrapper_test/input_table", "--dst", "//home/wrapper_test/map_output", "--async"]).decode("utf-8").strip()

        sort_op = yt_cli.check_output(
            [
                "yt", "sort", "--src", "//home/wrapper_test/input_table", "--dst",
                "//home/wrapper_test/sort_output", "--sort-by", "x", "--async"
            ]
        ).decode("utf-8").strip()
        yt_cli.check_output(["yt", "track-op", sort_op])

        reduce_op = yt_cli.check_output(
            [
                "yt", "reduce", "cat", "--src", "//home/wrapper_test/sort_output", "--dst", "//home/wrapper_test/reduce_output",
                "--sort-by", "x", "--reduce-by", "x", "--async"
            ]
        ).decode("utf-8").strip()
        yt_cli.check_output(["yt", "track-op", reduce_op])

        op = yt_cli.check_output(
            [
                "yt", "map-reduce", "--mapper", "cat", "--reducer", "cat", "--src", "//home/wrapper_test/sort_output",
                "--dst", "//home/wrapper_test/map_reduce_output", "--reduce-by", "x", "--async"
            ]
        ).decode("utf-8").strip()
        yt_cli.check_output(["yt", "track-op", op])

        yt_cli.check_output(["yt", "track-op", map_op])
        assert yt_cli.check_output(["yt", "read", "//home/wrapper_test/map_output"]) == b"x=2\n"

        del yt_cli.env["YT_TABULAR_DATA_FORMAT"]

    @authors("ilyaibraev")
    def test_json_structured_format(self, yt_cli: YtCli):
        yt_cli.env["YT_STRUCTURED_DATA_FORMAT"] = "json"
        yt_cli.check_output(["yt", "set", "//home/wrapper_test/folder", "{}"])

        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test"]) == b"{\n    \"folder\": {\n\n    }\n}\n\n"

        yt_cli.check_output(["yt", "set", "//home/wrapper_test/folder/@attr", "{\"test\": \"value\"}"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/folder/@attr"]) == b"{\n    \"test\": \"value\"\n}\n\n"
        del yt_cli.env["YT_STRUCTURED_DATA_FORMAT"]

    @authors("ilyaibraev")
    def test_transform(self, yt_cli: YtCli):
        yt_cli.env["YT_TABULAR_DATA_FORMAT"] = "dsv"
        yt_cli.check_output(["yt", "write", "//home/wrapper_test/table_to_transform"], stdin="k=v\n")
        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/table_to_transform"])

        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/table_to_transform", "--compression-codec", "zlib_6"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/table_to_transform/@compression_codec"]) == b"\"zlib_6\"\n"

        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/table_to_transform", "--compression-codec", "zlib_6", "--check-codecs"])

        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/table_to_transform", "//home/wrapper_test/other_table", "--compression-codec", "zlib_6"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@compression_codec"]) == b"\"zlib_6\"\n"

        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/table_to_transform", "//home/wrapper_test/other_table", "--optimize-for", "scan"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/other_table/@optimize_for"]) == b"\"scan\"\n"

        yt_cli.check_output(["yt", "create", "table", "//home/wrapper_test/empty_table_to_transform"])
        yt_cli.check_output(["yt", "transform", "//home/wrapper_test/empty_table_to_transform", "--compression-codec", "brotli_8", "--optimize-for", "scan"])
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/empty_table_to_transform/@optimize_for"]) == b"\"scan\"\n"
        assert yt_cli.check_output(["yt", "get", "//home/wrapper_test/empty_table_to_transform/@compression_codec"]) == b"\"brotli_8\"\n"

        del yt_cli.env["YT_TABULAR_DATA_FORMAT"]

    @authors("ilyaibraev")
    def test_create_temp_table(self, yt_cli: YtCli):
        table_yt_pwd = yt_cli.check_output(["yt", "create-temp-table"]).decode("utf-8").strip()
        assert yt_cli.check_output(["yt", "exists", table_yt_pwd]) == b"true\n"

        table_yt_pwd = yt_cli.check_output(["yt", "create-temp-table", "--attributes", "{test_attribute=a}"]).decode("utf-8").strip()
        assert yt_cli.check_output(["yt", "get", f"{table_yt_pwd}/@test_attribute"]) == b"\"a\"\n"

        yt_cli.check_output(["yt", "create", "map_node", "//home/wrapper_test/temp_tables"])
        table_yt_pwd = yt_cli.check_output(["yt", "create-temp-table", "--path", "//home/wrapper_test/temp_tables", "--name-prefix", "check"]).decode("utf-8").strip()
        assert table_yt_pwd.startswith("//home/wrapper_test/temp_tables/check")

        table_yt_pwd = yt_cli.check_output(["yt", "create-temp-table", "--expiration-timeout", "1000"]).decode("utf-8").strip()
        wait(lambda: yt_cli.check_output(["yt", "exists", table_yt_pwd]) == b"false\n", timeout=2)

    @authors("ilyaibraev")
    def test_dynamic_table_commands(self, yt_cli: YtCli):
        tablet_cell_id = yt_cli.check_output(["yt", "create", "tablet_cell", "--attributes", "{size=1}"]).decode("utf-8").strip()

        schema = "[{name=x; type=string; sort_order=ascending};{name=y; type=int64}]"

        table_pwd = "//home/wrapper_test/dyn_table"
        yt_cli.check_output(["yt", "create", "table", table_pwd, "--attributes", "{" f"schema={schema}; dynamic=%true" "}"])

        wait(lambda: yt_cli.check_output(["yt", "get", f"//sys/tablet_cells/{tablet_cell_id}/@health"]) == b"\"good\"\n", timeout=30)

        yt_cli.check_output(["yt", "mount-table", table_pwd, "--sync"])

        yt_cli.check_output(["yt", "insert-rows", table_pwd, "--format", "<format=text>yson"], stdin="{x=a; y=1};{x=b;y=2}")
        yt_cli.check_output(["yt", "delete-rows", table_pwd, "--format", "<format=text>yson"], stdin="{x=a}")

        assert yt_cli.check_output(["yt", "select-rows", f"x FROM [{table_pwd}]", "--format", "<format=text>yson"]) == b"{\"x\"=\"b\";};\n"

        yt_cli.check_output(["yt", "select-rows", f"x FROM [{table_pwd}]", "--print-statistics", "--format", "<format=text>yson"])
        yt_cli.check_output(["yt", "unmount-table", table_pwd, "--sync"])

    @authors("ilyaibraev")
    def test_atomicity_argument(self, yt_cli: YtCli):
        tablet_cell_id = yt_cli.check_output(["yt", "create", "tablet_cell", "--attributes", "{size=1}"]).decode("utf-8").strip()

        schema = "[{name=x; type=string; sort_order=ascending};{name=y; type=int64}]"

        table_pwd = "//home/wrapper_test/dyn_table"
        yt_cli.check_output(["yt", "create", "table", table_pwd, "--attributes", "{" f"schema={schema}; dynamic=%true; atomicity=none" "}"])

        wait(lambda: yt_cli.check_output(["yt", "get", f"//sys/tablet_cells/{tablet_cell_id}/@health"]) == b"\"good\"\n", timeout=30)

        yt_cli.check_output(["yt", "mount-table", table_pwd, "--sync"])
        yt_cli.check_output(["yt", "insert-rows", table_pwd, "--format", "<format=text>yson", "--atomicity", "none"], stdin="{x=a; y=1};{x=b;y=2}")
        yt_cli.check_output(["yt", "delete-rows", table_pwd, "--format", "<format=text>yson", "--atomicity", "none"], stdin="{x=a}")

        yt_cli.check_output(["yt", "unmount-table", table_pwd, "--sync"])

    @authors("ilyaibraev")
    def test_sandbox_file_name_specification(self, yt_cli: YtCli):
        table_path = "//home/wrapper_test/table"
        yt_cli.check_output(["yt", "write", table_path, "--format", "dsv"], stdin="a=b\n")

        script_file_path = os.path.join(yt_cli.cwd, "script")

        with open(script_file_path, "w") as script_file:
            script_file.write("content")

        os.chmod(script_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        yt_cli.check_output(["yt", "map", "ls some_file >/dev/null && cat", "--src", table_path, "--dst", table_path, "--local-file", "<file_name=some_file>script", "--format", "dsv"])

    @authors("ilyaibraev")
    def test_execute(self, yt_cli: YtCli):
        import json
        table_path = "//home/wrapper_test/test_table"
        yt_cli.check_output(["yt", "execute", "create", "{type=table;path=\"" + table_path + "\";output_format=yson}"])
        response = yt_cli.check_output(["yt", "execute", "exists", "{path=\"" + table_path + "\";output_format=json}"]).decode("utf-8").strip()
        assert json.loads(response)["value"]
        yt_cli.check_output(["yt", "execute", "remove", "{path=\"" + table_path + "\"}"])
        response = yt_cli.check_output(["yt", "execute", "exists", "{path=\"" + table_path + "\";output_format=json}"])
        assert not json.loads(response)["value"]

    @authors("ilyaibraev")
    def test_brotli_write(self, yt_cli: YtCli):
        table_path = "//home/wrapper_test/test_table"
        yt_cli.check_output(["yt", "write", table_path, "--format", "dsv", "--config", "{proxy={content_encoding=br}}"], stdin="x=1\nx=2\nx=3\nx=4\n")
        assert sorted(yt_cli.check_output(["yt", "read", table_path, "--format", "dsv"]).decode().strip().split("\n")) == ["x=1", "x=2", "x=3", "x=4"]

    @authors("ilyaibraev")
    def test_vanilla_operations(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "vanilla", "--tasks", "{sample={command=\"echo AAA >&2\";job_count=1}}"])
        op_id = yt_cli.check_output(["yt", "vanilla", "--tasks", "{sample={command=\"echo AAA >&2\";job_count=1}}", "--async"]).decode("utf-8").strip()
        yt_cli.check_output(["yt", "get-operation", op_id])

    @authors("ilyaibraev")
    def test_ping_ancestor_transactions_in_operations(self, yt_cli: YtCli):
        tx = yt_cli.check_output(["yt", "start-tx", "--timeout", "5000"]).decode("utf-8").strip()
        completed_process = yt_cli.run(["yt", "vanilla", "--tasks", "{sample={command=\"sleep 6\";job_count=1}}", "--tx", tx])
        assert completed_process.returncode == 1
        assert f"\"code\"=11000;\"message\"=\"User transaction {tx} has expired or was aborted\"".encode("utf-8") in completed_process.stderr

        tx = yt_cli.check_output(["yt", "start-tx", "--timeout", "10000"]).decode("utf-8").strip()
        yt_cli.check_output(["yt", "vanilla", "--tasks", "{sample={command=\"sleep 12\";job_count=1}}", "--tx", tx, "--ping-ancestor-txs"])

    @authors("ilyaibraev")
    def test_operation_and_job_commands(self, yt_cli: YtCli):
        import json

        yt_cli.env["YT_TABULAR_DATA_FORMAT"] = "dsv"
        yt_cli.check_output(["yt", "write", "//home/wrapper_test/input_table"], stdin="x=1\n")
        map_op = yt_cli.check_output(
            [
                "yt", "map", "echo \"Well hello there\" >&2 && tr 1 2", "--src", "//home/wrapper_test/input_table",
                "--dst", "//home/wrapper_test/map_output", "--async"
            ]
        ).decode("utf-8").strip()
        yt_cli.check_output(["yt", "track-op", map_op])

        get_operation_res = yt_cli.check_output(["yt", "get-operation", map_op, "--attribute", "state", "--attribute", "authenticated_user", "--format", "json"])
        state = json.loads(get_operation_res)["state"]
        user = json.loads(get_operation_res)["authenticated_user"]
        assert state == "completed"
        assert user == "root"
        assert len(json.loads(get_operation_res)) == 2

        list_operations_res = yt_cli.check_output(["yt", "list-operations", "--format", "json"])
        assert map_op in [d["id"] for d in json.loads(list_operations_res)["operations"]]

        list_operations_res_ts = yt_cli.check_output(["yt", "list-operations", "--to-time", "1", "--format", "json"])
        assert map_op not in [d["id"] for d in json.loads(list_operations_res_ts)["operations"]]

        list_jobs_res = yt_cli.check_output(["yt", "list-jobs", map_op, "--format", "json"])
        job_id = json.loads(list_jobs_res)["jobs"][0]["id"].__str__()

        wait(lambda: yt_cli.check_output(["yt", "get-job", job_id, map_op, "--format", "json"]), timeout=30)
        get_job_res = yt_cli.check_output(["yt", "get-job", job_id, map_op, "--format", "json"])
        job_state = json.loads(get_job_res)["state"]
        assert job_state == "completed"

        del yt_cli.env["YT_TABULAR_DATA_FORMAT"]

    @authors("ilyaibraev")
    def test_check_permissions(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "create", "user", "--attribute", "{name=test_user}"])

        table = "//home/wrapper_test/table"
        yt_cli.check_output(["yt", "create", "table", table, "--attributes", "{schema=[{name=a;type=string;};{name=b;type=string;}];acl=[{action=allow;subjects=[test_user];permissions=[read]}]}"])
        yt_cli.check_output(["yt", "write-table", table, "--format", "dsv"], stdin="a=10\tb=20\n")

        stdout = yt_cli.check_output(["yt", "check-permission", "test_user", "read", "//home/wrapper_test/table"])
        assert b"allow" in stdout
        stdout = yt_cli.check_output(["yt", "check-permission", "test_user", "read", "//home/wrapper_test/table", "--columns", "[a]"])
        assert b"allow" in stdout

    @authors("ilyaibraev")
    def test_transfer_account_resources(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "create", "account", "--attributes", "{name=a1;resource_limits={node_count=10}}"])
        yt_cli.check_output(["yt", "create", "account", "--attributes", "{name=a2;resource_limits={node_count=12}}"])

        yt_cli.check_output(["yt", "transfer-account-resources", "a1", "a2", "--resource-delta", "{node_count=3}"])
        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a1/@resource_limits/node_count"]) == b"7\n"
        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a2/@resource_limits/node_count"]) == b"15\n"
        yt_cli.check_output(["yt", "transfer-account-resources", "--src", "a1", "--dst", "a2", "--resource-delta", "{node_count=3}"])

        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a1/@resource_limits/node_count"]) == b"4\n"
        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a2/@resource_limits/node_count"]) == b"18\n"

        yt_cli.check_output(["yt", "transfer-account-resources", "--destination-account", "a1", "--source-account", "a2", "--resource-delta", "{node_count=6}"])
        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a1/@resource_limits/node_count"]) == b"10\n"
        assert yt_cli.check_output(["yt", "get", "//sys/accounts/a2/@resource_limits/node_count"]) == b"12\n"

    @authors("ilyaibraev")
    def test_transfer_pool_resources(self, yt_cli: YtCli):
        attributes = \
            "{name=from;pool_tree=default;strong_guarantee_resources={cpu=10};\
                integral_guarantees={resource_flow={cpu=20};burst_guarantee_resources={cpu=30}};max_running_operation_count=40;max_operation_count=50}"
        yt_cli.check_output(
            [
                "yt", "create", "scheduler_pool",
                "--attributes", attributes
            ]
        )
        attributes = \
            "{name=to;pool_tree=default;strong_guarantee_resources={cpu=10};\
                integral_guarantees={resource_flow={cpu=20};burst_guarantee_resources={cpu=30}};max_running_operation_count=40;max_operation_count=50}"
        yt_cli.check_output(
            [
                "yt", "create", "scheduler_pool",
                "--attributes", attributes
            ]
        )

        resource_delta = "{strong_guarantee_resources={cpu=4};resource_flow={cpu=8};burst_guarantee_resources={cpu=12};max_running_operation_count=16;max_operation_count=20}"
        yt_cli.check_output(
            ["yt", "transfer-pool-resources", "--src", "from", "--dst", "to",
             "--pool-tree", "default", "--resource-delta", resource_delta])

        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/from/@strong_guarantee_resources/cpu"]) == b"6.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/from/@integral_guarantees/resource_flow/cpu"]) == b"12.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/from/@integral_guarantees/burst_guarantee_resources/cpu"]) == b"18.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/from/@max_running_operation_count"]) == b"24\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/from/@max_operation_count"]) == b"30\n"

        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/to/@strong_guarantee_resources/cpu"]) == b"14.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/to/@integral_guarantees/resource_flow/cpu"]) == b"28.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/to/@integral_guarantees/burst_guarantee_resources/cpu"]) == b"42.\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/to/@max_running_operation_count"]) == b"56\n"
        assert yt_cli.check_output(["yt", "get", "//sys/pool_trees/default/to/@max_operation_count"]) == b"70\n"

    @authors("ilyaibraev")
    def test_generate_timestamp(self, yt_cli: YtCli):
        ts = yt_cli.check_output(["yt", "generate-timestamp"]).decode("utf-8").strip()
        assert ts.isdigit()
        assert int(ts) > 0

    @authors("ilyaibraev")
    def test_sort_order(self, yt_cli: YtCli):
        in_table = "//home/wrapper_test/in_table_to_sort"
        out_table = "//home/wrapper_test/out_table_to_sort"

        yt_cli.check_output(["yt", "create", "table", in_table, "--attributes", "{schema=[{name=a;type=int64;};{name=b;type=int64;};{name=c;type=int64;}];}"])
        values = "a=2\tb=1\tc=3\n" \
                 "a=1\tb=2\tc=2\n" \
                 "a=2\tb=2\tc=4\n" \
                 "a=1\tb=1\tc=1\n"
        yt_cli.check_output(["yt", "write-table", in_table, "--format", "<enable_string_to_all_conversion=%true>dsv"], stdin=values)

        yt_cli.check_output(["yt", "sort", "--src", in_table, "--dst", out_table, "--sort-by", "c"])
        stdout = yt_cli.check_output(["yt", "read-table", out_table, "--format", "dsv"])
        c_values = list(map(lambda x: x.split()[0], stdout.decode("utf-8").strip().split("\n")))
        assert c_values == ["c=1", "c=2", "c=3", "c=4"]

        yt_cli.check_output(["yt", "sort", "--src", in_table, "--dst", out_table, "--sort-by", "{name=c; sort_order=descending;}"])
        stdout = yt_cli.check_output(["yt", "read-table", out_table, "--format", "dsv"])
        c_values = list(map(lambda x: x.split()[0], stdout.decode("utf-8").strip().split("\n")))
        assert c_values == ["c=4", "c=3", "c=2", "c=1"]

        yt_cli.check_output(["yt", "sort", "--src", in_table, "--dst", out_table, "--sort-by", "{name=a; sort_order=descending;}", "--sort-by", "b"])
        stdout = yt_cli.check_output(["yt", "read-table", out_table, "--format", "dsv"])
        c_values = list(map(lambda x: x.split()[2], stdout.decode("utf-8").strip().split("\n")))
        assert c_values == ["c=3", "c=4", "c=1", "c=2"]

        yt_cli.check_output(["yt", "sort", "--src", in_table, "--dst", out_table, "--sort-by", "b", "--sort-by", "a"])
        stdout = yt_cli.check_output(["yt", "read-table", out_table, "--format", "dsv"])
        c_values = list(map(lambda x: x.split()[2], stdout.decode("utf-8").strip().split("\n")))
        assert c_values == ["c=1", "c=3", "c=2", "c=4"]

        yt_cli.check_output(["yt", "sort", "--src", in_table, "--dst", out_table, "--sort-by", "b", "--sort-by", "{name=a; sort_order=descending;}"])
        stdout = yt_cli.check_output(["yt", "read-table", out_table, "--format", "dsv"])
        c_values = list(map(lambda x: x.split()[2], stdout.decode("utf-8").strip().split("\n")))
        assert c_values == ["c=3", "c=1", "c=4", "c=2"]

    def write_random_file(self, path: str, shape_x: int, shape_y: int):
        symbols = string.ascii_lowercase
        with open(path, "w") as file:
            for _ in range(shape_y):
                file.write("".join(random.choices(symbols, k=shape_x))+"\n")

    @authors("ilyaibraev")
    def test_dirtable(self, yt_cli: YtCli):
        cwd = yt_cli.cwd

        source_dir = os.path.join(cwd, "tmp_source_files")
        download_dir = os.path.join(cwd, "tmp_download_files")
        os.mkdir(source_dir)

        self.write_random_file(os.path.join(source_dir, "random_file1"), 1024, 1024)

        os.mkdir(os.path.join(source_dir, "data"))
        self.write_random_file(os.path.join(source_dir, "data", "random_file2"), 1024, 2048)

        yt_cli.check_output(["yt", "dirtable", "upload", "--directory", source_dir, "--yt-table", "//home/wrapper_test/dirtable"])
        os.mkdir(download_dir)
        yt_cli.check_output(["yt", "dirtable", "download", "--directory", download_dir, "--yt-table", "//home/wrapper_test/dirtable"])
        assert yt_cli.check_output(["diff", "-qr", source_dir, download_dir]) == b""

        download_dir += "_new"

        self.write_random_file(os.path.join(source_dir, "i_am_just_appended_random_file"), 1024, 3840)
        yt_cli.check_output(
            [
                "yt", "dirtable", "append-single-file", "--yt-name", "i_am_just_appended_random_file",
                "--fs-path", f"{source_dir}/i_am_just_appended_random_file", "--yt-table", "//home/wrapper_test/dirtable"
            ]
        )
        yt_cli.check_output(["yt", "dirtable", "download", "--directory", download_dir, "--yt-table", "//home/wrapper_test/dirtable"])
        assert yt_cli.check_output(["diff", "-qr", source_dir, download_dir]) == b""

    @authors("nadya73")
    def test_queue_producer(self, yt_cli: YtCli):
        yt_cli.check_output(["yt", "create", "tablet_cell", "--attributes", "{size=1}"]).decode("utf-8").strip()

        queue_path = "//home/wrapper_test/queue"
        producer_path = "//home/wrapper_test/producer"

        yt_cli.check_output(["yt", "create", "table", queue_path, "--attribute", "{dynamic=%true;schema=[{name=data;type=string;}]}"])
        yt_cli.check_output(["yt", "mount-table", queue_path])

        yt_cli.check_output(["yt", "create", "queue_producer", producer_path])
        time.sleep(0.2)

        session_id = "session_123"

        session_info = yson.loads(yt_cli.check_output(["yt", "create-queue-producer-session", "--producer-path", producer_path, "--queue-path", queue_path, "--session-id", session_id]))
        assert session_info["epoch"] == 0
        assert session_info["sequence_number"] == -1

        push_result = yson.loads(yt_cli.check_output([
            "yt", "push-queue-producer", producer_path, queue_path,
            "--session-id", session_id, "--input-format", "<format=text>yson", "--epoch", "0"
        ], stdin="{data=a;\"$sequence_number\"=1};{data=b;\"$sequence_number\"=3};"))
        assert push_result["last_sequence_number"] == 3

        yt_cli.check_output(["yt", "remove-queue-producer-session", "--producer-path", producer_path, "--queue-path", queue_path, "--session-id", session_id])
