from .conftest import authors
from .helpers import TEST_DIR, wait

from yt.wrapper.batch_helpers import create_batch_client
from yt.wrapper.batch_response import apply_function_to_result
from yt.wrapper.batch_execution import YtBatchRequestFailedError

import yt.wrapper as yt

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

import pytest

import datetime
import tempfile
import time
import os


@pytest.mark.usefixtures("yt_env")
class TestBatchExecution(object):
    def setup(self):
        client = create_batch_client()
        client.create("user", attributes={"name": "batch_tester"})
        client.create("account", attributes={"name": "batch_tester"})
        client.create("group", attributes={"name": "batch_testers"})
        client.create("group", attributes={"name": "batch_super_testers"})
        client.commit_batch()

    def teardown(self):
        client = create_batch_client()
        client.remove("//sys/users/batch_tester", force=True)
        client.remove("//sys/groups/batch_testers", force=True)
        client.remove("//sys/accounts/batch_tester", force=True)
        client.remove("//sys/groups/batch_super_testers", force=True)
        client.commit_batch()

    @authors("ostyakov")
    def test_unsupported_commands(self):
        with pytest.raises(AttributeError):
            client = create_batch_client()
            client.write_table(TEST_DIR + "/batch_node/table", [{"a": 1}])
            client.commit_batch()

        with pytest.raises(Exception):
            client = create_batch_client()
            batch_node = client.get(TEST_DIR + "/batch_node")
            client.set(TEST_DIR + "/batch_node2", batch_node)
            client.commit_batch()

    @authors("ostyakov")
    def test_cypress_commands(self):
        table = TEST_DIR + "/batch_node/table"
        other_table = TEST_DIR + "/batch_node/other_table"
        map_node = TEST_DIR + "/batch_node/map_node"
        link = TEST_DIR + "/batch_node/table_link"

        client = create_batch_client()
        client.mkdir(TEST_DIR + "/batch", recursive=True)
        client.set(TEST_DIR + "/batch_node", {"attr": 1})
        client.set(TEST_DIR + "/batch_node2", {})
        client.commit_batch()

        mkdir_response = client.mkdir(TEST_DIR + "/batch")
        batch_node = client.get(TEST_DIR + "/batch_node")
        exists_result = client.exists(TEST_DIR + "/batch")
        client.commit_batch()

        assert batch_node.get_result() == {"attr": 1}
        assert mkdir_response.get_result() is None
        assert not mkdir_response.is_ok()
        assert exists_result.get_result()

        client.set_attribute(TEST_DIR + "/batch_node2", "attr", 1)
        client.remove(TEST_DIR + "/batch")
        client.create("file", TEST_DIR + "/test_file", recursive=True)
        client.create("table", table)
        client.commit_batch()

        node = client.get(TEST_DIR + "/batch_node2", attributes=["attr", "other_attr"])
        copy_result = client.copy(table, table)
        client.copy(table, other_table)
        client.commit_batch()

        assert node.get_result().attributes == {"attr": 1}
        assert not copy_result.is_ok()
        assert yt.exists(table)
        assert yt.exists(other_table)

        yt.remove(other_table)

        client.move(table, other_table)
        client.commit_batch()

        assert not yt.exists(table)
        assert yt.exists(other_table)

        yt.create("table", table)

        client.link(table, link)
        client.create("map_node", map_node)
        client.commit_batch()

        assert not yt.get_attribute(link + "&", "broken")
        assert yt.get_attribute(link + "&", "target_path") == table

        table_type = client.get_type(table)
        map_node_type = client.get_type(map_node)
        client.commit_batch()

        assert table_type.get_result() == "table"
        assert map_node_type.get_result() == "map_node"

        tables = ["{0}/table_{1}".format(TEST_DIR + "/batch_node_list", str(i)) for i in xrange(10)]
        for table in tables:
            client.create("table", table, recursive=True)
        client.commit_batch()

        list_result = client.list(TEST_DIR + "/batch_node_list")
        client.commit_batch()

        assert set(list_result.get_result()) == set(("table_" + str(i) for i in xrange(10)))

    @authors("ostyakov")
    def test_acl_commands(self):
        client = create_batch_client()
        permissions_read = client.check_permission("batch_tester", "read", "//sys")
        permissions_write = client.check_permission("batch_tester", "write", "//sys")
        client.commit_batch()
        assert permissions_read.get_result()["action"] == "allow"
        assert permissions_write.get_result()["action"] == "deny"

        client.add_member("batch_tester", "batch_testers")
        client.commit_batch()

        assert yt.get_attribute("//sys/groups/batch_testers", "members") == ["batch_tester"]

        client.remove_member("batch_tester", "batch_testers")
        client.commit_batch()

        assert yt.get_attribute("//sys/groups/batch_testers", "members") == []

    @authors("ostyakov")
    def test_table_commands(self, yt_env):
        table = TEST_DIR + "/batch_node/test_table"

        client = create_batch_client()

        client.create("table", table, recursive=True)
        client.commit_batch()

        assert yt.exists(table)

        row_count = client.row_count(table)
        is_sorted = client.is_sorted(table)
        is_empty = client.is_empty(table)
        client.commit_batch()

        assert row_count.get_result() == 0
        assert is_empty.get_result()
        assert not is_sorted.get_result()

        yt.remove(table)
        yt.create("table", table, attributes={
            "dynamic": True,
            "schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
            ]})
        tablet_id = yt.create("tablet_cell", attributes={"size": 1})
        while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != "good":
            time.sleep(0.1)

        client.mount_table(table)
        client.commit_batch()

        while yt.get("{0}/@tablets/0/state".format(table)) != "mounted":
            time.sleep(0.1)

        with pytest.raises(yt.YtError):
            client.unmount_table(table, sync=True)

        client.unmount_table(table)
        client.commit_batch()

        while yt.get("{0}/@tablets/0/state".format(table)) != "unmounted":
            time.sleep(0.1)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("all_keys", [True, False])
    def test_get_in_sync_replicas(self, all_keys):
        client = create_batch_client()

        def _get_in_sync_replicas(table, ts, keys):
            return client.get_in_sync_replicas(
                table,
                ts,
                [] if all_keys else keys,
                all_keys=all_keys,
            )

        schema = [{"name": "x", "type": "string", "sort_order": "ascending"},
                  {"name": "y", "type": "string"}]

        def _create_replicas(table, replicas):
            return [
                yt.create(
                    "table_replica",
                    attributes={
                        "table_path": table,
                        "cluster_name": "primary",
                        "mode": "sync",
                        "replica_path": replica,
                    }
                ) for replica in replicas
            ]

        def _create_supertable(table):
            yt.create("replicated_table", table, attributes={"dynamic": True, "schema": schema})
            yt.mount_table(table, sync=True)
            table_replicas = [table + "_replica_" + str(i) for i in range(2)]
            table_replica_ids = _create_replicas(table, table_replicas)
            return table_replicas, table_replica_ids

        def _create_tables(replicas, replica_ids):
            assert len(replicas) == len(replica_ids)
            for i in range(len(replicas)):
                yt.create("table", replicas[i], attributes={
                    "dynamic": True,
                    "schema": schema,
                    "upstream_replica_id": replica_ids[i]
                })
                yt.mount_table(replicas[i], sync=True)

        def _get_in_sync_replicas_checker(expected_table_replica_ids, expected_table2_replica_ids):
            def _equal_as_sets(lhs, rhs):
                return set(lhs) == set(rhs)

            ts = yt.generate_timestamp()

            def _checkable():
                sync_replicas = _get_in_sync_replicas(table, ts, [{"x": "a"}])
                sync_replicas2 = _get_in_sync_replicas(table2, ts, [{"x": "b"}])
                client.commit_batch()
                return (
                    _equal_as_sets(sync_replicas.get_result(), expected_table_replica_ids)
                    and
                    _equal_as_sets(sync_replicas2.get_result(), expected_table2_replica_ids)
                )
            return _checkable

        def _sync_enable(replica_ids):
            for replica_id in replica_ids:
                yt.alter_table_replica(replica_id, enabled=True)
                wait(lambda: yt.get("#{0}/@state".format(replica_id)) == "enabled")

        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")

        table = TEST_DIR + "/test_get_in_sync_replicas"
        table2 = TEST_DIR + "/test_get_in_sync_replicas2"

        table_replicas, table_replica_ids = _create_supertable(table)
        table2_replicas, table2_replica_ids = _create_supertable(table2)

        wait(_get_in_sync_replicas_checker([], []))

        _create_tables(table_replicas, table_replica_ids)
        _create_tables(table2_replicas, table2_replica_ids)

        wait(_get_in_sync_replicas_checker([], []))

        _sync_enable(table_replica_ids)
        wait(_get_in_sync_replicas_checker(table_replica_ids, []))

        _sync_enable(table2_replica_ids)
        wait(_get_in_sync_replicas_checker(table_replica_ids, table2_replica_ids))

    @authors("ostyakov")
    def test_transactions(self):
        table = TEST_DIR + "/batch_node/test_transaction_table"
        new_client = yt.YtClient(token=yt.config["token"], config=yt.config.config)

        with yt.Transaction():
            client = create_batch_client()
            client.create("table", table, recursive=True)
            client.commit_batch()

            assert not new_client.exists(table)

    @authors("ostyakov")
    def test_commit(self):
        client = create_batch_client()
        client.mkdir(TEST_DIR + "/batch_commit", recursive=True)
        client.commit_batch()
        exist_result = client.exists(TEST_DIR + "/batch_commit")
        client.commit_batch()
        client.commit_batch()
        assert exist_result.get_result()

    @authors("ostyakov")
    def test_raise_errors(self):
        yt.mkdir(TEST_DIR + "/raise_error", recursive=True)
        with pytest.raises(YtBatchRequestFailedError):
            client = create_batch_client(raise_errors=True)
            client.mkdir(TEST_DIR + "/raise_error")
            client.mkdir(TEST_DIR + "/raise_error/dir/dir")
            client.commit_batch()

    @authors("ostyakov")
    def test_retries(self):
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True
        try:
            client = create_batch_client()
            client.mkdir(TEST_DIR + "/batch_retries", recursive=True)
            client.commit_batch()
            assert yt.exists(TEST_DIR + "/batch_retries")

            client = create_batch_client()
            client.remove(TEST_DIR + "/batch_retries")
            client.commit_batch()
            assert not yt.exists(TEST_DIR + "/batch_retries")
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

    @authors("ostyakov")
    def test_batch_response(self):
        table = TEST_DIR + "/test_batch_response_table"

        client = create_batch_client()
        create_result = client.create("table", table, recursive=True)

        with pytest.raises(yt.YtError):
            create_result.get_result()

        with pytest.raises(yt.YtError):
            create_result.get_error()

        with pytest.raises(yt.YtError):
            create_result.is_ok()

        result = []

        @authors("ostyakov")
        def test_result_function(output, error):
            assert error is None
            result.append(output)
            return output, error

        apply_function_to_result(test_result_function, create_result, include_error=True)
        client.commit_batch()

        create_result.get_result()
        assert result[0] is not None

    @authors("aleexfi")
    def test_supported_formats(self):
        user_attrs = {
            "int_attr": 42,
            "string_attr": "arbitrary string",
            "complex_attr": [1, 2]
        }
        supported_formats = [
            yt.YsonFormat("text"),
            yt.YsonFormat("binary"),
            yt.JsonFormat()
        ]

        client = create_batch_client()
        client.set(TEST_DIR + "/default_format", user_attrs)
        for i, format in enumerate(supported_formats):
            client.set(TEST_DIR + "/format_{}".format(i), format.dumps_node(user_attrs), format=format)
        client.commit_batch()

        nodes_attrs = [client.get(TEST_DIR + "/default_format")]
        for i in range(len(supported_formats)):
            nodes_attrs.append(client.get(TEST_DIR + "/format_{}".format(i)))
        client.commit_batch()

        assert [attrs.get_result() for attrs in nodes_attrs] == [user_attrs] * (len(supported_formats) + 1)


@authors("levysotsky")
@pytest.mark.usefixtures("yt_env_job_archive")
class TestBatchExecutionOperationCommands(object):
    def test_operation_commands(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        with tempfile.NamedTemporaryFile("r") as f:
            os.chmod(f.name, 0x777)
            op1 = yt.run_map("echo $YT_JOB_ID > {} && cat".format(f.name), table, table, job_count=1, format="json")
            job_id = f.read().strip()
        after_op1 = datetime.datetime.utcnow()
        op2 = yt.run_sort(table, table, sort_by=["x"])
        op3 = yt.run_reduce("cat", table, table, reduce_by=["x"], format="json")

        client = create_batch_client()
        get_job_result = client.get_job(op1.id, job_id)
        list_jobs_result = client.list_jobs(op1.id)
        get_operation_result = client.get_operation(op1.id)
        list_operations_result = client.list_operations(from_time=after_op1)

        client.commit_batch()

        assert get_job_result.is_ok(), get_job_result.get_error()
        job = get_job_result.get_result()
        assert job["job_id"] == job_id
        assert job["type"] == "map"
        assert job["state"] == "completed"

        assert list_jobs_result.is_ok(), list_jobs_result.get_error()
        jobs = list_jobs_result.get_result()["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["type"] == "map"

        assert get_operation_result.is_ok(), get_operation_result.get_error()
        operation = get_operation_result.get_result()
        assert operation["id"] == op1.id
        assert operation["type"] == "map"

        assert list_operations_result.is_ok(), list_operations_result.get_error()
        operations = list_operations_result.get_result()["operations"]
        assert [op["id"] for op in operations] == [op3.id, op2.id]
