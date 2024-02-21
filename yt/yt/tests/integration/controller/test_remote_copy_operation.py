from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, print_debug, wait, create, get, set, remove,
    exists, create_user,
    make_ace, insert_rows, select_rows, lookup_rows, delete_rows, alter_table, read_table, write_table, merge,
    remote_copy, sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table,
    sync_reshard_table, sync_flush_table, sync_compact_table,
    multicell_sleep, set_node_banned, set_nodes_banned, set_all_nodes_banned, sorted_dicts,
    raises_yt_error, get_driver,
    create_pool, update_pool_tree_config_option,
    update_controller_agent_config,
    write_file, read_file)

from yt_helpers import skip_if_no_descending, skip_if_old
from yt_type_helpers import make_schema, normalize_schema, normalize_schema_v3, optional_type, list_type
import yt_error_codes

from yt.common import YtError
from yt.environment.helpers import assert_items_equal
import yt.yson as yson

import pytest

from functools import partial
import time
import builtins


##################################################################


class TestSchedulerRemoteCopyCommandsBase(YTEnvSetup):
    NUM_TEST_PARTITIONS = 5

    NUM_MASTERS = 1
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyCommandsBase, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)


##################################################################


class TestSchedulerRemoteCopyCommands(TestSchedulerRemoteCopyCommandsBase):
    @authors("ignat")
    def test_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == []
        assert not get("//tmp/t2/@sorted")

    @authors("ignat")
    def test_non_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert not get("//tmp/t2/@sorted")

    @authors("asaitgalin", "babenko")
    def test_schema_inference(self):
        schema = make_schema(
            [{"name": "a", "type": "string", "required": False}],
            strict=True,
            unique_keys=False,
        )

        create(
            "table",
            "//tmp/t1",
            attributes={"schema": schema},
            driver=self.remote_driver,
        )

        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")
        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert normalize_schema(get("//tmp/t2/@schema")) == schema
        assert get("//tmp/t2/@schema_mode") == "strong"

        create(
            "table",
            "//tmp/t3",
            attributes={"schema": [{"name": "b", "type": "string"}]},
        )

        with pytest.raises(YtError):
            # To do remote copy into table with "strong" schema mode schemas must be identical.
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t3",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        with pytest.raises(YtError):
            # To do remote copy into table with "strong" schema mode schemas must be identical.
            # Even if we force scheduler to infer schema from output.
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t3",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "schema_inference_mode": "from_output",
                },
            )

    @authors("ermolovd")
    def test_schema_validation_complex_types(self):
        input_schema = make_schema(
            [
                {"name": "index", "type_v3": "int64"},
                {"name": "value", "type_v3": optional_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )
        output_schema = make_schema(
            [
                {"name": "index", "type_v3": "int64"},
                {"name": "value", "type_v3": list_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )

        create(
            "table",
            "//tmp/input",
            attributes={"schema": input_schema},
            driver=self.remote_driver,
        )
        create("table", "//tmp/output", attributes={"schema": output_schema})
        write_table(
            "//tmp/input",
            [
                {"index": 1, "value": [None]},
                {"index": 2, "value": ["foo"]},
            ],
            driver=self.remote_driver,
        )

        # We check that yson representation of types are compatible with each other
        write_table("//tmp/output", read_table("//tmp/input", driver=self.remote_driver))

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/input",
                out="//tmp/output",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "schema_inference_mode": "auto",
                },
            )
        remote_copy(
            in_="//tmp/input",
            out="//tmp/output",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "schema_inference_mode": "from_input",
            },
        )
        assert normalize_schema_v3(input_schema) == normalize_schema_v3(get("//tmp/output/@schema"))

    @authors("ignat")
    def test_cluster_connection_config(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        cluster_connection = get("//sys/clusters/" + self.REMOTE_CLUSTER_NAME)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_connection": cluster_connection},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("ignat")
    def test_multi_chunk_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert sorted_dicts(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    @authors("ignat")
    def test_multi_chunk_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        for value in range(10):
            write_table(
                "<append=true;sorted_by=[a]>//tmp/t1",
                {"a": value},
                driver=self.remote_driver,
            )

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "job_count": 10},
        )

        assert read_table("//tmp/t2") == [{"a": value} for value in range(10)]
        assert get("//tmp/t2/@chunk_count") == 10

    @authors("ignat")
    def test_multiple_jobs(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "job_count": 2},
        )

        assert sorted_dicts(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    @authors("ignat")
    def test_heterogenius_chunk_in_one_job(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}, {"c": "d"}]

    @authors("ignat")
    def test_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table(
            "//tmp/t1",
            [{"a": "b"}, {"a": "c"}],
            sorted_by="a",
            driver=self.remote_driver,
        )

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}, {"a": "c"}]
        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["a"]

    @authors("ignat")
    def test_erasure_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    # COMPAT(kvk1920)
    def _enable_maintenance_flag_set(self):
        path = "//sys/@config/node_tracker/forbid_maintenance_attribute_writes"
        local = get(path)
        remote = get(path, driver=self.remote_driver)
        set(path, False)
        set(path, False, driver=self.remote_driver)
        return local, remote

    def _restore_maintenance_flag_config(self, old_value):
        local, remote = old_value
        path = "//sys/@config/node_tracker/forbid_maintenance_attribute_writes"
        set(path, local)
        set(path, remote, driver=self.remote_driver)

    @authors("ignat")
    def test_chunk_scraper(self):
        old_value = self._enable_maintenance_flag_set()
        try:
            create("table", "//tmp/t1", driver=self.remote_driver)
            set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
            write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

            chunk_id = get("//tmp/t1/@chunk_ids/0", driver=self.remote_driver)
            chunk_replicas = get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)
            node = list(str(r) for r in chunk_replicas if r.attributes["index"] == 0)[0]

            set(
                "//sys/@config/chunk_manager/enable_chunk_replicator",
                False,
                driver=self.remote_driver,
            )
            multicell_sleep()

            set_node_banned(node, True, driver=self.remote_driver)

            wait(lambda: not get("#{}/@available".format(chunk_id), driver=self.remote_driver))

            create("table", "//tmp/t2")
            op = remote_copy(
                track=False,
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "unavailable_chunk_strategy": "wait",
                    "network_name": "interconnect",
                },
            )

            set_node_banned(node, False, driver=self.remote_driver)

            wait(lambda: get("#{}/@available".format(chunk_id), driver=self.remote_driver))

            op.track()

            assert read_table("//tmp/t2") == [{"a": "b"}]
        finally:
            self._restore_maintenance_flag_config(old_value)

    @authors("ignat")
    def test_revive(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        op = remote_copy(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_io": {
                    "table_writer": {
                        "testing_delay": 5000,
                    }
                },
            },
        )

        wait(lambda: op.get_state() == "running")
        wait(lambda: exists(op.get_path() + "/snapshot"))

        input_tx = get(op.get_path() + "/@input_transaction_id")

        with Restarter(self.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE]):
            time.sleep(1)

        op.track()

        assert input_tx == get(op.get_path() + "/@input_transaction_id")

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("ignat")
    def test_revive_with_specified_connection(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table(
            "//tmp/t1",
            [{"a": i} for i in range(10)],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
            driver=self.remote_driver,
        )

        create("table", "//tmp/t2")

        clusters = get("//sys/clusters")
        cluster_connection = clusters[self.REMOTE_CLUSTER_NAME]
        try:
            set("//sys/clusters", {})
            # TODO(babenko): wait for cluster sync
            time.sleep(2)
            op = remote_copy(
                track=False,
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_connection": cluster_connection, "job_count": 10, "resource_limits": {"user_slots": 1}},
            )

            wait(lambda: op.get_state() == "running")
            wait(lambda: exists(op.get_path() + "/snapshot"))

            input_tx = get(op.get_path() + "/@input_transaction_id")

            with Restarter(self.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE]):
                time.sleep(1)

            op.track()

            assert input_tx != get(op.get_path() + "/@input_transaction_id")
        finally:
            set("//sys/clusters", clusters)
            # TODO(babenko): wait for cluster sync
            time.sleep(2)

        assert read_table("//tmp/t2") == [{"a": i} for i in range(10)]

    @authors("ignat")
    def test_failed_cases(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "unexisting"})

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "network_name": "unexisting",
                },
            )

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/unexisting",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        write_table("//tmp/t1", [{"a": "b"}, {"c": "d"}], driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1[:#1]",
                out="//tmp/unexisting",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

    @authors("asaitgalin", "ignat")
    def test_acl(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        create_user("u")
        create_user("u", driver=self.remote_driver)

        multicell_sleep()

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            authenticated_user="u",
        )

        set(
            "//tmp/t1/@acl/end",
            make_ace("deny", "u", "read"),
            driver=self.remote_driver,
        )
        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
                authenticated_user="u",
            )
        set("//tmp/t1/@acl", [], driver=self.remote_driver)

        set(
            "//sys/schemas/transaction/@acl/end",
            make_ace("deny", "u", "create"),
            driver=self.remote_driver,
        )
        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
                authenticated_user="u",
            )

    @authors("asaitgalin", "ignat")
    def test_copy_attributes(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        set("//tmp/t1/@custom_attr1", "attr_value1", driver=self.remote_driver)
        set("//tmp/t1/@custom_attr2", "attr_value2", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True},
        )

        assert get("//tmp/t2/@custom_attr1") == "attr_value1"
        assert get("//tmp/t2/@custom_attr2") == "attr_value2"

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t3",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "copy_attributes": True,
                "attribute_keys": ["custom_attr2"],
            },
        )

        assert not exists("//tmp/t3/@custom_attr1")
        assert get("//tmp/t3/@custom_attr2") == "attr_value2"

        with pytest.raises(YtError):
            remote_copy(
                in_=["//tmp/t1", "//tmp/t1"],
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "copy_attributes": True,
                },
            )

    @authors("asaitgalin", "savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_copy_strict_schema(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t1",
            driver=self.remote_driver,
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "string", "sort_order": sort_order},
                        {"name": "b", "type": "string"},
                    ],
                    unique_keys=True,
                )
            },
        )
        assert get("//tmp/t1/@schema_mode", driver=self.remote_driver) == "strong"

        create("table", "//tmp/t2")

        rows = [{"a": "x", "b": "v"}, {"a": "y", "b": "v"}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows, driver=self.remote_driver)

        assert get("//tmp/t1/@schema_mode", driver=self.remote_driver) == "strong"

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == rows
        assert get("//tmp/t2/@schema/@strict")
        assert get("//tmp/t2/@schema/@unique_keys")
        assert get("//tmp/t2/@schema_mode") == "strong"

    @authors("gritukan")
    @pytest.mark.timeout(300)
    def test_erasure_repair(self):
        old_value = self._enable_maintenance_flag_set()
        try:
            create("table", "//tmp/t1", driver=self.remote_driver)
            create("table", "//tmp/t2")
            set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)

            content = [{"key": i, "value": "x" * 1024} for i in range(12)]
            write_table("//tmp/t1",
                        content,
                        table_writer={"block_size": 1024},
                        driver=self.remote_driver)

            set(
                "//sys/@config/chunk_manager/enable_chunk_replicator",
                False,
                driver=self.remote_driver,
            )
            set("//sys/@config/chunk_manager/enable_chunk_replicator", False)
            multicell_sleep()

            chunk_id = get("//tmp/t1/@chunk_ids/0", driver=self.remote_driver)

            def run_operation():
                op = remote_copy(
                    track=False,
                    in_="//tmp/t1",
                    out="//tmp/t2",
                    spec={
                        "cluster_name": self.REMOTE_CLUSTER_NAME,
                        "max_failed_job_count": 1,
                        "delay_in_copy_chunk": 5000,
                        "erasure_chunk_repair_delay": 2000,
                        "repair_erasure_chunks": True,
                        "chunk_availability_policy": "repairable",
                    },
                )
                wait(lambda: len(op.get_running_jobs()) == 1)
                return op

            def check_everything():
                assert get("//tmp/t2/@chunk_count") == 1
                if self.Env.get_component_version("ytserver-job-proxy").abi > (21, 3):
                    new_chunk_id = get("//tmp/t2/@chunk_ids/0")
                    new_chunk_replicas = get("#{}/@stored_replicas".format(new_chunk_id))
                    replicas = [str(r) for r in new_chunk_replicas]
                    assert len(replicas) == 9 and len({r for r in replicas}) == 9
                assert read_table("//tmp/t2") == content

            chunk_replicas = get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)

            def set_banned_flag_for_part_nodes(part_indices, banned_flag):
                nodes_to_ban = []
                for part_index in part_indices:
                    nodes = list(str(r) for r in chunk_replicas if r.attributes["index"] == part_index)
                    nodes_to_ban += nodes

                set_nodes_banned(nodes_to_ban, banned_flag, driver=self.remote_driver)

            def unban_all_nodes():
                set_all_nodes_banned(False, driver=self.remote_driver)
                wait(lambda: get("#{}/@available".format(chunk_id), driver=self.remote_driver))

            op = run_operation()
            # Some 3 parts are unavailable.
            # NB(gritukan): Cannot ban node before job started because CA will not start job until
            # all the parts were found.
            set_banned_flag_for_part_nodes([2, 3, 5], True)
            op.track()
            check_everything()
            unban_all_nodes()

            op = run_operation()
            # Some 4 parts are unavailable, repair is impossible.
            set_banned_flag_for_part_nodes([0, 1, 3, 8], True)
            time.sleep(8)
            get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)
            assert op.get_state() not in ("failed", "aborted", "completed")
            # Unban one part, job should complete.
            set_banned_flag_for_part_nodes([1], False)
            get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)
            op.track()
            check_everything()
            unban_all_nodes()
        finally:
            self._restore_maintenance_flag_config(old_value)

    @authors("ignat")
    def test_multiple_tables_are_not_supported(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write_table("//tmp/t2", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/output")

        with pytest.raises(YtError):
            remote_copy(
                in_=["//tmp/t1", "//tmp/t2"],
                out="//tmp/output",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

    @authors("egor-gutrov", "eshcherbin")
    def test_user_slots_validation(self):
        update_pool_tree_config_option("default", "fail_remote_copy_on_missing_resource_limits", True)
        update_pool_tree_config_option("default", "required_resource_limits_for_remote_copy", {"user_slots": 10})

        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "resource_limits": {"user_slots": 11},
                },
            )

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "resource_limits": {"user_slots": 10},
                },
            )

        create_pool("cool_pool", attributes={"resource_limits": {"user_slots": 10}})
        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "pool": "cool_pool",
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert not get("//tmp/t2/@sorted")

        create_pool("limitless_pool")
        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "pool": "limitless_pool",
                },
            )

    @authors("egor-gutrov")
    def test_auto_create(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                },
            )

        remote_copy(
            in_="//tmp/t1",
            out="<create=true>//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert not get("//tmp/t2/@sorted")

        create("map_node", "//tmp/t3")
        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="<create=true>//tmp/t3",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                }
            )

    @authors("akozhikhov")
    def test_seed_replicas(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_io": {
                    "table_reader": {
                        # Forbid reader from locating seeds and populating node directory.
                        "retry_count": 1,
                    }
                },
            },
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def _create_inout_files(self, chunks, **remote_attributes):
        create("file", "//tmp/in.txt", attributes=remote_attributes, driver=self.remote_driver)
        create("file", "//tmp/out.txt")

        for _ in range(chunks):
            write_file("<append=%true>//tmp/in.txt", b"remote_text.", driver=self.remote_driver)

        assert get("//tmp/in.txt/@chunk_count", driver=self.remote_driver) == chunks

    @authors("coteeq")
    @pytest.mark.parametrize("chunks", [1, 2])
    def test_copy_file(self, chunks):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        self._create_inout_files(chunks)

        remote_content = read_file("//tmp/in.txt", driver=self.remote_driver)

        op = remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        jobs_count = op.get_statistics()["data"]["input"]["data_weight"][0]["summary"]["count"]
        assert jobs_count == 1

        assert read_file("//tmp/out.txt") == remote_content
        assert get("//tmp/out.txt/@chunk_count") == chunks

    @authors("coteeq")
    def test_copy_file_create_destination(self):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        self._create_inout_files(2)
        remove("//tmp/out.txt")

        remote_content = read_file("//tmp/in.txt", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/in.txt",
            out="<create=%true>//tmp/out.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        assert read_file("//tmp/out.txt") == remote_content
        assert get("//tmp/out.txt/@chunk_count") == 2

    @authors("coteeq")
    @pytest.mark.parametrize("constraint", ["job_count", "data_weight"])
    def test_copy_file_job_constraints(self, constraint):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        self._create_inout_files(4)

        remote_content = read_file("//tmp/in.txt", driver=self.remote_driver)

        if constraint == "data_weight":
            spec_patch = {"data_weight_per_job": len(remote_content) // 2}
        else:
            spec_patch = {"job_count": 2}

        op = remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                **spec_patch,
            }
        )

        assert op.get_job_count("completed") == 2

        assert read_file("//tmp/out.txt") == remote_content

    @authors("coteeq")
    def test_copy_file_copy_attributes(self):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        self._create_inout_files(1)
        create("file", "//tmp/out2.txt")

        set("//tmp/in.txt/@custom_attr1", "attr_value1", driver=self.remote_driver)
        set("//tmp/in.txt/@custom_attr2", "attr_value2", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True},
        )

        assert get("//tmp/out.txt/@custom_attr1") == "attr_value1"
        assert get("//tmp/out.txt/@custom_attr2") == "attr_value2"

        remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out2.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "copy_attributes": True,
                "attribute_keys": ["custom_attr2"],
            },
        )

        assert not exists("//tmp/out2.txt/@custom_attr1")
        assert get("//tmp/out2.txt/@custom_attr2") == "attr_value2"

    @authors("coteeq")
    def test_remote_copy_file_codecs(self):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        compression = "lz4"
        erasure = "reed_solomon_6_3"
        self._create_inout_files(2, compression_codec=compression, erasure_codec=erasure)
        set("//tmp/out.txt/@compression_codec", compression)
        set("//tmp/out.txt/@erasure_codec", erasure)

        remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            },
        )

        assert read_file("//tmp/in.txt", driver=self.remote_driver) == read_file("//tmp/out.txt")
        assert get("//tmp/out.txt/@compression_codec") == compression
        assert get("//tmp/out.txt/@erasure_codec") == erasure
        for chunk_id in get("//tmp/out.txt/@chunk_ids"):
            assert get(f"#{chunk_id}/@compression_codec") == compression
            assert get(f"#{chunk_id}/@erasure_codec") == erasure

    @authors("coteeq")
    def test_remote_copy_types_of_objects(self):
        skip_if_old(self.Env, (23, 3), "no copy files in 23.2")
        create("file", "//tmp/file", driver=self.remote_driver)
        create("file", "//tmp/file2", driver=self.remote_driver)
        create("table", "//tmp/table", driver=self.remote_driver)
        create("table", "//tmp/table2", driver=self.remote_driver)
        create("document", "//tmp/document", driver=self.remote_driver)

        create("file", "//tmp/file")
        create("file", "//tmp/file2")
        create("table", "//tmp/table")
        create("table", "//tmp/table2")
        create("document", "//tmp/document")

        def check(src, dst, allow=True, error=None):
            op = remote_copy(
                track=False,
                in_=src,
                out=dst,
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME}
            )
            if allow:
                op.track()
            else:
                with raises_yt_error(error):
                    op.track()

        allow = partial(check, allow=True)
        disallow = partial(check, allow=False)

        allow(["//tmp/file"], "//tmp/file")
        disallow(["//tmp/file", "//tmp/file2"], "//tmp/file")
        allow(["//tmp/table"], "//tmp/table")
        disallow(["//tmp/table", "//tmp/table2"], "//tmp/table")
        disallow([], "//tmp/table")

        disallow(["//tmp/file"], "<compression_codec=zstd_6>//tmp/file", error="disallowed for files")
        disallow(["//tmp/file"], "<erasure_codec=reed_solomon_6_3>//tmp/file", error="disallowed for files")

        disallow(["//tmp/table"], "//tmp/file", error="Output object type does not match that of the input object")
        disallow(["//tmp/file"], "//tmp/table", error="Output object type does not match that of the input object")

        disallow(["//tmp/document"], "//tmp/document", error="Only files and tables are allowed")

    @authors("coteeq")
    def test_remote_copy_restrict_attributes(self):
        skip_if_old(self.Env, (23, 3), "no such logic in 23.2")
        create("table", "//tmp/in", driver=self.remote_driver)
        write_table("//tmp/in", [{"a": 1}, {"a": 2}], driver=self.remote_driver)

        spec = {
            "cluster_name": self.REMOTE_CLUSTER_NAME,
            "restrict_destination_ypath_attributes": True,
        }

        with raises_yt_error("Found unexpected attribute \"user_attribute\""):
            remote_copy(in_="//tmp/in", out="<create=%true;user_attribute=42>//tmp/out", spec=spec)

        remote_copy(
            in_="//tmp/in",
            out="<create=%true;optimize_for=scan;compression_codec=zstd_10;format=whatever>//tmp/out",
            spec=spec,
        )

        assert read_table("//tmp/out") == [{"a": 1}, {"a": 2}]


##################################################################


class TestSchedulerRemoteCopyNetworks(TestSchedulerRemoteCopyCommandsBase):
    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["addresses"].append(["custom_network", dict(config["addresses"])["default"]])

    @authors("ignat")
    def test_default_network(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("ignat")
    def test_custom_network(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "network_name": "custom_network",
            },
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("egor-gutrov")
    def test_network_preference_list(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "networks": ["unexisting"],
                },
            )

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "networks": ["default"],
                    "network_name": "default",
                },
            )

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "networks": ["unexisting", "custom_network"],
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "networks": ["custom_network", "unexisting"],
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "network_names": ["custom_network", "unexisting"],
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("ignat")
    def test_network_with_spec_template(self):
        update_controller_agent_config("remote_copy_operation_options/networks", ["undefined"])
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                },
            )

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "network_name": "custom_network",
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "networks": ["custom_network"],
            },
        )
        assert read_table("//tmp/t2") == [{"a": "b"}]


##################################################################


class TestSchedulerRemoteCopyCommandsMulticell(TestSchedulerRemoteCopyCommands):
    NUM_TEST_PARTITIONS = 6
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


class TestSchedulerRemoteCopyDynamicTables(TestSchedulerRemoteCopyCommandsBase):
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    def _create_sorted_table(self, path, driver=None, **attributes):
        if "schema" not in attributes:
            schema = yson.YsonList([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
            schema.attributes["unique_keys"] = True
            attributes.update({"schema": schema})
        if "dynamic" not in attributes:
            attributes["dynamic"] = True
        create("table", path, driver=driver, attributes=attributes)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("from_static", [True, False])
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_copy_sorted_dynamic_table(self, optimize_for, from_static):
        sync_create_cells(1)
        sync_create_cells(1, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2", optimize_for=optimize_for)
        update_controller_agent_config("enable_bulk_insert_for_everyone", False)
        assert not exists("//sys/users/root/@enable_bulk_insert")

        rows = [{"key": i, "value": str(i)} for i in range(10)]

        self._create_sorted_table(
            "//tmp/t1",
            optimize_for=optimize_for,
            dynamic=not from_static,
            driver=self.remote_driver)

        if from_static:
            write_table("//tmp/t1", rows, driver=self.remote_driver)
            alter_table("//tmp/t1", dynamic=True, driver=self.remote_driver)
        else:
            sync_mount_table("//tmp/t1", driver=self.remote_driver)
            insert_rows("//tmp/t1", rows[::2], driver=self.remote_driver)
            insert_rows("//tmp/t1", rows[1::2], driver=self.remote_driver)
            sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == rows
        src_chunk = get("//tmp/t1/@chunk_ids/0", driver=self.remote_driver)
        dst_chunk = get("//tmp/t2/@chunk_ids/0")
        attr_list = [
            "optimize_for",
            "table_chunk_format",
            "min_timestamp",
            "max_timestamp",
            "min_key",
            "max_key",
        ]
        src_attrs = get("#{}/@".format(src_chunk), attributes=attr_list, driver=self.remote_driver)
        dst_attrs = get("#{}/@".format(dst_chunk), attributes=attr_list)
        assert src_attrs == dst_attrs

        sync_mount_table("//tmp/t2")
        assert_items_equal(select_rows("* from [//tmp/t2]"), rows)

    @authors("ifsmirnov")
    @pytest.mark.parametrize(
        ["src_pivots", "dst_pivots"],
        [
            [[0], [0, 5]],
            [[0, 2, 3, 7], [0, 5]],
            [[0, 5], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]
        ])
    def test_multiple_tablets(self, src_pivots, dst_pivots):
        sync_create_cells(1, driver=self.remote_driver)

        self._create_sorted_table(
            "//tmp/t2",
            pivot_keys=[[]] + [[x] for x in dst_pivots[1:]])
        self._create_sorted_table(
            "//tmp/t1",
            pivot_keys=[[]] + [[x] for x in src_pivots[1:]],
            driver=self.remote_driver)

        rows = [{"key": i, "value": str(i)} for i in range(15)]
        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", rows, driver=self.remote_driver)
        sync_compact_table("//tmp/t1", driver=self.remote_driver)
        sync_freeze_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == rows

        # Check that tablet chunk lists contain expected chunks.
        root_chunk_list_id = get("//tmp/t2/@chunk_list_id")
        tree = get("#{}/@tree".format(root_chunk_list_id))
        src_pivots.append(100)
        dst_pivots.append(100)
        for tablet_index in range(len(tree)):
            lhs = dst_pivots[tablet_index]
            rhs = dst_pivots[tablet_index + 1]
            expected_chunk_count = 0
            for i in range(len(src_pivots) - 1):
                if lhs < src_pivots[i + 1] and rhs > src_pivots[i]:
                    expected_chunk_count += 1
            assert len(tree[tablet_index]) == expected_chunk_count
            assert all(child.attributes.get("type", "chunk") == "chunk" for child in tree[tablet_index])

    @authors("ifsmirnov")
    def test_versions_preserved(self):
        sync_create_cells(1, driver=self.remote_driver)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2")

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", [{"key": 1, "value": "foo"}], driver=self.remote_driver)
        delete_rows("//tmp/t1", [{"key": 1}], driver=self.remote_driver)
        insert_rows("//tmp/t1", [{"key": 1, "value": "bar"}], driver=self.remote_driver)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t2")
        assert\
            lookup_rows("//tmp/t1", [{"key": 1}], versioned=True, driver=self.remote_driver) ==\
            lookup_rows("//tmp/t2", [{"key": 1}], versioned=True)

    @authors("ifsmirnov")
    def test_invalid_input_chunks(self):
        sync_create_cells(1, driver=self.remote_driver)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2")

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", [{"key": 0}, {"key": 1}], driver=self.remote_driver)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        # Chunk crosses tablet boundaries and is under nontrivial chunk view.
        sync_reshard_table("//tmp/t1", [[], [1]], driver=self.remote_driver)
        with raises_yt_error(yt_error_codes.InvalidInputChunk):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        # Chunk is under bulk-inserted chunk view with timestamp.
        self._create_sorted_table("//tmp/t3")
        sync_mount_table("//tmp/t3")
        insert_rows("//tmp/t3", [{"key": 1}])
        sync_freeze_table("//tmp/t3")
        merge(in_="//tmp/t3", out="//tmp/t3", mode="ordered")
        sync_unmount_table("//tmp/t3")
        with raises_yt_error(yt_error_codes.InvalidInputChunk):
            remote_copy(
                in_="//tmp/t3",
                out="//tmp/t2",
                spec={"cluster_name": "primary"},
            )

    @authors("ifsmirnov")
    def test_invalid_tables(self):
        sync_create_cells(1)

        def _run_remote_copy(src, dst):
            remote_copy(
                in_=src,
                out=dst,
                spec={"cluster_name": "primary"}
            )

        self._create_sorted_table("//tmp/dynamic1")
        self._create_sorted_table("//tmp/dynamic2")
        self._create_sorted_table("//tmp/static", dynamic=False)

        # Cannot copy static table into dynamic and vice versa.
        with raises_yt_error():
            _run_remote_copy("//tmp/dynamic1", "//tmp/static")
        with raises_yt_error():
            _run_remote_copy("//tmp/static", "//tmp/dynamic1")

        # Cannot have several dynamic tables in the input.
        with raises_yt_error():
            _run_remote_copy(["//tmp/dynamic1", "//tmp/dynamic1"], "//tmp/dynamic2")

        # Input table should be frozen or unmounted.
        sync_mount_table("//tmp/dynamic1")
        with raises_yt_error():
            _run_remote_copy("//tmp/dynamic1", "//tmp/dynamic2")
        sync_freeze_table("//tmp/dynamic1")
        _run_remote_copy("//tmp/dynamic1", "//tmp/dynamic2")

        # Output table should be unmounted.
        with raises_yt_error():
            _run_remote_copy("//tmp/dynamic2", "//tmp/dynamic1")
        sync_unmount_table("//tmp/dynamic1")
        _run_remote_copy("//tmp/dynamic2", "//tmp/dynamic1")

        # Ordered tables are not supported.
        self._create_sorted_table("//tmp/ordered", schema=[{"name": "key", "type": "int64"}])
        with raises_yt_error():
            _run_remote_copy("//tmp/ordered", "//tmp/ordered")

    @authors("ifsmirnov")
    def test_self_cluster(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        self._create_sorted_table("//tmp/t2")
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key": 1, "value": "foo"}])
        sync_unmount_table("//tmp/t1")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": "primary"},
        )
        assert read_table("//tmp/t2") == [{"key": 1, "value": "foo"}]

        remove("//tmp/t1")
        self._create_sorted_table("//tmp/t1")
        remote_copy(
            in_="//tmp/t2",
            out="//tmp/t1",
            spec={"cluster_name": "primary"},
        )
        assert read_table("//tmp/t1") == [{"key": 1, "value": "foo"}]

    @authors("ifsmirnov")
    def test_erasure(self):
        sync_create_cells(1, driver=self.remote_driver)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2")

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", [{"key": 1, "value": "foo"}], driver=self.remote_driver)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        assert read_table("//tmp/t2") == [{"key": 1, "value": "foo"}]
        chunk_id = get("//tmp/t2/@chunk_ids/0")
        assert get("#{}/@erasure_codec".format(chunk_id)) == "reed_solomon_6_3"

    @authors("ifsmirnov")
    def test_multiple_jobs(self):
        sync_create_cells(1)
        sync_create_cells(1, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2")

        rows = [{"key": i, "value": str(i)} for i in range(10)]

        self._create_sorted_table("//tmp/t1", driver=self.remote_driver)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", rows[::2], driver=self.remote_driver)
        sync_flush_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", rows[1::2], driver=self.remote_driver)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        op = remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_count": 2,
            }
        )

        print_debug(f"Job count: {op.get_job_count('completed')}")

        assert read_table("//tmp/t2") == rows

    # TODO(ifsmirnov): YT-20044
    @authors("ifsmirnov")
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_no_hunks(self, dynamic):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 1},
        ]
        self._create_sorted_table("//tmp/t1", schema=schema, dynamic=dynamic, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2", schema=schema, dynamic=dynamic)

        with raises_yt_error():
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "bypass_hunk_remote_copy_prohibition": True,
            }
        )


##################################################################


class TestSchedulerRemoteCopyDynamicTablesMulticell(TestSchedulerRemoteCopyDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("ifsmirnov")
    def test_remote_copy_from_primary_cell_to_secondary(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", external=False)
        self._create_sorted_table("//tmp/t2")
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key": 1, "value": "foo"}])
        sync_unmount_table("//tmp/t1")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": "primary"},
        )

        assert not\
            builtins.set(get("//tmp/t1/@chunk_ids")) &\
            builtins.set(get("//tmp/t2/@chunk_ids"))
        remove("//tmp/t1")
        assert get("//tmp/t2/@external")
        assert read_table("//tmp/t2") == [{"key": 1, "value": "foo"}]
        sync_mount_table("//tmp/t2")
        assert select_rows("* from [//tmp/t2]") == [{"key": 1, "value": "foo"}]
