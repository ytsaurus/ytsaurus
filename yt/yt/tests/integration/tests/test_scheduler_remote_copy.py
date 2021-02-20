import pytest

from yt_env_setup import (
    YTEnvSetup,
    wait,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from yt_helpers import skip_if_no_descending

import time

import __builtin__


##################################################################


class TestSchedulerRemoteCopyCommandsBase(YTEnvSetup):
    NUM_TEST_PARTITIONS = 4

    NUM_MASTERS = 1
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyCommandsBase, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)


##################################################################


class TestSchedulerRemoteCopyCommands(TestSchedulerRemoteCopyCommandsBase):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

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
            # To do remote copy into table with "stong" schema mode schemas must be identical.
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t3",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

        with pytest.raises(YtError):
            # To do remote copy into table with "stong" schema mode schemas must be identical.
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

        assert sorted(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    @authors("ignat")
    def test_multi_chunk_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        for value in xrange(10):
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

        assert read_table("//tmp/t2") == [{"a": value} for value in xrange(10)]
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

        assert sorted(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
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

    @authors("ignat")
    def test_chunk_scraper(self):
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

        set_banned_flag(True, [node], driver=self.remote_driver)

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

        set_banned_flag(False, [node], driver=self.remote_driver)

        wait(lambda: get("#{}/@available".format(chunk_id), driver=self.remote_driver))

        op.track()

        assert read_table("//tmp/t2") == [{"a": "b"}]

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
            [{"a": i} for i in xrange(100)],
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
                spec={"cluster_connection": cluster_connection, "job_count": 100},
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

        assert read_table("//tmp/t2") == [{"a": i} for i in xrange(100)]

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
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

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
                },
            )
            wait(lambda: len(op.get_running_jobs()) == 1)
            return op

        def check_everything():
            assert get("//tmp/t2/@chunk_count") == 1
            assert read_table("//tmp/t2") == [{"a": "b"}]

        def set_banned_flag_for_part_nodes(part_indicies, banned_flag):
            chunk_replicas = get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)

            nodes_to_ban = []
            for part_index in part_indicies:
                nodes = list(str(r) for r in chunk_replicas if r.attributes["index"] == part_index)
                nodes_to_ban += nodes

            set_banned_flag(banned_flag, nodes_to_ban, driver=self.remote_driver)

        def unban_all_nodes():
            nodes = list(get("//sys/cluster_nodes", driver=self.remote_driver).keys())
            set_banned_flag(False, nodes, driver=self.remote_driver)
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
        # Job freezes.
        assert len(op.get_running_jobs()) == 1
        # Unban one part, job should complete.
        set_banned_flag_for_part_nodes([1], False)
        op.track()
        check_everything()
        unban_all_nodes()

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


##################################################################


class TestSchedulerRemoteCopyNetworks(TestSchedulerRemoteCopyCommandsBase):
    @classmethod
    def modify_node_config(cls, config):
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


##################################################################


class TestSchedulerRemoteCopyCommandsMulticell(TestSchedulerRemoteCopyCommands):
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
        assert (
            lookup_rows("//tmp/t1", [{"key": 1}], versioned=True, driver=self.remote_driver) ==
            lookup_rows("//tmp/t2", [{"key": 1}], versioned=True))

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
        with raises_yt_error(InvalidInputChunk):
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
        with raises_yt_error(InvalidInputChunk):
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

        print_debug("job_count: {}".format(op.get_job_count("completed")))

        assert read_table("//tmp/t2") == rows


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

        assert not (
            __builtin__.set(get("//tmp/t1/@chunk_ids")) &
            __builtin__.set(get("//tmp/t2/@chunk_ids")))
        remove("//tmp/t1")
        assert get("//tmp/t2/@external")
        assert read_table("//tmp/t2") == [{"key": 1, "value": "foo"}]
        sync_mount_table("//tmp/t2")
        assert select_rows("* from [//tmp/t2]") == [{"key": 1, "value": "foo"}]
