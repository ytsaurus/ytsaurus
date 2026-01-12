from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE
)

from yt_commands import (
    assert_statistics_v2, authors, print_debug, wait, create, get, set, remove,
    exists, create_user, disable_tablet_cells_on_node, get_job,
    make_ace, insert_rows, select_rows, lookup_rows, delete_rows, alter_table, read_table, write_table, merge,
    remote_copy, sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table,
    sync_reshard_table, sync_flush_table, sync_compact_table, remount_table,
    multicell_sleep, set_node_banned, set_nodes_banned, set_all_nodes_banned, sorted_dicts,
    raises_yt_error, get_driver, ls, disable_write_sessions_on_node,
    create_dynamic_table,
    update_controller_agent_config, remember_controller_agent_config,
    write_file, wait_for_nodes, read_file, get_singular_chunk_id)

from yt_sequoia_helpers import not_implemented_in_sequoia

from yt_helpers import skip_if_component_old, profiler_factory
from yt_type_helpers import make_column, make_schema, normalize_schema, normalize_schema_v3, optional_type, list_type
import yt_error_codes

from yt.common import YtError
from yt.common import YtResponseError
from yt.environment.helpers import assert_items_equal
import yt.yson as yson

import pytest

from functools import partial
import time
import datetime
import builtins
from math import ceil
from copy import deepcopy

HUNK_COMPATIBLE_CHUNK_FORMATS = [
    "table_versioned_simple",
    "table_versioned_columnar",
    "table_versioned_slim",
    "table_versioned_indexed",
]

##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyCommandsBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
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

    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "21": {"roles": ["chunk_host", "cypress_node_host"]},
    }

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyCommandsBase, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)


##################################################################


class TestSchedulerRemoteCopyCommands(TestSchedulerRemoteCopyCommandsBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    DELTA_LOCAL_YT_CONFIG = {
        "node_network_names": ["default", "interconnect"],
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

    @authors("aleksandra-zh")
    def test_remote_copy_forbidden_erasure_codecs(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        set("//sys/@config/chunk_manager/forbidden_erasure_codecs", [1])  # forbid reed_solomon_6_3
        multicell_sleep()

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

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

            chunk_id = get_singular_chunk_id("//tmp/t1", driver=self.remote_driver)
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
            clusters_without_remote = deepcopy(clusters)
            del clusters_without_remote[self.REMOTE_CLUSTER_NAME]
            set("//sys/clusters", clusters_without_remote)
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                # NB(coteeq): This will clear scheduler's internal cache of connections.
                # And also scheduler will reconfigure cluster directory.
                pass

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
                transactions_to_check = [
                    "input_transaction_id",
                    "output_transaction_id",
                ]
                if self.Env.get_component_version("ytserver-controller-agent").abi >= (24, 1):
                    transactions_to_check.append("input_transaction_ids/0")
                for transaction in transactions_to_check:
                    path = op.get_path() + "/@" + transaction
                    assert exists(path)
                    assert get(path) != "0-0-0-0"

            op.track()

            assert input_tx == get(op.get_path() + "/@input_transaction_id")
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
    @not_implemented_in_sequoia
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

    @authors("coteeq")
    @pytest.mark.parametrize("copy_user_attributes", [True, False])
    @pytest.mark.parametrize("object_type", ["table", "file"])
    def test_copy_basic_attributes(self, copy_user_attributes, object_type):
        skip_if_component_old(self.Env, (25, 1), "controller-agent")
        create(object_type, "//tmp/t_in", driver=self.remote_driver)
        create(object_type, "//tmp/t_out")

        set("//tmp/t_in/@custom_attr", "attr_value", driver=self.remote_driver)

        ypath_attrs = [
            "compression_codec=lz4",
            "erasure_codec=reed_solomon_6_3",
        ]
        if object_type == "table":
            ypath_attrs.append("optimize_for=lookup")
            write_table(f"<{';'.join(ypath_attrs)}>//tmp/t_in", [{"column": "value"}], driver=self.remote_driver)
        else:
            write_file(f"<{';'.join(ypath_attrs)}>//tmp/t_in", b"asdf", driver=self.remote_driver)

        # Verify that attributes are set
        assert get("//tmp/t_in/@compression_codec", driver=self.remote_driver) == "lz4"
        assert get(
            "#{chunk_id}/@compression_codec".format(
                chunk_id=get_singular_chunk_id("//tmp/t_in", driver=self.remote_driver)
            ),
            driver=self.remote_driver,
        ) == "lz4"

        extra_spec = {}
        if self.Env.get_component_version("ytserver-controller-agent").abi < (25, 4):
            extra_spec = {"force_copy_system_attributes": True}

        remote_copy(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "copy_attributes": copy_user_attributes,
                "attribute_keys": ["custom_attr", "compression_codec"],
                **extra_spec,
            },
        )

        if copy_user_attributes:
            assert get("//tmp/t_out/@custom_attr") == "attr_value"
        else:
            assert not exists("//tmp/t_out/@custom_attr")

        assert get("//tmp/t_out/@compression_codec") == "lz4"
        assert get("//tmp/t_out/@erasure_codec") == "reed_solomon_6_3"
        if object_type == "table":
            assert get("//tmp/t_out/@optimize_for") == "lookup"

    @authors("asaitgalin", "ignat")
    def test_copy_attributes(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        set("//tmp/t1/@custom_attr1", "attr_value1", driver=self.remote_driver)
        set("//tmp/t1/@custom_attr2", "attr_value2", driver=self.remote_driver)
        set("//tmp/t1/@custom_attr3\\/1", "attr_value3", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True},
        )

        assert get("//tmp/t2/@custom_attr1") == "attr_value1"
        assert get("//tmp/t2/@custom_attr2") == "attr_value2"
        assert get("//tmp/t2/@custom_attr3\\/1") == "attr_value3"

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t3",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "copy_attributes": True,
                "attribute_keys": ["custom_attr2", "custom_attr3/1"],
            },
        )

        assert not exists("//tmp/t3/@custom_attr1")
        assert get("//tmp/t3/@custom_attr2") == "attr_value2"
        assert get("//tmp/t3/@custom_attr3\\/1") == "attr_value3"

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

            chunk_id = get_singular_chunk_id("//tmp/t1", driver=self.remote_driver)

            # COMPAT(coteeq)
            compat_repair_erasure_chunks = {}
            if self.Env.get_component_version("ytserver-controller-agent").abi < (26, 1):
                compat_repair_erasure_chunks["repair_erasure_chunks"] = True

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
                        "chunk_availability_policy": "repairable",
                        **compat_repair_erasure_chunks,
                    },
                )
                wait(lambda: len(op.get_running_jobs()) == 1)
                return op

            def check_everything():
                assert get("//tmp/t2/@chunk_count") == 1
                if self.Env.get_component_version("ytserver-job-proxy").abi > (21, 3):
                    new_chunk_id = get_singular_chunk_id("//tmp/t2")
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

    def _create_inout_files(self, chunks, content=b"remote_text", **remote_attributes):
        create("file", "//tmp/in.txt", attributes=remote_attributes, driver=self.remote_driver)
        create("file", "//tmp/out.txt")

        for _ in range(chunks):
            write_file("<append=%true>//tmp/in.txt", content, driver=self.remote_driver)

        assert get("//tmp/in.txt/@chunk_count", driver=self.remote_driver) == chunks

    @authors("coteeq")
    @pytest.mark.parametrize("chunks", [1, 2])
    def test_copy_file(self, chunks):
        self._create_inout_files(chunks)

        remote_content = read_file("//tmp/in.txt", driver=self.remote_driver)

        op = remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        def assert_job_count(job_count):
            assert job_count == 1

        assert_statistics_v2(
            op,
            "data.input.data_weight",
            assert_job_count,
            summary_type="count",
            job_type="remote_copy")

        assert read_file("//tmp/out.txt") == remote_content
        assert get("//tmp/out.txt/@chunk_count") == chunks

    @authors("coteeq")
    def test_copy_file_data_weight_per_job(self):
        skip_if_component_old(self.Env, (25, 1), "controller-agent")
        n_chunks = 3
        self._create_inout_files(n_chunks, content=b"content" * 100)

        remote_content = read_file("//tmp/in.txt", driver=self.remote_driver)

        with remember_controller_agent_config():
            update_controller_agent_config("remote_copy_operation_options/data_weight_per_job", 500)
            op = remote_copy(
                in_="//tmp/in.txt",
                out="//tmp/out.txt",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                }
            )

        def assert_job_count(job_count):
            assert job_count == n_chunks

        assert_statistics_v2(
            op,
            "data.input.data_weight",
            assert_job_count,
            summary_type="count",
            job_type="remote_copy")

        assert read_file("//tmp/out.txt") == remote_content
        assert get("//tmp/out.txt/@chunk_count") == n_chunks

    @authors("coteeq")
    def test_copy_file_create_destination(self):
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
        self._create_inout_files(1)
        create("file", "//tmp/out2.txt")

        set("//tmp/in.txt/@custom_attr1", "attr_value1", driver=self.remote_driver)
        set("//tmp/in.txt/@custom_attr2", "attr_value2", driver=self.remote_driver)
        set("//tmp/in.txt/@custom_attr3\\/1", "attr_value3", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out.txt",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True},
        )

        assert get("//tmp/out.txt/@custom_attr1") == "attr_value1"
        assert get("//tmp/out.txt/@custom_attr2") == "attr_value2"
        assert get("//tmp/out.txt/@custom_attr3\\/1") == "attr_value3"

        remote_copy(
            in_="//tmp/in.txt",
            out="//tmp/out2.txt",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "copy_attributes": True,
                "attribute_keys": ["custom_attr2", "custom_attr3/1"],
            },
        )

        assert not exists("//tmp/out2.txt/@custom_attr1")
        assert get("//tmp/out2.txt/@custom_attr2") == "attr_value2"
        assert get("//tmp/out.txt/@custom_attr3\\/1") == "attr_value3"

    @authors("coteeq")
    def test_remote_copy_file_codecs(self):
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

    @authors("coteeq")
    def test_copy_file_strange_ypaths(self):
        self._create_inout_files(chunks=1)

        strange_paths = [
            "//tmp/in.txt{column1}",
            "//tmp/in.txt[\"exact_key\"]",
            "//tmp/in.txt[#100500]",
            "//tmp/in.txt[#100:#500]",
            "//tmp/in.txt{column1}[#100500]",
        ]

        for path in strange_paths:
            with raises_yt_error("Input file path must not contain"):
                remote_copy(
                    in_=path,
                    out="//tmp/out.txt",
                    spec={
                        "cluster_name": self.REMOTE_CLUSTER_NAME,
                        "input_table_columnar_statistics": {
                            "enabled": True,
                        }
                    }
                )

    @authors("alexelexa")
    def test_dynamic_table_with_hunk_column(self):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 1},
        ]

        for path, driver in [("//tmp/t1", self.remote_driver), ("//tmp/t2", None)]:
            set("//sys/accounts/tmp/@resource_limits/tablet_count", 10, driver=driver)

            create("table", path, attributes=dict(dynamic=True, schema=schema), driver=driver)
            sync_create_cells(1, driver=driver)

        keys = [{"key": 1}]
        rows = [{"key": 1, "value": "foo"}]
        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        insert_rows("//tmp/t1", rows, driver=self.remote_driver)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "bypass_hunk_remote_copy_prohibition": True,
            }
        )

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t2")

        assert read_table("//tmp/t2") == rows
        assert (
            lookup_rows("//tmp/t1", keys, versioned=True, driver=self.remote_driver) ==
            lookup_rows("//tmp/t2", keys, versioned=True)
        )
        assert_items_equal(select_rows("* from [//tmp/t2]"), rows)

    @authors("coteeq")
    @pytest.mark.parametrize("abuse_via", ["ypath", "omit_inaccessible_columns"])
    @not_implemented_in_sequoia
    def test_columnar_acl(self, abuse_via):
        skip_if_component_old(self.Env, (25, 3), "controller-agent")
        for user in [
            "has_full_read",
            "has_only_public_read",
            "has_no_read",
        ]:
            create_user(user)
            create_user(user, driver=self.remote_driver)

        create(
            "table",
            "//tmp/t",
            attributes={
                "inherit_acl": False,
                "acl": [
                    make_ace("allow", ["has_full_read", "has_only_public_read"], "read"),
                    make_ace("allow", ["has_full_read"], "read", columns=["private"]),
                ],
                "schema": make_schema([
                    make_column("public", "string"),
                    make_column("private", "int64"),
                ])
            },
            driver=self.remote_driver
        )

        write_table("//tmp/t", [{"public": "what is the answer?", "private": 42}], driver=self.remote_driver)

        in_ = "//tmp/t" if abuse_via != "ypath" else "//tmp/t{public}"

        def do_copy(user):
            return remote_copy(
                in_=in_,
                out="<create=%true>//tmp/t",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "omit_inaccessible_columns": abuse_via == "omit_inaccessible_columns",
                },
                authenticated_user=user,
            )

        with raises_yt_error():
            do_copy("has_no_read")

        with raises_yt_error():
            do_copy("has_only_public_read")

        do_copy("has_full_read")

    @authors("coteeq")
    def test_chunk_reader_statistics(self):
        skip_if_component_old(self.Env, (25, 3), "controller-agent")
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a": "b"}], driver=self.remote_driver)

        op = remote_copy(
            in_=[
                "//tmp/t1",
            ],
            out="<create=%true>//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        assert_statistic = partial(
            assert_statistics_v2,
            op,
            job_type="remote_copy"
        )

        assert assert_statistic("chunk_reader_statistics.data_bytes_transmitted", lambda actual: actual is not None and actual > 0)
        # NB: remote_copy jobs do not operate on rows, so row count should be zero.
        assert assert_statistic("chunk_reader_statistics.row_count", lambda actual: actual is None)


##################################################################

class TestSchedulerRemoteCopyCommandsRevive(TestSchedulerRemoteCopyCommandsBase):
    REMOTE_TRANSACTION_COORDINATOR = hex(20)[2:]

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

        tx_cell_tag = self.REMOTE_TRANSACTION_COORDINATOR
        assert input_tx.split('-')[2] == tx_cell_tag + "0001"

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
            clusters_without_remote = deepcopy(clusters)
            del clusters_without_remote[self.REMOTE_CLUSTER_NAME]
            set("//sys/clusters", clusters_without_remote)
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

            assert input_tx == get(op.get_path() + "/@input_transaction_id")
        finally:
            set("//sys/clusters", clusters)
            # TODO(babenko): wait for cluster sync
            time.sleep(2)

        assert read_table("//tmp/t2") == [{"a": i} for i in range(10)]


##################################################################


class TestSchedulerRemoteCopyCommandsShardedTx(TestSchedulerRemoteCopyCommandsRevive):
    NUM_TEST_PARTITIONS = 2
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 2
    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "20": {"roles": ["cypress_node_host"]},
        "21": {"roles": ["transaction_coordinator"]},
        "22": {"roles": ["chunk_host"]},
    }

    REMOTE_TRANSACTION_COORDINATOR = hex(21)[2:]


##################################################################


class TestSchedulerRemoteCopyCommandsSequoiaRemote(TestSchedulerRemoteCopyCommands):
    USE_SEQUOIA = True
    USE_SEQUOIA_PRIMARY = False
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA_REMOTE_0 = True
    ENABLE_TMP_ROOTSTOCK_REMOTE_0 = True
    ENABLE_SYS_OPERATIONS_ROOTSTOCK_REMOTE_0 = True
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 3
    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "20": {"roles": ["cypress_node_host"]},
        "21": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "22": {"roles": ["chunk_host"]},
        "23": {"roles": ["sequoia_node_host"]}
    }
    REMOTE_TRANSACTION_COORDINATOR = hex(21)[2:]


class TestSchedulerRemoteCopyCommandsSequoiaPrimary(TestSchedulerRemoteCopyCommands):
    USE_SEQUOIA = True
    USE_SEQUOIA_REMOTE_0 = False
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA_PRIMARY = True
    ENABLE_TMP_ROOTSTOCK_PRIMARY = True
    ENABLE_SYS_OPERATIONS_ROOTSTOCK_PRIMARY = True
    NUM_SECONDARY_MASTER_CELLS_PRIMARY = 3
    MASTER_CELL_DESCRIPTORS_PRIMARY = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["sequoia_node_host"]}
    }


class TestSchedulerRemoteCopyCommandsSequoia(TestSchedulerRemoteCopyCommands):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    ENABLE_SYS_OPERATIONS_ROOTSTOCK = True
    NUM_SECONDARY_MASTER_CELLS_PRIMARY = 3
    MASTER_CELL_DESCRIPTORS_PRIMARY = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["sequoia_node_host"]}
    }
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 3
    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "20": {"roles": ["cypress_node_host"]},
        "21": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "22": {"roles": ["chunk_host"]},
        "23": {"roles": ["sequoia_node_host"]}
    }
    REMOTE_TRANSACTION_COORDINATOR = hex(21)[2:]


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyNetworks(TestSchedulerRemoteCopyCommandsBase):
    ENABLE_MULTIDAEMON = True

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
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 6
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTablesBase(TestSchedulerRemoteCopyCommandsBase):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    def _create_sorted_table(self, path, max_inline_hunk_size=None, driver=None, **attributes):
        if "schema" not in attributes:
            schema = yson.YsonList([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
            if max_inline_hunk_size is not None:
                schema[1]["max_inline_hunk_size"] = max_inline_hunk_size
            schema.attributes["unique_keys"] = True
            attributes.update({"schema": schema})
        if "dynamic" not in attributes:
            attributes["dynamic"] = True

        create("table", path, driver=driver, attributes=attributes)

    def _check_all_read_methods(self, source_path, dest_path, keys, rows, source_driver=None, dest_driver=None, lookup_source_result=None):
        assert read_table(dest_path, driver=dest_driver) == rows
        assert (
            lookup_source_result if lookup_source_result is not None else lookup_rows(source_path, keys, versioned=True, driver=source_driver) ==
            lookup_rows(dest_path, keys, versioned=True, driver=dest_driver)
        )
        assert_items_equal(select_rows(f"* from [{dest_path}]", driver=dest_driver), rows)

    def _get_chunk_ids(self, path, chunk_type, driver=None):
        for _ in range(5):
            try:
                chunk_ids = get("{}/@chunk_ids".format(path), driver=driver)
                return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id), driver=driver) == chunk_type]
            except YtError as err:
                if not err.is_resolve_error():
                    raise
        raise RuntimeError("Method _get_chunk_ids failed")


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTables(TestSchedulerRemoteCopyDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

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
        src_chunk = get_singular_chunk_id("//tmp/t1", driver=self.remote_driver)
        dst_chunk = get_singular_chunk_id("//tmp/t2")
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
        chunk_id = get_singular_chunk_id("//tmp/t2")
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


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTablesWithHunks(TestSchedulerRemoteCopyDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @authors("alexelexa")
    @pytest.mark.parametrize("max_inline_hunk_size", [1, 5, 10])
    def test_copy_sorted_table_with_hunks(self, max_inline_hunk_size):
        sync_create_cells(1)
        sync_create_cells(1, driver=self.remote_driver)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": max_inline_hunk_size},
        ]
        self._create_sorted_table("//tmp/t1", schema=schema, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2", schema=schema)

        set("//tmp/t1/@enable_compaction_and_partitioning", False, driver=self.remote_driver)
        sync_reshard_table("//tmp/t1", [[], [3]], driver=self.remote_driver)

        set("//tmp/t2/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t2", [[], [3]])

        rows = [{"key": i, "value": "foo" + str(i) * i} for i in range(5)]
        keys = [{"key" : dictionary["key"]} for dictionary in rows]

        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        for row in rows:
            insert_rows("//tmp/t1", [row], driver=self.remote_driver)
            sync_flush_table("//tmp/t1", driver=self.remote_driver)

        hunk_chunks_count = sum(map(lambda d: len(d["value"]) >= max_inline_hunk_size, rows))
        assert len(self._get_chunk_ids("//tmp/t1", "hunk", driver=self.remote_driver)) == hunk_chunks_count

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t2")

        def _check_statistics(tablet_index):
            tablet_statistics1 = get(f"//tmp/t1/@tablets/{tablet_index}/statistics", driver=self.remote_driver)
            tablet_statistics2 = get(f"//tmp/t2/@tablets/{tablet_index}/statistics")
            return tablet_statistics1 == tablet_statistics2

        for tablet_index in range(2):
            wait(lambda: _check_statistics(tablet_index))

        assert get("//tmp/t1/@hunk_statistics", driver=self.remote_driver) == get("//tmp/t2/@hunk_statistics")

        assert get("//tmp/t1/@resource_usage", driver=self.remote_driver) == get("//tmp/t2/@resource_usage")

        self._check_all_read_methods("//tmp/t1", "//tmp/t2", keys, rows, source_driver=self.remote_driver, dest_driver=None)

        set("//tmp/t2/@enable_compaction_and_partitioning", True)

        sync_unmount_table("//tmp/t2")
        schema[1]["max_inline_hunk_size"] = 1000
        alter_table("//tmp/t2", schema=schema)
        set("//tmp/t2/@forced_compaction_revision", 1)
        sync_mount_table("//tmp/t2")

        wait(lambda: len(self._get_chunk_ids("//tmp/t2", "hunk")) == 0)

        self._check_all_read_methods("//tmp/t1", "//tmp/t2", keys, rows, source_driver=self.remote_driver, dest_driver=None)

    @authors("coteeq")
    def test_sorted_table_constraints(self):
        sync_create_cells(1)
        sync_create_cells(1, driver=self.remote_driver)

        schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]
        self._create_sorted_table("//tmp/t1", schema=schema, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2", schema=schema)

        row_count = 5
        rows = [{"key": "a" * 19 + str(i), "value": "a" * 20} for i in range(row_count)]

        set("//tmp/t1/@enable_compaction_and_partitioning", False, driver=self.remote_driver)
        sync_reshard_table("//tmp/t1", [[], [rows[3]["key"]]], driver=self.remote_driver)

        set("//tmp/t2/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t2", [[], [rows[3]["key"]]])

        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        for row in rows:
            insert_rows("//tmp/t1", [row], driver=self.remote_driver)
            sync_flush_table("//tmp/t1", driver=self.remote_driver)

        assert len(self._get_chunk_ids("//tmp/t1", "hunk", driver=self.remote_driver)) == 5

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        # This is exactly data weight of two rows, but still smaller than 5x data weight of hunk chunk.
        # Unfortunately, regular chunks will report data_weight _with_ size of hunk values,
        # so their slicing will look at virtual data size of the whole row.
        data_weight_per_job = 82
        hunk_job_count = ceil(sum(len(row["value"]) for row in rows) / data_weight_per_job)
        regular_job_count = ceil(sum(len(row["value"]) + len(row["key"]) + 1 for row in rows) / data_weight_per_job)

        op = remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "data_weight_per_job": data_weight_per_job,
            }
        )

        def get_job_count(task_name):
            tasks = get(op.get_path() + "/@progress/tasks")
            task = [task for task in tasks if task["task_name"] == task_name][0]
            return task["job_counter"]["completed"]["total"]

        assert hunk_job_count == get_job_count("hunk_remote_copy")
        assert regular_job_count == get_job_count("remote_copy")

    @authors("alexelexa", "akozhikhov")
    def test_no_hunks_in_static_table(self):
        self._create_sorted_table("//tmp/t1", max_inline_hunk_size=1, dynamic=False, driver=self.remote_driver)
        self._create_sorted_table("//tmp/t2", max_inline_hunk_size=1, dynamic=False)

        with raises_yt_error("Remote copy for static tables with hunks is not supported"):
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
            )

    @authors("alexelexa")
    @pytest.mark.parametrize("max_inline_hunk_size", [1, 5, 10])
    def test_remote_copy_hunks_after_reshard(self, max_inline_hunk_size):
        for path, driver in [("//tmp/t1", self.remote_driver), ("//tmp/t2", None)]:
            sync_create_cells(1, driver=driver)
            self._create_sorted_table(
                path,
                max_inline_hunk_size=1,
                pivot_keys=[[], [3]],
                enable_compaction_and_partitioning=False,
                max_hunk_compaction_size=5,
                driver=driver)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        def hunk_value(i):
            return str(i) * 20

        rows = [{"key": i, "value": hunk_value(i)} for i in range(5)]
        keys = [{"key" : dictionary["key"]} for dictionary in rows]

        for row in rows:
            insert_rows("//tmp/t1", [row], driver=self.remote_driver)
            sync_flush_table("//tmp/t1", driver=self.remote_driver)

        assert len(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver)) == 5

        hunk_chunks_count = sum(map(lambda d: len(d["value"]) >= max_inline_hunk_size, rows))
        assert len(self._get_chunk_ids("//tmp/t1", "hunk", driver=self.remote_driver)) == hunk_chunks_count

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        sync_reshard_table("//tmp/t1", [[], [1], [2], [7]], driver=self.remote_driver)
        sync_reshard_table("//tmp/t2", [[], [1], [2], [7]])

        chunk_ids_before_compaction = builtins.set(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver))
        set("//tmp/t1/@forced_store_compaction_revision", 1, driver=self.remote_driver)
        set("//tmp/t1/@enable_compaction_and_partitioning", True, driver=self.remote_driver)
        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        def _check_forced_compaction():
            chunk_ids = builtins.set(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver))
            return chunk_ids_before_compaction.isdisjoint(chunk_ids)
        wait(_check_forced_compaction)

        assert len(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver)) == 3
        hunk_chunk_ids = self._get_chunk_ids("//tmp/t1", "hunk", driver=self.remote_driver)
        assert len(hunk_chunk_ids) == 5
        assert builtins.set(hunk_chunk_ids)

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        assert len(self._get_chunk_ids("//tmp/t2", "table")) == 3
        hunk_chunk_ids = self._get_chunk_ids("//tmp/t2", "hunk")
        assert len(hunk_chunk_ids) == 5
        assert builtins.set(hunk_chunk_ids)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t2")

        self._check_all_read_methods("//tmp/t1", "//tmp/t2", keys, rows, source_driver=self.remote_driver, dest_driver=None)

        rows2 = [{"key": i, "value": hunk_value(i)} for i in range(5, 8)]
        for row in rows2:
            insert_rows("//tmp/t2", [row])
            sync_flush_table("//tmp/t2")

        assert_items_equal(select_rows("* from [//tmp/t2]"), rows + rows2)

        delete_rows("//tmp/t2", [{"key": i} for i in range(1, 8, 2)])
        assert_items_equal(select_rows("* from [//tmp/t2]"), [{"key": i, "value": hunk_value(i)} for i in range(0, 8, 2)])

    @authors("alexelexa")
    @pytest.mark.parametrize("max_inline_hunk_size", [15, 1000000000])
    def test_remote_copy_hunks_with_compression_dictionaries(self, max_inline_hunk_size):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": max_inline_hunk_size},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 20 * max_inline_hunk_size},
        ]

        for path, driver in [("//tmp/t1", self.remote_driver), ("//tmp/t2", None)]:
            create_dynamic_table(
                path,
                schema=SCHEMA_WITH_MULTIPLE_COLUMNS,
                enable_dynamic_store_read=False,
                hunk_chunk_reader={
                    "max_decompression_blob_size": 100,
                },
                max_hunk_compaction_garbage_ratio=0.5,
                chunk_format="table_versioned_simple",
                mount_config={
                    "value_dictionary_compression": {
                        "enable": True,
                        "column_dictionary_size": 256,
                        "max_processed_chunk_count": 2,
                    }},
                driver=driver)

            sync_create_cells(1, driver=driver)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100, "value2": "y" * 100} for i in range(100)]
        insert_rows("//tmp/t1", rows, driver=self.remote_driver)
        sync_flush_table("//tmp/t1", driver=self.remote_driver)

        def _get_attached_hunk_count(path, driver=None):
            chunk_ids = self._get_chunk_ids(path, "table", driver=driver)
            hunk_chunk_ids = builtins.set()
            compression_dictionary_ids = builtins.set()
            for chunk_id in chunk_ids:
                hunk_chunk_refs = get("#{}/@hunk_chunk_refs".format(chunk_id), driver=driver)
                for ref in hunk_chunk_refs:
                    if ref["hunk_count"] == 0:
                        compression_dictionary_ids.add(ref["chunk_id"])
                    else:
                        hunk_chunk_ids.add(ref["chunk_id"])
            return len(hunk_chunk_ids), len(compression_dictionary_ids)

        def _check_hunk_count(table_path, usual_hunk_count, unattached_cd_count, attached_cd_count, wait_until=True, driver=None):
            hunk_count = usual_hunk_count if max_inline_hunk_size < 1000 else 0
            total_hunk_count = attached_cd_count + unattached_cd_count + hunk_count
            if wait_until:
                wait(lambda: len(self._get_chunk_ids(table_path, "hunk", driver=driver)) == total_hunk_count)
            else:
                assert len(self._get_chunk_ids(table_path, "hunk", driver=driver)) == total_hunk_count
            assert _get_attached_hunk_count(table_path, driver=driver) == (hunk_count, attached_cd_count)

        _check_hunk_count("//tmp/t1", usual_hunk_count=1, attached_cd_count=0, unattached_cd_count=2, driver=self.remote_driver)

        chunk_ids_before_compaction = builtins.set(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver))
        set("//tmp/t1/@forced_store_compaction_revision", 1, driver=self.remote_driver)
        remount_table("//tmp/t1", driver=self.remote_driver)

        def _check_forced_compaction():
            chunk_ids = builtins.set(self._get_chunk_ids("//tmp/t1", "table", driver=self.remote_driver))
            return chunk_ids_before_compaction.isdisjoint(chunk_ids)
        wait(_check_forced_compaction)

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t1", keys, driver=self.remote_driver), rows)
        _check_hunk_count("//tmp/t1", usual_hunk_count=1, attached_cd_count=1, unattached_cd_count=1, wait_until=False, driver=self.remote_driver)

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
            }
        )

        # Do not copy unattached compression dictionaries
        _check_hunk_count("//tmp/t2", usual_hunk_count=1, attached_cd_count=1, unattached_cd_count=0, wait_until=False)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t2")

        self._check_all_read_methods("//tmp/t1", "//tmp/t2", keys, rows, source_driver=self.remote_driver, dest_driver=None)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100, "value2": "y" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t2", rows2)
        sync_flush_table("//tmp/t2")

        _check_hunk_count("//tmp/t2", usual_hunk_count=2, attached_cd_count=2, unattached_cd_count=1)
        assert_items_equal(select_rows("* from [//tmp/t2]"), rows + rows2)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTablesErasure(TestSchedulerRemoteCopyDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_NODES = 12

    @authors("alexelexa")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("available", [False, True])
    @pytest.mark.parametrize("with_repair", [False, True])
    def test_remote_copy_erasure_hunks(self, chunk_format, available, with_repair):
        self._nodes = ls("//sys/cluster_nodes")
        assert len(self._nodes) == self.NUM_NODES

        reason = "separate tablet and data nodes"
        disable_write_sessions_on_node(self._nodes[0], reason=reason)
        for node in self._nodes[1:]:
            disable_tablet_cells_on_node(node, reason=reason)

        SCHEMA = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        for path, driver in [("//tmp/t1", self.remote_driver), ("//tmp/t2", None)]:
            self._create_sorted_table(
                path,
                schema=SCHEMA,
                enable_dynamic_store_read=False,
                hunk_chunk_writer={
                    "desired_block_size": 50,
                },
                chunk_format=chunk_format,
                hunk_erasure_codec="isa_reed_solomon_6_3",
                replication_factor=4,
                driver=driver)

            if chunk_format == "table_versioned_indexed":
                set(f"{path}/@compression_codec", "none", driver=driver)
                set(f"{path}/@mount_config/enable_hash_chunk_index_for_lookup", True, driver=driver)

            sync_create_cells(1, driver=driver)
            set("//sys/@config/chunk_manager/enable_chunk_replicator", False, driver=driver)

        sync_mount_table("//tmp/t1", driver=self.remote_driver)
        keys = [{"key": i} for i in range(11)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(11)]
        insert_rows("//tmp/t1", rows, driver=self.remote_driver)
        assert_items_equal(select_rows("* from [//tmp/t1]", driver=self.remote_driver), rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys, driver=self.remote_driver), rows)
        sync_unmount_table("//tmp/t1", driver=self.remote_driver)
        sync_mount_table("//tmp/t1", driver=self.remote_driver)

        hunk_chunk_ids = self._get_chunk_ids("//tmp/t1", "hunk", driver=self.remote_driver)
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        lookup_source_result = lookup_rows("//tmp/t1", keys, versioned=True, driver=self.remote_driver)

        def _set_ban_for_chunk_parts(part_indices, banned_flag, chunk_id, driver=None):
            chunk_replicas = get("#{}/@stored_replicas".format(chunk_id), driver=driver)

            nodes_to_ban = []
            for part_index in part_indices:
                nodes = list(str(r) for r in chunk_replicas if r.attributes["index"] == part_index)
                nodes_to_ban += nodes

            set_nodes_banned(nodes_to_ban, banned_flag, driver=driver)

        sync_unmount_table("//tmp/t1", driver=self.remote_driver)

        def run_operation():
            op = remote_copy(
                track=False,
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "testing": {"delay_inside_materialize": 10000},
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "max_failed_job_count": 1,
                    "delay_in_copy_chunk": 1000,
                    "erasure_chunk_repair_delay": 100,
                    "repair_erasure_chunks": with_repair,
                    "chunk_availability_policy": "repairable"
                },
            )
            wait(lambda: op.get_state() == "materializing")
            return op

        if not available:
            op = run_operation()
            _set_ban_for_chunk_parts([0, 1, 2, 8], True, hunk_chunk_id, driver=self.remote_driver)
            wait(lambda: get("//sys/data_missing_chunks/@count", driver=self.remote_driver) > 0)
            wait(lambda: get("//sys/parity_missing_chunks/@count", driver=self.remote_driver) > 0)

            wait(lambda: op.get_job_count("aborted") > 0)
            op.abort()
            return

        assert get("//sys/data_missing_chunks/@count", driver=self.remote_driver) == 0

        list_of_banned_nodes = []
        if with_repair:
            list_of_banned_nodes = [0, 1, 4]
            _set_ban_for_chunk_parts(list_of_banned_nodes, True, hunk_chunk_id, driver=self.remote_driver)
            wait(lambda: get("//sys/data_missing_chunks/@count", driver=self.remote_driver) > 0)

        run_operation().track()

        sync_mount_table("//tmp/t2")

        def check():
            self._check_all_read_methods(
                "//tmp/t1",
                "//tmp/t2",
                keys,
                rows,
                source_driver=self.remote_driver,
                dest_driver=None,
                lookup_source_result=lookup_source_result)

        check()

        set("//sys/@config/chunk_manager/enable_chunk_replicator", True)
        wait(lambda: get("//sys/data_missing_chunks/@count") == 0)
        wait(lambda: get("//sys/parity_missing_chunks/@count") == 0)

        check()


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTablesMulticell(TestSchedulerRemoteCopyDynamicTables):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

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


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteCopyDynamicTablesWithHunksMulticell(TestSchedulerRemoteCopyDynamicTablesWithHunks):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################


class TestSchedulerRemoteCopyWithClusterThrottlers(TestSchedulerRemoteCopyCommandsBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_DISCOVERY_SERVERS = 1
    NUM_NODES = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            # Enable job throttler on exe node.
            "job_throttler": {
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 100,
            "operation_alerts_push_period": 100,
            "alert_manager": {
                "period": 100,
                "task_unavailable_network_bandwidth_time_ratio_alert_threshold": 0.01,
            },
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
        },
    }

    CHUNK_COUNT = 32
    BANDWIDTH_LIMIT = 10 ** 7
    THROTTLER_JITTER_MULTIPLIER = 0.5
    DATA_WEIGHT_SIZE_PER_CHUNK = 10 ** 7

    LEASE_TIMEOUT_SECONDS = 1

    def _create_default_cluster_throttlers_config(self):
        return {
            "enabled": True,
            "update_period": 600,
            "cluster_limits": {
                # Limit bandwidth from remote cluster to local cluster.
                self.REMOTE_CLUSTER_NAME: {
                    "bandwidth": {
                        "limit": self.BANDWIDTH_LIMIT,
                    },
                },
            },
            "distributed_throttler": {
                "member_client": {
                    "lease_timeout": self.LEASE_TIMEOUT_SECONDS * 1000,
                },
                "heartbeat_period": 200,
                "attribute_update_period": 600,
                "heartbeat_throttler_count_limit": 2,
                "limit_update_period": 600,
                "leader_update_period": 600,
            },
        }

    def _create_empty_cluster_throttlers_config(self):
        return {}

    def _create_malformed_cluster_throttlers_config(self):
        return '{ malformed_config '

    # Setup cluster throttlers config on local cluster.
    def _setup_cluster_throttlers_config(self, config=None):
        create('document', '//sys/cluster_throttlers', force=True)
        if config is None:
            config = self._create_default_cluster_throttlers_config()
        set('//sys/cluster_throttlers', config)

    # Restart exe nodes to initialize cluster throttlers after config setup.
    def _restart_nodes(self):
        with Restarter(self.Env, NODES_SERVICE):
            time.sleep(1)

        wait_for_nodes()

    # Create and initialize cluster throttlers config on all exe nodes.
    def _init_cluster_throttlers_config(self, config=None):
        # Create and set cluster throttlers config on local cluster.
        self._setup_cluster_throttlers_config(config=config)

        # Restart exe nodes to pick up new cluster throttlers config.
        self._restart_nodes()

        # Wait for exe nodes to apply new cluster throttlers config.
        self._wait_for_all_remote_cluster_throttlers_group_members()

    # Wait for all exe nodes to register in remote_cluster_throttlers_group.
    def _wait_for_all_remote_cluster_throttlers_group_members(self):
        def has_all_remote_cluster_throttlers_group_members():
            try:
                sys = ls("//sys")
                if len(sys) == 0:
                    return False
                if 'discovery_servers' not in sys:
                    return False
                servers = ls("//sys/discovery_servers")
                if len(servers) == 0:
                    return False
                discovery_server = servers[0]
                groups = ls("//sys/discovery_servers/{}/orchid/discovery_server".format(discovery_server))
                if 'remote_cluster_throttlers_group' not in groups:
                    return False
                group_members = ls("//sys/discovery_servers/{}/orchid/discovery_server/remote_cluster_throttlers_group/@members".format(discovery_server))
                return len(group_members) >= self.NUM_NODES
            except YtResponseError:
                return False

        # Wait for all exe nodes to register in discovery service.
        wait(lambda: has_all_remote_cluster_throttlers_group_members(), timeout=60)

    # Wait for all exe nodes to unregister from remote_cluster_throttlers_group.
    def _wait_for_no_remote_cluster_throttlers_group_members(self):
        def has_no_remote_cluster_throttlers_group_members():
            try:
                sys = ls("//sys")
                if len(sys) == 0:
                    return False
                if 'discovery_servers' not in sys:
                    return False
                servers = ls("//sys/discovery_servers")
                if len(servers) == 0:
                    return False
                discovery_server = servers[0]
                groups = ls("//sys/discovery_servers/{}/orchid/discovery_server".format(discovery_server))
                if 'remote_cluster_throttlers_group' not in groups:
                    return True
                group_members = ls("//sys/discovery_servers/{}/orchid/discovery_server/remote_cluster_throttlers_group/@members".format(discovery_server))
                return len(group_members) == 0
            except YtResponseError:
                return False

        # Wait for all exe nodes to unregister from discovery service.
        wait(lambda: has_no_remote_cluster_throttlers_group_members(), timeout=60)

    # Wait for bandwidth to become unavailable in CA.
    def _wait_for_bandwidth_to_become_unavailable(self, op):
        wait(lambda: exists(op.get_orchid_path() + "/controller/network_bandwidth_availability"))

        def is_not_available(cluster, op):
            value = get(op.get_orchid_path() + "/controller/network_bandwidth_availability")
            return str(value.get(cluster, None)) == "false"

        wait(lambda: is_not_available(self.REMOTE_CLUSTER_NAME, op))

    @authors("yuryalekseev")
    def test_cluster_throttlers(self):
        # Create and initialize default cluster throttlers config on all exe nodes.
        self._init_cluster_throttlers_config()

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_table",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Fill up table on remote cluster.
        for c in range(self.CHUNK_COUNT):
            write_table("<append=%true>//tmp/remote_table", {"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK}, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_table")

        remote_copy_start_time = time.time()

        # Copy table from remote cluster to local cluster.
        op = remote_copy(
            in_="//tmp/remote_table",
            out="//tmp/local_table",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
                "job_count": 1,
                "use_cluster_throttlers": True,
            },
        )

        remote_copy_end_time = time.time()

        # Check result table on local cluster.
        assert read_table("//tmp/local_table") == [{"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK} for c in range(self.CHUNK_COUNT)]
        assert not get("//tmp/local_table/@sorted")

        # Check that throttling has happened.
        assert (remote_copy_end_time - remote_copy_start_time) > (self.CHUNK_COUNT * self.DATA_WEIGHT_SIZE_PER_CHUNK * self.THROTTLER_JITTER_MULTIPLIER / self.BANDWIDTH_LIMIT)

        # Check that solomon counters have showed up.
        for job_id in op.list_jobs():
            job = get_job(op.id, job_id)

            profiler = profiler_factory().at_node(job["address"])
            wait(lambda: profiler.get("exec_node/throttler_manager/distributed_throttler/limit", {"throttler_id": "bandwidth_{}".format(self.REMOTE_CLUSTER_NAME)}) is not None)
            wait(lambda: profiler.get("exec_node/throttler_manager/distributed_throttler/usage", {"throttler_id": "bandwidth_{}".format(self.REMOTE_CLUSTER_NAME)}) is not None)

    @authors("yuryalekseev")
    @pytest.mark.parametrize("config_type", ["empty", "malformed"])
    def test_absent_cluster_throttlers(self, config_type):
        if config_type == "empty":
            config = self._create_empty_cluster_throttlers_config()
        if config_type == "malformed":
            config = self._create_malformed_cluster_throttlers_config()

        # Create default cluster throttlers config.
        self._setup_cluster_throttlers_config(config)

        # Restart exe nodes to pick up new cluster throttlers config.
        self._restart_nodes()

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_table",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Fill up table on remote cluster.
        for c in range(self.CHUNK_COUNT):
            write_table("<append=%true>//tmp/remote_table", {"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK}, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_table")

        remote_copy_start_time = time.time()

        # Copy table from remote cluster to local cluster.
        remote_copy(
            in_="//tmp/remote_table",
            out="//tmp/local_table",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
                "job_count": 1,
                "use_cluster_throttlers": True,
            },
        )

        remote_copy_end_time = time.time()

        # Check that throttling has been disabled.
        assert (remote_copy_end_time - remote_copy_start_time) < (self.CHUNK_COUNT * self.DATA_WEIGHT_SIZE_PER_CHUNK * self.THROTTLER_JITTER_MULTIPLIER / self.BANDWIDTH_LIMIT)

        # Check result table on local cluster.
        assert read_table("//tmp/local_table") == [{"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK} for c in range(self.CHUNK_COUNT)]
        assert not get("//tmp/local_table/@sorted")

    @authors("yuryalekseev")
    def test_cluster_throttlers_all_nodes_banned(self):
        # Create and initialize default cluster throttlers config on all exe nodes.
        self._init_cluster_throttlers_config()

        # Ban all nodes on local cluster.
        set_all_nodes_banned(True)

        # Wait for all nodes to disappear from group.
        self._wait_for_no_remote_cluster_throttlers_group_members()

    @authors("yuryalekseev")
    def test_rate_limit_ratio_hard_threshold(self):
        config = self._create_default_cluster_throttlers_config()
        # Set parameters to disable scheduling of operations.
        config["rate_limit_ratio_hard_threshold"] = -1
        config["cluster_limits"][self.REMOTE_CLUSTER_NAME] = {
            "bandwidth": {
                "limit": 0,
            },
        }
        self._init_cluster_throttlers_config(config)

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_table",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Fill up table on remote cluster.
        for c in range(self.CHUNK_COUNT):
            write_table("<append=%true>//tmp/remote_table", {"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK}, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_table")

        operation_time_limit_seconds = 30

        # Copy table from remote cluster to local cluster.
        op = remote_copy(
            track=False,
            in_="//tmp/remote_table",
            out="//tmp/local_table",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
                "job_count": self.CHUNK_COUNT,
                "use_cluster_throttlers": True,
                "time_limit": operation_time_limit_seconds * 1000,
            },
        )

        # Wait for bandwidth to become unavailable in CA.
        self._wait_for_bandwidth_to_become_unavailable(op)

        # Wait for operation abortion by time limit.
        with pytest.raises(YtError) as err:
            op.track(timeout=datetime.timedelta(seconds=2*operation_time_limit_seconds))

        assert 'has not finished in' in str(err.value) or 'running for too long' in str(err.value)
        assert 'unavailable_network_bandwidth_to_clusters' in op.get_alerts()
