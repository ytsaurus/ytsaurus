from __future__ import with_statement

from .conftest import authors
from .helpers import TEST_DIR, set_config_option, get_tests_sandbox, wait, check_rows_equality

from yt.wrapper.driver import get_api_version

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

from yt.local import start, stop

import yt.wrapper as yt
from yt.wrapper.dynamic_table_commands import BackupManifest, ClusterBackupManifest
from yt.wrapper.batch_helpers import create_batch_client

import os
import pytest
import uuid
import threading


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestDynamicTableCommands(object):
    def _sync_create_tablet_cell(self):
        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        return cell_id

    def _create_dynamic_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
            ]})
        attributes.update({"dynamic": True})
        yt.create("table", path, attributes=attributes)

    @authors("ifsmirnov")
    def test_mount_unmount(self):
        table = TEST_DIR + "/table"
        self._create_dynamic_table(table)

        self._sync_create_tablet_cell()

        def _check_tablet_state(expected):
            assert yt.get("{0}/@tablet_state".format(table)) == expected
            assert yt.get("{0}/@tablets/0/state".format(table)) == expected

        yt.mount_table(table, sync=True)
        _check_tablet_state("mounted")

        yt.unmount_table(table, sync=True)
        _check_tablet_state("unmounted")

        yt.unmount_table(table, sync=True)
        yt.mount_table(table, sync=True, freeze=True)
        _check_tablet_state("frozen")

        yt.unmount_table(table, sync=True)
        _check_tablet_state("unmounted")

    @authors("ifsmirnov")
    def test_reshard(self):
        table = TEST_DIR + "/table_reshard"
        self._create_dynamic_table(table)

        pivot_keys = [[], ["a"], ["b"]]
        yt.reshard_table(table, pivot_keys, sync=True)
        assert yt.get("{}/@tablet_state".format(table)) == "unmounted"
        assert yt.get("{}/@tablet_count".format(table)) == 3
        assert yt.get("{}/@pivot_keys".format(table)) == pivot_keys

        yt.reshard_table(table, tablet_count=1, sync=True)
        assert yt.get("{}/@tablet_state".format(table)) == "unmounted"
        assert yt.get("{}/@tablet_count".format(table)) == 1

    @authors("ifsmirnov")
    def test_reshard_uniform(self):
        table = TEST_DIR + "/table_reshard_uniform"
        self._create_dynamic_table(table, schema=[
            {"type": "uint64", "name": "key", "sort_order": "ascending"},
            {"type": "string", "name": "value"}
        ])

        yt.reshard_table(table, tablet_count=5, uniform=True, sync=True)
        assert yt.get("{}/@tablet_state".format(table)) == "unmounted"
        assert yt.get("{}/@tablet_count".format(table)) == 5

    @authors("ifsmirnov")
    def test_insert_lookup_delete(self):
        with set_config_option("tabular_data_format", None):
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table2"
            self._create_dynamic_table(table)

            self._sync_create_tablet_cell()

            yt.mount_table(table, sync=True)
            yt.insert_rows(table, [{"x": "a", "y": "b"}], raw=False)
            assert [{"x": "a", "y": "b"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))

            yt.insert_rows(table, [{"x": "c", "y": "d"}], raw=False)
            assert [{"x": "c", "y": "d"}] == list(yt.lookup_rows(table, [{"x": "c"}], raw=False))

            yt.delete_rows(table, [{"x": "a"}], raw=False)
            assert [{"x": "c", "y": "d"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))

    @authors("ifsmirnov")
    @pytest.mark.xfail(run=False, reason="In progress")
    def test_select(self):
        table = TEST_DIR + "/table"

        def select():
            schema = '<schema=[{"name"="x";"type"="int64"}; {"name"="y";"type"="int64"}; {"name"="z";"type"="int64"}]>'
            return list(yt.select_rows(
                "* from [{0}{1}]".format(schema, table),
                format=yt.YsonFormat(format="text"),
                raw=False))

        yt.remove(table, force=True)
        yt.create("table", table)
        yt.run_sort(table, sort_by=["x"])

        yt.set(table + "/@schema", [
            {"name": "x", "type": "int64", "sort_order": "ascending"},
            {"name": "y", "type": "int64"},
            {"name": "z", "type": "int64"}])

        assert [] == select()

        yt.write_table(yt.TablePath(table, append=True, sorted_by=["x"]),
                       ["{x=1;y=2;z=3}"], format=yt.YsonFormat())

        assert [{"x": 1, "y": 2, "z": 3}] == select()

    @authors("ifsmirnov")
    def test_insert_lookup_delete_with_transaction(self):
        if yt.config["backend"] != "native":
            pytest.skip()

        with set_config_option("tabular_data_format", None):
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table3"
            yt.remove(table, force=True)
            self._create_dynamic_table(table)

            self._sync_create_tablet_cell()

            yt.mount_table(table, sync=True)
            vanilla_client = yt.YtClient(config=yt.config.config)

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == []
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == []

            with yt.Transaction(type="tablet"):
                yt.insert_rows(table, [{"x": "a", "y": "a"}], raw=False)
                assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == []
                assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == []

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == [{"x": "a", "y": "a"}]
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == [{"x": "a", "y": "a"}]

            class FakeError(RuntimeError):
                pass

            with pytest.raises(FakeError):
                with yt.Transaction(type="tablet"):
                    yt.insert_rows(table, [{"x": "b", "y": "b"}], raw=False)
                    raise FakeError()

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == [{"x": "a", "y": "a"}]
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == [{"x": "a", "y": "a"}]

            with yt.Transaction(type="tablet"):
                yt.lock_rows(table, [{"x": "b"}, {"x": "c"}], raw=False)

    @authors("ifsmirnov")
    def test_read_from_dynamic_table(self):
        with set_config_option("tabular_data_format", None):
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table4"
            self._create_dynamic_table(table)

            self._sync_create_tablet_cell()

            yt.mount_table(table, sync=True)
            yt.insert_rows(table, [{"x": "a", "y": "b"}], raw=False)
            yt.unmount_table(table, sync=True)

            assert list(yt.read_table(table)) == [{"x": "a", "y": "b"}]
            with set_config_option("read_parallel/enable", True):
                assert list(yt.read_table(table)) == [{"x": "a", "y": "b"}]

    @authors("ifsmirnov")
    def test_incorrect_dynamic_table_commands(self):
        table = TEST_DIR + "/dyn_table"
        yt.create("table", table, attributes={
            "dynamic": True,
            "schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
            ]})

        self._sync_create_tablet_cell()

        yt.mount_table(table, sync=True)

        with pytest.raises(yt.YtResponseError):
            yt.select_rows("* from [//none]")

        with pytest.raises(yt.YtResponseError):
            yt.select_rows("abcdef")

        with pytest.raises(yt.YtResponseError):
            yt.insert_rows(table, [{"a": "b"}])

    @authors("ifsmirnov")
    def test_trim_rows(self):
        def remove_control_attributes(rows):
            for row in rows:
                for x in ["$tablet_index", "$row_index"]:
                    if x in row:
                        del row[x]

        with set_config_option("tabular_data_format", None):
            table = TEST_DIR + "/test_trimmed_table"
            self._create_dynamic_table(table, schema=[
                {"name": "x", "type": "string"},
                {"name": "y", "type": "string"}])

            self._sync_create_tablet_cell()

            yt.mount_table(table, sync=True)
            yt.insert_rows(table, [{"x": "a", "y": "b"}, {"x": "c", "y": "d"}, {"x": "e", "y": "f"}], raw=False)
            rows = list(yt.select_rows("* from [{0}]".format(table), raw=False))
            tablet_index = rows[0].get("$tablet_index")
            remove_control_attributes(rows)
            assert [{"x": "a", "y": "b"}, {"x": "c", "y": "d"}, {"x": "e", "y": "f"}] == rows

            yt.trim_rows(table, tablet_index, 1)

            rows = list(yt.select_rows("* from [{0}]".format(table), raw=False))
            remove_control_attributes(rows)
            assert [{"x": "c", "y": "d"}, {"x": "e", "y": "f"}] == rows

            yt.trim_rows(table, tablet_index, 3)
            assert [] == list(yt.select_rows("* from [{0}]".format(table), raw=False))

    @authors("ifsmirnov")
    def test_alter_table_replica(self):
        mode = yt.config["backend"]
        if mode != "native":
            mode = yt.config["api_version"]

        test_name = "TestYtWrapper" + mode.capitalize()
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, fqdn="localhost", cell_tag=2)
            second_cluster_client = instance.create_client()
            second_cluster_connection = second_cluster_client.get("//sys/@cluster_connection")
            yt.set("//sys/clusters", {"second_cluster": second_cluster_connection})

            table = TEST_DIR + "/test_replicated_table"
            schema = [{"name": "x", "type": "string", "sort_order": "ascending"},
                      {"name": "y", "type": "string"}]
            yt.create("replicated_table", table, attributes={"dynamic": True, "schema": schema})
            replica_id = yt.create("table_replica", attributes={"table_path": table,
                                                                "cluster_name": "second_cluster",
                                                                "replica_path": table + "_replica"})

            attributes = yt.get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
            assert attributes["state"] == "disabled"
            assert len(attributes["tablets"]) == 1
            assert attributes["tablets"][0]["state"] == "none"

            yt.alter_table_replica(replica_id, enabled=True)
            attributes = yt.get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
            assert attributes["state"] == "enabled"
            assert len(attributes["tablets"]) == 1
            assert attributes["tablets"][0]["state"] == "none"

            yt.alter_table_replica(replica_id, enabled=False)
            attributes = yt.get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
            assert attributes["state"] == "disabled"
            assert len(attributes["tablets"]) == 1
            assert attributes["tablets"][0]["state"] == "none"

            yt.alter_table_replica(replica_id, enabled=True, mode="sync")
            attributes = yt.get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
            assert attributes["state"] == "enabled"
            assert len(attributes["tablets"]) == 1
            assert attributes["tablets"][0]["state"] == "none"

        finally:
            if instance is not None:
                stop(instance.id, path=dir, remove_runtime_data=True)

    @authors("ifsmirnov")
    def test_reshard_automatic(self):
        self._sync_create_tablet_cell()
        table = TEST_DIR + "/table_reshard_auto"
        self._create_dynamic_table(table)

        yt.reshard_table(table, [[], ["a"]], sync=True)
        yt.set("{}/@tablet_balancer_config/enable_auto_reshard".format(table), False)
        yt.mount_table(table, sync=True)

        yt.reshard_table_automatic(table, sync=True)
        assert yt.get("{}/@tablet_count".format(table)) == 1

    @authors("ifsmirnov")
    @pytest.mark.parametrize("tables", [None, [TEST_DIR + "/table_balance_cells"]])
    def test_balance_tablet_cells(self, tables):
        cells = [self._sync_create_tablet_cell() for i in xrange(2)]
        table = TEST_DIR + "/table_balance_cells"
        yt.remove(table, force=True)
        self._create_dynamic_table(table, external=False)

        yt.reshard_table(table, [[], ["b"]], sync=True)
        yt.set("{}/@tablet_balancer_config/enable_auto_reshard".format(table), False)
        yt.set("{}/@tablet_balancer_config/enable_auto_tablet_move".format(table), False)
        yt.set("{}/@in_memory_mode".format(table), "uncompressed")
        yt.mount_table(table, cell_id=cells[0], sync=True)
        yt.insert_rows(table, [{"x": "a", "y": "a"}, {"x": "b", "y": "b"}])
        yt.freeze_table(table, sync=True)

        yt.balance_tablet_cells("default", tables, sync=True)
        tablets = yt.get("{}/@tablets".format(table))
        assert tablets[0]["cell_id"] != tablets[1]["cell_id"]

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("all_keys", [True, False])
    @pytest.mark.parametrize("batch", ["batch", "plain"])
    @pytest.mark.parametrize("input_format", [None, "yson", "json"])
    def test_get_in_sync_replicas(self, all_keys, batch, input_format):
        def _get_in_sync_replicas(table, ts):
            client = create_batch_client() if batch == "batch" else yt

            keys = [] if all_keys else [{"x": "a"}]

            sync_replicas = client.get_in_sync_replicas(
                table,
                ts,
                keys,
                all_keys=all_keys,
                format=input_format
            )

            if batch == "batch":
                client.commit_batch()
                assert sync_replicas.get_error() is None
                sync_replicas = sync_replicas.get_result()

            return sync_replicas

        self._sync_create_tablet_cell()
        table = TEST_DIR + "/test_get_in_sync_replicas"
        table_replica = table + "_replica"
        schema = [{"name": "x", "type": "string", "sort_order": "ascending"},
                  {"name": "y", "type": "string"}]

        yt.create("replicated_table", table, attributes={"dynamic": True, "schema": schema})
        yt.mount_table(table, sync=True)

        replica_id = yt.create("table_replica", attributes={
            "table_path": table,
            "cluster_name": "primary",
            "mode": "sync",
            "replica_path": table_replica}
        )
        yt.create("table", table_replica, attributes={
            "dynamic": True,
            "schema": schema,
            "upstream_replica_id": replica_id
        })
        yt.mount_table(table_replica, sync=True)

        ts = yt.generate_timestamp()
        assert _get_in_sync_replicas(table, ts) == []

        yt.alter_table_replica(replica_id, enabled=True)
        wait(lambda: yt.get("#{0}/@state".format(replica_id)) == "enabled")

        ts = yt.generate_timestamp()
        wait(lambda: _get_in_sync_replicas(table, ts) == [replica_id])

        yt.insert_rows(table, [{"x": "a", "y": "b"}])

        ts = yt.generate_timestamp()
        assert _get_in_sync_replicas(table, ts) == [replica_id]

    @authors("ignat")
    def test_get_tablet_infos(self):
        if get_api_version() != "v4":
            pytest.skip()

        self._sync_create_tablet_cell()
        table = TEST_DIR + "/table_reshard_auto"
        self._create_dynamic_table(table)
        yt.mount_table(table, sync=True)

        assert yt.get_tablet_infos(table, [0])["tablets"][0]["total_row_count"] == 0

    @authors("ignat")
    def test_tablet_index_control_attribute(self):
        self._sync_create_tablet_cell()
        table = TEST_DIR + "/dyntable"
        dump_table = TEST_DIR + "/dumptable"
        schema = [{"name": "key", "type": "string"}]

        self._create_dynamic_table(table, schema=schema)
        yt.create("table", dump_table)

        yt.reshard_table(table, tablet_count=2, sync=True)

        yt.mount_table(table, sync=True, first_tablet_index=0, last_tablet_index=0)
        yt.insert_rows(table, [{"key": "a"}])
        yt.unmount_table(table, sync=True)

        yt.mount_table(table, sync=True, first_tablet_index=1, last_tablet_index=1)
        yt.insert_rows(table, [{"key": "b"}])
        yt.unmount_table(table, sync=True)

        @yt.with_context
        def mapper(row, context):
            yield {"tablet_index": context.tablet_index}

        yt.run_map(
            mapper,
            table,
            dump_table,
            job_io={
                "control_attributes": {
                    "enable_tablet_index": True
                }
            }
        )

        check_rows_equality(yt.read_table(dump_table),
                            [{"tablet_index": 0}, {"tablet_index": 1}],
                            ordered=False)

    @authors("lukyan")
    def test_explain_query(self):
        self._sync_create_tablet_cell()

        table = TEST_DIR + "/dyntable"
        schema = [{"name": "key", "type": "string"}]
        self._create_dynamic_table(table, schema=schema)
        # Added by ignat to fix test failures.
        yt.mount_table(table, sync=True)

        list(yt.explain_query("* from [{}]".format(table)))

    @authors("ignat")
    def test_transaction_abort_on_commit_failure(self):
        table = TEST_DIR + "/dyntable_unmounted"
        self._create_dynamic_table(table)

        thread_count_before = threading.active_count()

        with pytest.raises(yt.YtError):
            with yt.Transaction(type="tablet"):
                try:
                    yt.insert_rows(table, [{"x": "a", "y": "b"}])
                except yt.YtError:
                    assert False, "insert_rows should not throw"

        thread_count_after = threading.active_count()

        assert thread_count_before == thread_count_after

    @authors("ifsmirnov")
    def test_retention_timestamp(self):
        self._sync_create_tablet_cell()
        table = TEST_DIR + "/retention_ts"
        self._create_dynamic_table(table)
        yt.mount_table(table, sync=True)
        rows1 = [{"x": "aaa", "y": "bbb"}]
        rows2 = [{"x": "ccc", "y": "ddd"}]
        keys = [{"x": "aaa"}, {"x": "ccc"}]
        yt.insert_rows(table, rows1)
        ts_between = yt.generate_timestamp()
        yt.insert_rows(table, rows2)
        ts_after = yt.generate_timestamp()

        ts_kwargs = {"timestamp": ts_after, "retention_timestamp": ts_between}
        assert rows2 == list(yt.lookup_rows(table, keys, **ts_kwargs))
        assert rows2 == list(yt.select_rows("* from [{}] order by x limit 10".format(table), **ts_kwargs))

        ts_kwargs.pop("retention_timestamp")
        assert rows1 + rows2 == list(yt.lookup_rows(table, keys, **ts_kwargs))
        assert rows1 + rows2 == list(yt.select_rows("* from [{}] order by x limit 10".format(table), **ts_kwargs))

    @authors("alexelexa")
    def test_get_tablet_errors(self):
        if get_api_version() != "v4":
            pytest.skip()

        self._sync_create_tablet_cell()
        table = TEST_DIR + "/dyntable_tablet_errors"
        self._create_dynamic_table(table)
        yt.mount_table(table, sync=True)

        errors = yt.get_tablet_errors(table)
        errors_with_limit = yt.get_tablet_errors(table, limit=4)
        assert len(errors["tablet_errors"]) == 0
        assert len(errors["replication_errors"]) == 0
        assert len(errors_with_limit["tablet_errors"]) == 0
        assert len(errors_with_limit["replication_errors"]) == 0
        assert "incomplete" not in errors
        assert "incomplete" not in errors_with_limit

    @authors("ifsmirnov")
    def test_backup_restore(self):
        if yt.config["backend"] == "rpc":
            pytest.skip()

        cluster_connection = yt.get("//sys/@cluster_connection")
        yt.set("//sys/clusters", {"self": cluster_connection})
        yt.set("//sys/@config/tablet_manager/enable_backups", True)

        self._sync_create_tablet_cell()
        table = TEST_DIR + "/table_to_backup"
        self._create_dynamic_table(table, enable_dynamic_store_read=True)
        yt.mount_table(table, sync=True)

        manifest = (BackupManifest() # noqa
            .add_cluster("self", ClusterBackupManifest() # noqa
                .add_table(table, table + ".bak"))) # noqa
        yt.create_table_backup(manifest)
        assert yt.exists(table + ".bak")

        manifest = (BackupManifest() # noqa
            .add_cluster("self", ClusterBackupManifest() # noqa
                .add_table(table + ".bak", table + ".res"))) # noqa
        yt.restore_table_backup(manifest, mount=True)
        assert yt.exists(table + ".res")
        wait(lambda: yt.get(table + ".res" + "/@tablet_state") == "mounted")
