from __future__ import with_statement

from .helpers import TEST_DIR, set_config_option, get_tests_sandbox, wait

from yt.wrapper.driver import get_command_list

from yt.local import start, stop

import yt.wrapper as yt

import os
import pytest
import uuid

@pytest.mark.usefixtures("yt_env")
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

    def test_mount_unmount(self, yt_env):
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

    def test_reshard(self, yt_env):
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

    def test_insert_lookup_delete(self, yt_env):
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

    def test_insert_lookup_delete_with_transaction(self, yt_env):
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

    def test_alter_table_replica(self):
        mode = yt.config["backend"]
        if mode != "native":
            mode = yt.config["api_version"]

        commands = get_command_list()
        if "alter_table_replica" not in commands:
            pytest.skip()

        test_name = "TestYtWrapper" + mode.capitalize()
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, cell_tag=1, use_new_proxy=True)
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
                stop(instance.id, path=dir)

    def test_reshard_automatic(self):
        self._sync_create_tablet_cell()
        table = TEST_DIR + "/table_reshard_auto"
        self._create_dynamic_table(table)

        yt.reshard_table(table, [[], ["a"]], sync=True)
        yt.set("{}/@tablet_balancer_config/enable_auto_reshard".format(table), False)
        yt.mount_table(table, sync=True)

        yt.reshard_table_automatic(table, sync=True)
        assert yt.get("{}/@tablet_count".format(table)) == 1

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

