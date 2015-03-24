import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep


##################################################################

class TestTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0

    def _wait(self, predicate):
        while not predicate():
            sleep(1)

    def _sync_create_cells(self, size, count):
        ids = []
        for _ in xrange(count):
            ids.append(create_tablet_cell(size))

        print "Waiting for tablet cells to become healthy..."
        self._wait(lambda: all(get("//sys/tablet_cells/" + id + "/@health") == "good" for id in ids))

    def _create_table(self, path):
        create("table", path,
            attributes = {
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "string"}],
                "key_columns": ["key"]
            })

    def _create_table_with_computed_column(self, path):
        create("table", path,
            attributes = {
                "schema": [
                    {"name": "key1", "type": "int64"},
                    {"name": "key2", "type": "int64", "expression": "key1 * 100 + 3"},
                    {"name": "value", "type": "string"}],
                "key_columns": ["key1", "key2"]
            })

    def _get_tablet_leader_address(self, tablet_id):
        cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
        peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
        leader_peer = list(x for x in peers if x["state"] == "leading")[0]
        return leader_peer["address"]
 
    def _find_tablet_orchid(self, address, tablet_id):
        cells = get("//sys/nodes/" + address + "/orchid/tablet_cells", ignore_opaque=True)
        for (cell_id, cell_data) in cells.iteritems():
            if cell_data["state"] == "leading":
                tablets = cell_data["tablets"]
                if tablet_id in tablets:
                    return tablets[tablet_id]
        return None

    def _sync_mount_table(self, path):
        mount_table(path)

        print "Waiting for tablets to become mounted..."
        self._wait(lambda: all(x["state"] == "mounted" for x in get(path + "/@tablets")))
                
    def _sync_unmount_table(self, path):
        unmount_table(path)

        print "Waiting for tablets to become unmounted..."
        self._wait(lambda: all(x["state"] == "unmounted" for x in get(path + "/@tablets")))
 
    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]
           

    def test_mount1(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")

        mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]
        cell_id = tablets[0]["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]


    def test_unmount1(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")

        mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert tablet["pivot_key"] == []

        print "Waiting for table to become mounted..."
        self._wait(lambda: get("//tmp/t/@tablets/0/state") == "mounted")

        unmount_table("//tmp/t")

        print "Waiting for table to become unmounted..."
        self._wait(lambda: get("//tmp/t/@tablets/0/state") == "unmounted")

    def test_mount_unmount(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys);
        self.assertItemsEqual(rows, actual);

        self._sync_unmount_table("//tmp/t")
        with pytest.raises(YtError): lookup_rows("//tmp/t", keys)

        self._sync_mount_table("//tmp/t")
        actual = lookup_rows("//tmp/t", keys);
        self.assertItemsEqual(rows, actual);

    def test_reshard_unmounted(self):
        self._sync_create_cells(1, 1)
        create("table", "//tmp/t",
            attributes = {
                "schema": [
                    {"name": "k", "type": "int64"},
                    {"name": "l", "type": "uint64"},
                    {"name": "value", "type": "int64"}],
                "key_columns": ["k", "l"]
            })

        reshard_table("//tmp/t", [[]])
        assert self._get_pivot_keys("//tmp/t") == [[]]

        reshard_table("//tmp/t", [[], [100]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[], []])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[101]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[300]], first_tablet_index=3, last_tablet_index=3)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        reshard_table("//tmp/t", [[100], [150], [200]], first_tablet_index=1, last_tablet_index=2)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[100], [100]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[], [100, 200]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

    def test_force_unmount_on_remove(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None
       
        remove("//tmp/t")
        sleep(1)
        assert self._find_tablet_orchid(address, tablet_id) is None
         
    def test_read_write_table(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")

        with pytest.raises(YtError): read_table("//tmp/t")
        with pytest.raises(YtError): write_table("//tmp/t", [{"key": 1, "value": 2}])

    @pytest.mark.skipif('os.environ.get("BUILD_ENABLE_LLVM", None) == "NO"')
    def test_computed_column(self):
        self._sync_create_cells(1, 1)

        create("table", "//tmp/t1",
            attributes = {
                "schema": [
                    {"name": "key1", "type": "int64", "expression": "key2"},
                    {"name": "key2", "type": "uint64"},
                    {"name": "value", "type": "string"}],
                "key_columns": ["key1", "key2"]
            })
        with pytest.raises(YtError): self._sync_mount_table("//tmp/t1")

        self._create_table_with_computed_column("//tmp/t")
        self._sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key1": 1, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        self.assertItemsEqual(expected, actual)

        insert_rows("//tmp/t", [{"key1": 2, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1" : 1}])
        self.assertItemsEqual(expected, actual)
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1": 2}])
        self.assertItemsEqual(expected, actual)

        delete_rows("//tmp/t", [{"key1": 1}])
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        self.assertItemsEqual(expected, actual)

        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key1": 3, "key2": 3, "value": "3"}])
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key1": 2, "key2": 203}])
        with pytest.raises(YtError): delete_rows("//tmp/t", [{"key1": 2, "key2": 203}])

        expected = []
        actual = lookup_rows("//tmp/t", [{"key1": 3}])
        self.assertItemsEqual(expected, actual)

        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        self.assertItemsEqual(expected, actual)

    def test_no_copy(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t1")
        self._sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2")

    def test_no_move_mounted(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t1")
        self._sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2")

    def test_move_unmounted(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t1")
        self._sync_mount_table("//tmp/t1")
        self._sync_unmount_table("//tmp/t1")

        move("//tmp/t1", "//tmp/t2")

    def test_any_value_type(self):
        self._sync_create_cells(1, 1)
        create("table", "//tmp/t1",
            attributes = {
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "any"}],
                "key_columns": ["key"]
            })
        self._sync_mount_table("//tmp/t1")

        rows = [
            {"key": 11, "value": 100},
            {"key": 12, "value": False},
            {"key": 13, "value": True},
            {"key": 14, "value": 2**63 + 1 },
            {"key": 15, "value": 'stroka'},
            {"key": 16, "value": [1, {"attr": 3}, 4]},
            {"key": 17, "value": {"numbers": [0,1,42]}}]

        insert_rows("//tmp/t1", rows)
        actual = select_rows("* from [//tmp/t1]")
        self.assertItemsEqual(rows, actual)
        actual = lookup_rows("//tmp/t1", [{"key": row["key"]} for row in rows])
        self.assertItemsEqual(rows, actual)

    def test_swap(self):
        self.test_move_unmounted()

        self._create_table("//tmp/t3")
        self._sync_mount_table("//tmp/t3")
        self._sync_unmount_table("//tmp/t3")
        
        reshard_table("//tmp/t3", [[], [100], [200], [300], [400]])
        self._sync_mount_table("//tmp/t3")
        self._sync_unmount_table("//tmp/t3")

        move("//tmp/t3", "//tmp/t1")

        assert self._get_pivot_keys("//tmp/t1") == [[], [100], [200], [300], [400]]

    def _prepare_allowed(self, permission):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@inherit_acl", False)
        set("//tmp/t/@acl", [{"permissions": [permission], "action": "allow", "subjects": ["u"]}])
        
    def _prepare_denied(self, permission):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@acl", [{"permissions": [permission], "action": "deny", "subjects": ["u"]}])
        
    def test_select_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = select_rows("* from [//tmp/t]", user="u")
        self.assertItemsEqual(expected, actual)

    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError): select_rows("* from [//tmp/t]", user="u")

    def test_lookup_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key" : 1}], user="u")
        self.assertItemsEqual(expected, actual)

    def test_lookup_denied(self):
        self._prepare_denied("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key" : 1}], user="u")

    def test_insert_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}], user="u")
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key" : 1}])
        self.assertItemsEqual(expected, actual)

    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value": "test"}], user="u")

    def test_delete_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        delete_rows("//tmp/t", [{"key": 1}], user="u")
        expected = []
        actual = lookup_rows("//tmp/t", [{"key" : 1}])
        self.assertItemsEqual(expected, actual)

    def test_delete_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError): delete_rows("//tmp/t", [{"key": 1}], user="u")
