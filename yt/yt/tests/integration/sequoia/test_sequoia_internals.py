from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, create, ls, get, remove, build_master_snapshots, raises_yt_error,
    exists, set
)

from yt_sequoia_helpers import resolve_sequoia_id, resolve_sequoia_path

from yt.common import YtError
import pytest

from time import sleep

##################################################################


class TestSequoiaEnvSetup(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_MASTERS = 1
    NUM_CLOCKS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_REMOTE_CLUSTERS = 2
    USE_SEQUOIA_REMOTE_0 = False
    GROUND_INDEX_OFFSET = 10

    @authors("h0pless")
    def test1(self):
        sleep(10)  # Just don't crash...
        assert True

    @authors("h0pless")
    def test2(self):
        sleep(10)  # Just don't crash... (again)
        assert True


##################################################################


class TestSequoiaInternals(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1

    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["sequoia_node_host"]},
    }

    @authors("kvk1920", "cherepashka")
    def test_create_and_remove(self):
        create("map_node", "//tmp/some_node")
        remove("//tmp/some_node")
        with pytest.raises(YtError):
            get("//tmp/some_node")
        assert ls("//tmp") == []

    @authors("cherepashka")
    def test_recursive_remove(self):
        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m1/m2")
        create("map_node", "//tmp/m1/m2/m3")
        create("int64_node", "//tmp/m1/m2/m3/i")

        remove("//tmp/m1")
        assert ls("//tmp") == []

    @authors("danilalexeev")
    def test_list(self):
        assert ls("//tmp") == []

        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m2")
        assert ls("//tmp") == ["m1", "m2"]

        create("map_node", "//tmp/m2/m3")
        create("int64_node", "//tmp/m1/i")
        assert ls("//tmp/m2") == ["m3"]
        assert ls("//tmp") == ["m1", "m2"]
        assert ls("//tmp/m1") == ["i"]

    @authors("danilalexeev")
    def test_create_recursive_fail(self):
        create("map_node", "//tmp/some_node")
        with pytest.raises(YtError):
            create("map_node", "//tmp/a/b")

    @authors("danilalexeev")
    def test_create_recursive_success(self):
        create("map_node", "//tmp/a/b", recursive=True)
        assert ls("//tmp/a") == ["b"]
        child_id = create("map_node", "//tmp/a/b/c/d/e/f/g/h/i", recursive=True)
        assert ls("//tmp/a/b/c/d/e/f/g/h") == ["i"]
        assert get("//tmp/a/b/c/d/e/f/g/h/@children") == {"i": child_id}
        root_id = get("//tmp/a/b/c/@id")
        assert get("//tmp/a/b/@children") == {"c": root_id}

    @authors("cherepashka", "danilalexeev")
    def test_create_and_set_scalars(self):
        create("int64_node", "//tmp/i")
        set("//tmp/i", 0xbebe)
        assert get("//tmp/i") == 0xbebe
        create("uint64_node", "//tmp/ui")
        set("//tmp/ui", 0xbebebe)
        assert get("//tmp/ui") == 0xbebebe
        set("//tmp/s", "str")
        assert get("//tmp/s") == "str"
        set("//tmp/d", 0.5)
        assert get("//tmp/d") == 0.5
        set("//tmp/b", False)
        assert not get("//tmp/b")
        with pytest.raises(YtError, match="List nodes cannot be created inside Sequoia"):
            set("//tmp/l", [])

    @authors("danilalexeev")
    def test_set_map(self):
        subtree = {"hello": "world", "m": {"n": 0, "s": "string", "m": {}}, "d": 0.5}
        set("//tmp/m", subtree)
        assert get("//tmp/m/hello") == "world"
        assert sorted(ls("//tmp/m/m")) == ["m", "n", "s"]
        set("//tmp/a/b/c", subtree, recursive=True)
        assert sorted(ls("//tmp/a/b/c")) == ["d", "hello", "m"]
        assert get("//tmp/a/b/c/m/s") == "string"

    @authors("danilalexeev")
    def test_set_map_force(self):
        create("map_node", "//tmp/m/m", recursive=True)
        with pytest.raises(YtError, match="forbidden"):
            set("//tmp/m", {"a": 0})
        set("//tmp/m", {"a": 0}, force=True)
        assert ls("//tmp/m") == ["a"]

    @authors("danilalexeev")
    def test_nodes_cell_tags(self):
        ack_cell_tags = {}
        key = 0

        def create_and_check():
            nonlocal key
            create("map_node", f"//tmp/{key}")
            cell_tag = get(f"//tmp/{key}/@native_cell_tag")
            ack_cell_tags[cell_tag] = "ack"
            key += 1
            return len(ack_cell_tags) > 1

        while not create_and_check():
            pass

    @authors("danilalexeev")
    def test_escaped_symbols(self):
        with pytest.raises(YtError):
            create("map_node", "//tmp/special@&*[{symbols")
        path = r"//tmp/special\\\/\@\&\*\[\{symbols"
        create("map_node", path + "/m", recursive=True)
        assert r"special\/@&*[{symbols" in ls("//tmp")
        assert "m" in ls(path)
        child_id = create("map_node", r"//tmp/m\@1")
        assert get(r"//tmp/m\@1/@id") == child_id

    @authors("kvk1920", "gritukan")
    def test_create_map_node(self):
        m_id = create("map_node", "//tmp/m")
        # TODO(kvk1920): Support attribute setting.
        # set(f"#{m_id}/@foo", "bar")

        def check_everything():
            assert resolve_sequoia_path("//tmp") == get("//tmp&/@scion_id")
            assert resolve_sequoia_id(get("//tmp&/@scion_id")) == "//tmp"
            assert resolve_sequoia_path("//tmp/m") == m_id
            assert get(f"#{m_id}/@path") == "//tmp/m"
            assert get(f"#{m_id}/@key") == "m"
            assert get("//tmp/m/@path") == "//tmp/m"
            assert get("//tmp/m/@key") == "m"

            # TODO(kvk1920): Use attribute filter when it will be implemented in Sequoia.
            assert get(f"#{m_id}/@type") == "map_node"
            assert get(f"#{m_id}/@sequoia")

            # TODO(kvk1920): Support attribute setting.
            # assert get(f"#{m_id}/@foo") == "bar"

        check_everything()

        build_master_snapshots()

        # TODO(kvk1920): Move it to TestMasterSnapshots.
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        check_everything()

    @authors("kvk1920")
    def test_sequoia_map_node_explicit_creation_is_forbidden(self):
        with raises_yt_error("is internal type and should not be used directly"):
            create("sequoia_map_node", "//tmp/m")


##################################################################


@authors("kvk1920")
class TestSequoiaTmpCleanup(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 0
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }

    PATHS_TO_CHECK = []

    def _do_test(self):
        for path in TestSequoiaTmpCleanup.PATHS_TO_CHECK:
            assert not exists(path)
        TestSequoiaTmpCleanup.PATHS_TO_CHECK = []

        def make_path(node_id):
            return "#" + node_id

        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/b")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/c")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(get("//tmp/@rootstock_id")) + "&")

        for path in TestSequoiaTmpCleanup.PATHS_TO_CHECK:
            assert exists(path)

    def test1(self):
        self._do_test()

    def test2(self):
        self._do_test()
