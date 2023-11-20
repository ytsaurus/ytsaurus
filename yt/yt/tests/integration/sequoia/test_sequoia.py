from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, ls, get, remove, wait

from yt.common import YtError
import pytest

##################################################################


class TestSequoia(YTEnvSetup):
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

        # Scalars.
        create("int64_node", "//tmp/i")
        assert get("//tmp/i") == 0
        create("uint64_node", "//tmp/ui")
        assert get("//tmp/ui") == 0
        create("string_node", "//tmp/s")
        assert get("//tmp/s") == ""
        create("double_node", "//tmp/d")
        assert get("//tmp/d") == 0.0
        create("boolean_node", "//tmp/b")
        assert not get("//tmp/b")
        create("list_node", "//tmp/l")
        assert get("//tmp/l") == []

        for node in ["some_node", "i", "ui", "s", "d", "b", "l"]:
            remove(f"//tmp/{node}")
            with pytest.raises(YtError):
                get(f"//tmp/{node}")
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

        wait(create_and_check)

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
