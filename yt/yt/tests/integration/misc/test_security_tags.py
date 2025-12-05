from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, concatenate, start_transaction, commit_transaction,
    lock, write_file, copy, read_table, write_table, map)

from yt_helpers import wait_until_unlocked
from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import time

##################################################################


@pytest.mark.enabled_multidaemon
class TestSecurityTags(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_security_tags_empty_by_default(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@security_tags") == []

    @authors("babenko")
    def test_set_security_tags_upon_create(self):
        create("table", "//tmp/t", attributes={"security_tags": ["tag1", "tag2"]})
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_write_table_with_security_tags(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

        write_table("//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), [])

        write_table("<security_tags=[tag3]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag3"])

        write_table("<security_tags=[]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), [])

    @authors("babenko")
    def test_write_table_with_security_tags_append(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])
        write_table("<append=true>//tmp/t", [{"c": "d"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_write_file_with_security_tags(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", b"test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

        write_file("//tmp/f", b"test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

        write_file("<security_tags=[tag3]>//tmp/f", b"test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag3"])

        write_file("<security_tags=[]>//tmp/f", b"test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

    @authors("babenko")
    def test_write_file_with_security_tags_append(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", b"test")
        write_file("<append=true>//tmp/f", b"test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_overwrite_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3]>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag2", "tag3"])

    @authors("babenko")
    def test_append_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3];append=%true>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2", "tag3"])

    @authors("babenko")
    def test_operation(self):
        create("table", "//tmp/t_in1", attributes={"security_tags": ["tag1"]})
        create("table", "//tmp/t_in2", attributes={"security_tags": ["tag2"]})

        create("table", "//tmp/t_out1", attributes={"security_tags": ["xxx"]})
        create("table", "//tmp/t_out2", attributes={"security_tags": ["xxx"]})
        create("table", "//tmp/t_out3", attributes={"security_tags": ["tag3"]})

        create("file", "//tmp/f", attributes={"security_tags": ["tag4"]})

        map(
            command="cat",
            in_=["//tmp/t_in1", "//tmp/t_in2"],
            out=[
                "//tmp/t_out1",
                "<security_tags=[tag5]>//tmp/t_out2",
                "<append=%true;security_tags=[tag6]>//tmp/t_out3",
            ],
            spec={
                "mapper": {"file_paths": ["//tmp/f"]},
                "additional_security_tags": ["tag0"],
            },
        )
        assert_items_equal(get("//tmp/t_out1/@security_tags"), ["tag0", "tag1", "tag2", "tag4"])
        assert_items_equal(get("//tmp/t_out2/@security_tags"), ["tag5"])
        assert_items_equal(get("//tmp/t_out3/@security_tags"), ["tag3", "tag6"])

    @authors("babenko")
    def test_update_security_tags1(self):
        create("table", "//tmp/t")
        assert_items_equal(get("//tmp/t/@security_tags"), [])

        set("//tmp/t/@security_tags", ["tag1", "tag2"])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_update_security_tags2(self):
        tx = start_transaction()

        create("table", "//tmp/t", tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), [])

        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])

        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    # I think this should actually be fine...
    def test_cannot_update_security_tags_in_append_mode(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        write_table("<append=%true>//tmp/t", [{"a": "b"}], tx=tx)
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)

    @authors("babenko")
    def test_can_update_security_tags_in_overwrite_mode1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        write_table("//tmp/t", [{"a": "b"}], tx=tx)
        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)

        commit_transaction(tx)
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])
        assert_items_equal(read_table("//tmp/t"), [{"a": "b"}])

    @authors("babenko")
    def test_can_update_security_tags_in_overwrite_mode2(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        write_table("//tmp/t", [{"a": "b"}], tx=tx)
        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)
        write_table("<append=%true>//tmp/t", [{"c": "d"}], tx=tx)

        commit_transaction(tx)
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])
        assert_items_equal(read_table("//tmp/t"), [{"a": "b"}, {"c": "d"}])

    @authors("babenko")
    def test_update_security_tags_involves_exclusive_lock(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        lock("//tmp/t", mode="shared", tx=tx)

        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["tag1", "tag2"])

    @authors("babenko")
    def test_concatenate(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag1", "tag2", "tag3"])

    @authors("babenko")
    def test_concatenate_append(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union", attributes={"security_tags": ["tag4"]})
        write_table("<append=%true>//tmp/union", [{"key": "z"}])

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "z"}, {"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag1", "tag2", "tag3", "tag4"])

    @authors("babenko")
    def test_concatenate_override(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "<security_tags=[tag0]>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag0"])

    @authors("babenko")
    def test_tag_naming_on_set(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", [""])
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["a" * 129])
        set("//tmp/t/@security_tags", ["a" * 128])

    @authors("babenko")
    def test_tag_naming_on_create(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"security_tags": [""]})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"security_tags": ["a" * 129]})
        create("table", "//tmp/t", attributes={"security_tags": ["a" * 128]})

    @authors("babenko")
    def test_tag_naming_on_write(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            write_table('<security_tags=[""]>//tmp/t', [])
        wait_until_unlocked("//tmp/t")
        with pytest.raises(YtError):
            write_table('<security_tags=["' + "a" * 129 + '"]>//tmp/t', [])
        wait_until_unlocked("//tmp/t")
        write_table('<security_tags=["' + "a" * 128 + '"]>//tmp/t', [])

    @authors("h0pless")
    def test_tag_update_mode(self):
        path = "//tmp/table"
        create("table", path, attributes={"security_tags": ["my-amazing-tag"]})
        assert get(f"{path}/@security_tags_update_mode") == "none"

        set(f"{path}/@security_tags", ["some-other-tag"])
        assert get(f"{path}/@security_tags_update_mode") == "none"

        payload = [{"key": "key", "more": "a"}]

        tx = start_transaction()
        # Appending to a node with mode "none" should set mode to append.
        write_table(f"<append=%true;security_tags=[more-tags]>{path}", payload, tx=tx)
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "append"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["some-other-tag", "more-tags"])

        child_tx = start_transaction(tx=tx)
        # Overwriting tags, however, should set the mode to overwrite, while adding extra tags.
        write_table(f"<security_tags=[the-best-tag]>{path}", payload, tx=child_tx)
        assert get(f"{path}/@security_tags_update_mode", tx=child_tx) == "overwrite"
        assert_items_equal(get(f"{path}/@security_tags", tx=child_tx), ["the-best-tag"])

        # But should do nothing to the original branch.
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "append"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["some-other-tag", "more-tags"])

        # Merging overwrite over append should lead to an overwrite.
        commit_transaction(child_tx)
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "overwrite"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["the-best-tag"])

        # Directly overwriting an overwrite brach should take priority.
        set(f"{path}/@security_tags", ["cool-tag"], tx=tx)
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "overwrite"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["cool-tag"])

        # Appending security tags to an overwriting branch should keep overwrite priority, but add extra tags.
        write_table(f"<append=%true;security_tags=[more-tags]>{path}", payload, tx=tx)
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "overwrite"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["cool-tag", "more-tags"])

        # Appending data should not change anything about the security tags.
        write_table(f"<append=%true>{path}", payload, tx=tx)
        assert get(f"{path}/@security_tags_update_mode", tx=tx) == "overwrite"
        assert_items_equal(get(f"{path}/@security_tags", tx=tx), ["cool-tag", "more-tags"])

        # Trunk should keep the changes, while setting mode to none.
        commit_transaction(tx)
        assert get(f"{path}/@security_tags_update_mode") == "none"
        assert_items_equal(get(f"{path}/@security_tags"), ["cool-tag", "more-tags"])


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecurityTagsMulticell(TestSecurityTags):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 1

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestSecurityTagsSequoia(TestSecurityTagsMulticell):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_SECONDARY_MASTER_CELLS = 3

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["sequoia_node_host"]},
    }


##################################################################


class TestSecurityTagsPortal(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
    }

    PAYLOAD = [{"key": "value"}]

    @authors("h0pless")
    def test_security_tags_core(self):
        create("portal_entrance", "//tmp/portal", attributes={"exit_cell_tag": 11})

        source_path = "//tmp/table"
        destination_path = "//tmp/portal/table_copy_with_security_tags"

        tx = start_transaction()
        create("table", source_path, tx=tx)
        write_table(f"<append=%true>{source_path}", self.PAYLOAD, tx=tx)
        copy(source_path, destination_path, tx=tx)

        set(f"{destination_path}/@security_tags", ["my_amazing_tag"], tx=tx)
        commit_transaction(tx)

        # Just don't crash...
        time.sleep(2)
