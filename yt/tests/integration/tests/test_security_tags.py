from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.environment.helpers import assert_items_equal

##################################################################

class TestSecurityTags(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def test_security_tags_empty_by_default(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@security_tags") == []

    def test_set_security_tags_upon_create(self):
        create("table", "//tmp/t", attributes={
                "security_tags": ["tag1", "tag2"]
            })
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

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

    def test_write_table_with_security_tags_append(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])
        write_table("<append=true>//tmp/t", [{"c": "d"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    def test_write_file_with_security_tags(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

        write_file("//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

        write_file("<security_tags=[tag3]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag3"])

        write_file("<security_tags=[]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

    def test_write_file_with_security_tags_append(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", "test")
        write_file("<append=true>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

    def test_overwrite_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3]>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag2", "tag3"])

    def test_append_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3];append=%true>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2", "tag3"])

    def test_operation(self):
        create("table", "//tmp/t_in1", attributes={
                "security_tags": ["tag1"]
            })
        create("table", "//tmp/t_in2", attributes={
                "security_tags": ["tag2"]
            })

        create("table", "//tmp/t_out1", attributes={
                "security_tags": ["xxx"]
            })
        create("table", "//tmp/t_out2", attributes={
                "security_tags": ["xxx"]
            })
        create("table", "//tmp/t_out3", attributes={
                "security_tags": ["tag3"]
            })

        create("file", "//tmp/f", attributes={
                "security_tags": ["tag4"]
            })

        map(command="cat",
            in_=["//tmp/t_in1", "//tmp/t_in2"],
            out=["//tmp/t_out1", "<security_tags=[tag5]>//tmp/t_out2", "<append=%true;security_tags=[tag6]>//tmp/t_out3"],
            spec={
                "mapper": {
                    "file_paths": ["//tmp/f"]
                },
                "additional_security_tags": ["tag0"]
            })
        assert_items_equal(get("//tmp/t_out1/@security_tags"), ["tag0", "tag1", "tag2"])
        assert_items_equal(get("//tmp/t_out2/@security_tags"), ["tag5"])
        assert_items_equal(get("//tmp/t_out3/@security_tags"), ["tag3", "tag6"])

##################################################################

class TestSecurityTagsMulticell(TestSecurityTags):
    NUM_SECONDARY_MASTER_CELLS = 1
