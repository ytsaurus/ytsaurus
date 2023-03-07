import pytest
import json
import os.path

from flaky import flaky

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

@authors("avmatrosov")
class TestAccessLog(YTEnvSetup):
    NUM_MASTERS = 2
    NUM_NONVOTING_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    TEST_DIR = "//tmp/access_log"

    def _log_lines(self):
        with open(self.LOG_PATH, "r") as fd:
            for line in fd:
                try:
                    line_json = json.loads(line)
                except ValueError:
                    continue
                if line_json.get("path", "").startswith(self.TEST_DIR):
                    yield line_json

    def _is_node_in_logs(self, node):
        if not os.path.exists(self.LOG_PATH):
            return False
        for line_json in self._log_lines():
            if line_json.get("path", "") == self.TEST_DIR + '/' + node:
                return True
        return False

    def _validate_entries_are_in_log(self, entries):
        self.LOG_PATH = os.path.join(self.path_to_run, "logs/master-0-1.access.json.log")
        ts = str(generate_timestamp())
        create("table", "//tmp/access_log/{}".format(ts))
        wait(lambda: self._is_node_in_logs(ts))
        written_logs = [line_json for line_json in self._log_lines()]

        def _check_entry_is_in_log(log, line_json):
            for key, value in log.iteritems():
                if key in ["attributes", "transaction_id"]:
                    if line_json.get("transaction_info") is None or line_json.get("transaction_info")[key] != value:
                        return False
                elif line_json.get(key) != value:
                    return False
            return True

        for log in entries:
            assert any(_check_entry_is_in_log(log, line_json) for line_json in written_logs)


    @classmethod
    def modify_master_config(cls, config, index):
        config["logging"]["flush_period"] = 100
        config["logging"]["rules"].append({
            "min_level": "debug",
            "writers": ["access"],
            "include_categories": ["Access"],
            "message_format": "structured",
        })

        config["logging"]["writers"]["access"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/master-0-{}.access.json.log".format(index)),
            "accepted_message_format": "structured",
        }

    def test_logs(self):
        log_list = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/a")
        log_list.append({"path": "//tmp/access_log/a", "method": "Create", "type": "table"})

        set("//tmp/access_log/a/@abc", "abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Set"})

        get("//tmp/access_log/a/@abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Get"})

        copy("//tmp/access_log/a", "//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/a", "method": "Copy", "destination_path": "//tmp/access_log/b"})

        remove("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Remove"})

        create("table", "//tmp/access_log/some_table")
        create("table", "//tmp/access_log/b")

        exists("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Exists"})

        copy("//tmp/access_log/some_table", "//tmp/access_log/other_table")
        log_list.append({"path": "//tmp/access_log/some_table", "method": "Copy", "destination_path": "//tmp/access_log/other_table"})

        create("map_node", "//tmp/access_log/some_node")

        move("//tmp/access_log/other_table", "//tmp/access_log/some_node/b")
        log_list.append({"path": "//tmp/access_log/other_table", "method": "Move", "destination_path": "//tmp/access_log/some_node/b"})

        link("//tmp/access_log/b", "//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/some_node/q", "method": "Link", "destination_path": "//tmp/access_log/b"})

        get("//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/b", "method": "Get"})

        self._validate_entries_are_in_log(log_list)

    def test_transaction_logs(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        log_list = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/a", tx=tx1)
        log_list.append({"path": "//tmp/access_log/a", "method": "Create", "transaction_id": str(tx1)})

        set("//tmp/access_log/a/@test", "test", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a/@test", "method": "Set", "transaction_id": str(tx2)})

        copy("//tmp/access_log/a", "//tmp/access_log/b", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a", "destination_path": "//tmp/access_log/b", "method": "Copy", "transaction_id": str(tx2)})

        exists("//tmp/access_log/a", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a", "method": "Exists", "transaction_id": str(tx2)})

        self._validate_entries_are_in_log(log_list)

    def test_log_dynamic_config(self):
        enabled_logs = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/enabled")
        enabled_logs.append({"method": "Create", "path": "//tmp/access_log/enabled", "type": "table"})

        set("//sys/@config/security_manager/enable_access_log", False)

        ts = str(generate_timestamp())
        create("table", "//tmp/access_log/{}".format(ts))

        set("//sys/@config/security_manager/enable_access_log", True)

        self._validate_entries_are_in_log(enabled_logs)
        assert not self._is_node_in_logs(ts)

    def test_original_path(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("map_node", "//tmp/access_log/original")

        link("//tmp/access_log/original", "//tmp/access_log/linked")

        create("table", "//tmp/access_log/linked/t")
        log_list.append({"path": "//tmp/access_log/original/t", "method": "Create", "original_path": "//tmp/access_log/linked/t"})

        copy("//tmp/access_log/linked/t", "//tmp/access_log/linked/t2")
        log_list.append({"path": "//tmp/access_log/original/t", "method": "Copy", "original_path": "//tmp/access_log/linked/t",
                         "destination_path": "//tmp/access_log/original/t2", "original_destination_path": "//tmp/access_log/linked/t2"})

        set("//tmp/access_log/linked/t/@test", "test")
        log_list.append({"path": "//tmp/access_log/original/t/@test", "method": "Set", "original_path": "//tmp/access_log/linked/t/@test"})

        self._validate_entries_are_in_log(log_list)

    def test_table_logs(self):
        sync_create_cells(1)
        log_list = []

        create("map_node", "//tmp/access_log")
        create_dynamic_table("//tmp/access_log/table", schema=[{"name": "value", "type": "string"}])

        sync_mount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareMount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitMount"})

        remount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareRemount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitRemount"})

        sync_freeze_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareFreeze"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitFreeze"})

        sync_unfreeze_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareUnfreeze"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitUnfreeze"})

        sync_unmount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareUnmount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitUnmount"})

        sync_reshard_table("//tmp/access_log/table", 1)
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareReshard"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitReshard"})

        self._validate_entries_are_in_log(log_list)

##################################################################

class TestAccessLogPortal(TestAccessLog):
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_TMP_PORTAL = True

    # TODO(shakurov): fix it in YT-12457.
    @flaky(max_runs=3)
    def test_logs_portal(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("document", "//tmp/access_log/doc")

        create("portal_entrance", "//tmp/access_log/p1", attributes={"exit_cell_tag": 2})
        move("//tmp/access_log/doc", "//tmp/access_log/p1/doc")
        log_list.append({"path": "//tmp/access_log/doc", "method": "BeginCopy"})
        log_list.append({"path": "//tmp/access_log/p1/doc", "method": "EndCopy"})

        self._validate_entries_are_in_log(log_list)


