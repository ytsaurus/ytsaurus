import pytest
import json
import time
import os.path
from functools import partial

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestLogs(YTEnvSetup):
    NUM_MASTERS = 2
    NUM_NONVOTING_MASTERS = 1
    NUM_NODES = 0

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
                    if line_json.get("info") is None or line_json.get("info")[key] != value:
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
        log_list.append({"path": "//tmp/access_log/a", "method": "create", "type": "table"})

        set("//tmp/access_log/a/@abc", "abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "set"})

        get("//tmp/access_log/a/@abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "get"})

        copy("//tmp/access_log/a", "//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/a", "method": "copy", "destination_path": "//tmp/access_log/b"})

        remove("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "remove"})

        create("table", "//tmp/access_log/some_table")
        create("table", "//tmp/access_log/b")

        exists("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "exists"})

        copy("//tmp/access_log/some_table", "//tmp/access_log/other_table")
        log_list.append({"path": "//tmp/access_log/some_table", "method": "copy", "destination_path": "//tmp/access_log/other_table"})

        create("map_node", "//tmp/access_log/some_node")

        move("//tmp/access_log/other_table", "//tmp/access_log/some_node/b")
        log_list.append({"path": "//tmp/access_log/other_table", "method": "move", "destination_path": "//tmp/access_log/some_node/b"})

        link("//tmp/access_log/b", "//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/some_node/q", "method": "link", "destination_path": "//tmp/access_log/b"})

        get("//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/b", "method": "get"})

        self._validate_entries_are_in_log(log_list)

    def test_transaction_logs(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        log_list = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/a", tx=tx1)
        log_list.append({"path": "//tmp/access_log/a", "method": "create", "transaction_id": str(tx1)})

        set("//tmp/access_log/a/@test", "test", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a/@test", "method": "set", "transaction_id": str(tx2)})

        copy("//tmp/access_log/a", "//tmp/access_log/b", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a", "destination_path": "//tmp/access_log/b", "method": "copy", "transaction_id": str(tx2)})

        exists("//tmp/access_log/a", tx=tx2)
        log_list.append({"path": "//tmp/access_log/a", "method": "exists", "transaction_id": str(tx2)})

        self._validate_entries_are_in_log(log_list)

    def test_log_dynamic_config(self):
        enabled_logs = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/enabled")
        enabled_logs.append({"method": "create", "path": "//tmp/access_log/enabled", "type": "table"})

        set("//sys/@config/enable_access_log", False)

        ts = str(generate_timestamp())
        create("table", "//tmp/access_log/{}".format(ts))

        set("//sys/@config/enable_access_log", True)

        self._validate_entries_are_in_log(enabled_logs)
        assert not self._is_node_in_logs(ts)
