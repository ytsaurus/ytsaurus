import collections

from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, ls, get, set, copy, move,
    remove, link, commit_transaction,
    exists, start_transaction, abort_transaction, alter_table, write_table, sort, remount_table, generate_timestamp,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table, create_dynamic_table)

from yt_helpers import get_current_time
from yt_type_helpers import make_schema

import json
import os

##################################################################


@authors("shakurov", "avmatrosov")
class TestAccessLog(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SCHEDULERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    CELL_TAG_TO_DIRECTORIES = {10: "//tmp/access_log"}
    TRANSACTION_METHODS = {"StartTransaction", "CommitTransaction", "AbortTransaction"}

    def _log_lines(self, path, directory):
        with open(path, "rb") as fd:
            for line in fd:
                try:
                    line_json = json.loads(line)
                except ValueError:
                    continue
                is_transaction = line_json.get("method", None) in self.TRANSACTION_METHODS
                if is_transaction or line_json.get("path", "").startswith(directory):
                    yield line_json

    def _is_node_in_logs(self, node, path, directory):
        if not os.path.exists(path):
            return False
        for line_json in self._log_lines(path, directory):
            if line_json.get("path", "") == directory + "/" + node:
                return True
        return False

    def _collect_log_lines(self, cell_tag_to_directory=None):
        if cell_tag_to_directory is None:
            cell_tag_to_directory = self.CELL_TAG_TO_DIRECTORIES
        written_logs = []
        for master_tag, directory in cell_tag_to_directory.items():
            for idx in range(1, self.NUM_MASTERS):
                path = os.path.join(self.path_to_run, "logs/master-{}-{}.access.json.log".format(master_tag, idx))
                barrier_record = "{}-{}".format(master_tag, generate_timestamp())
                create("table", "{}/{}".format(directory, barrier_record))
                wait(lambda: self._is_node_in_logs(barrier_record, path, directory), iter=120, sleep_backoff=1.0)
                written_logs.extend([line_json for line_json in self._log_lines(path, directory)])
        return written_logs

    def _validate_entries_against_log(self, present_entries, missing_entries=[], cell_tag_to_directory=None):
        written_logs = self._collect_log_lines(cell_tag_to_directory)

        def _check_entry_is_in_log(log, line_json):
            for key, value in log.items():
                if key in ["attributes", "transaction_id", "operation_type"]:
                    if line_json.get("transaction_info") is None or \
                            key not in line_json.get("transaction_info") or \
                            line_json.get("transaction_info")[key] != value:
                        return False
                elif line_json.get(key) != value:
                    return False
            return True

        for log in present_entries:
            assert any(_check_entry_is_in_log(log, line_json) for line_json in
                       written_logs), "Entry {} is not present in access log".format(log)

        for log in missing_entries:
            assert not any(_check_entry_is_in_log(log, line_json) for line_json in
                           written_logs), "Entry {} is present in access log".format(log)

    @classmethod
    def modify_master_config(cls, config, tag, index):
        config["logging"]["flush_period"] = 100
        config["logging"]["rules"].append(
            {
                "min_level": "debug",
                "writers": ["access"],
                "include_categories": ["Access"],
                "message_format": "structured",
            }
        )
        config["logging"]["writers"]["access"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/master-{}-{}.access.json.log".format(tag, index)),
            "accepted_message_format": "structured",
        }

    def test_create(self):
        log_list = []

        map_node_id = create("map_node", "//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "Create", "type": "map_node", "id": map_node_id})

        table_id = create("table", "//tmp/access_log/t1")
        log_list.append({"path": "//tmp/access_log/t1", "method": "Create", "type": "table", "id": table_id})

        create("table", "//tmp/access_log/t1", ignore_existing=True)
        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "method": "Create",
                "type": "table",
                "id": table_id,
                "existing": "true",
            }
        )

        table_id2 = create("table", "//tmp/access_log/t1", force=True)
        assert table_id != table_id2
        log_list.append({"path": "//tmp/access_log/t1", "method": "Create", "type": "table", "id": table_id2})

        self._validate_entries_against_log(log_list)

    def test_logs(self):
        log_list = []

        map_node_id = create("map_node", "//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "Create", "type": "map_node", "id": map_node_id})

        a_id = create("table", "//tmp/access_log/a")
        log_list.append({"path": "//tmp/access_log/a", "method": "Create", "type": "table", "id": a_id})

        set("//tmp/access_log/a/@abc", "abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Set", "type": "table", "id": a_id})

        get("//tmp/access_log/a/@abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Get", "type": "table", "id": a_id})

        b_id = copy("//tmp/access_log/a", "//tmp/access_log/b")
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "destination_id": b_id,
                "destination_path": "//tmp/access_log/b",
            }
        )

        ls("//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "List", "type": "map_node", "id": map_node_id})

        remove("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Remove", "type": "table", "id": b_id})

        some_table_id = create("table", "//tmp/access_log/some_table")
        b_id = create("table", "//tmp/access_log/b")

        exists("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Exists", "type": "table", "id": b_id})

        other_table_id = copy("//tmp/access_log/some_table", "//tmp/access_log/other_table")
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": some_table_id,
                "path": "//tmp/access_log/some_table",
                "destination_id": other_table_id,
                "destination_path": "//tmp/access_log/other_table",
            }
        )

        some_node_id = create("map_node", "//tmp/access_log/some_node")

        some_node_b_id = move("//tmp/access_log/other_table", "//tmp/access_log/some_node/b")
        log_list.append(
            {
                "method": "Move",
                "type": "table",
                "id": other_table_id,
                "path": "//tmp/access_log/other_table",
                "destination_id": some_node_b_id,
                "destination_path": "//tmp/access_log/some_node/b",
            }
        )

        q_id = link("//tmp/access_log/b", "//tmp/access_log/some_node/q")
        log_list.append(
            {
                "method": "Link",
                "type": "link",
                "id": q_id,
                "path": "//tmp/access_log/some_node/q",
                "destination_path": "//tmp/access_log/b",
            }
        )

        get("//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/b", "method": "Get", "type": "table", "id": b_id})

        remove("//tmp/access_log/some_node", recursive=True)
        log_list.append(
            {
                "path": "//tmp/access_log/some_node",
                "method": "Remove",
                "type": "map_node",
                "id": some_node_id
            }
        )

        self._validate_entries_against_log(log_list)

    def test_transaction_logs(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        log_list = []

        create("map_node", "//tmp/access_log")

        a_id = create("table", "//tmp/access_log/a", tx=tx1)
        log_list.append(
            {
                "method": "Create",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "transaction_id": str(tx1),
            }
        )

        set("//tmp/access_log/a/@test", "test", tx=tx2)
        log_list.append(
            {
                "method": "Set",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a/@test",
                "transaction_id": str(tx2),
            }
        )

        b_id = copy("//tmp/access_log/a", "//tmp/access_log/b", tx=tx2)
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "destination_id": b_id,
                "destination_path": "//tmp/access_log/b",
                "transaction_id": str(tx2),
            }
        )

        exists("//tmp/access_log/a", tx=tx2)
        log_list.append(
            {
                "method": "Exists",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "transaction_id": str(tx2),
            }
        )

        self._validate_entries_against_log(log_list)

    def test_log_dynamic_config(self):
        enabled_logs = []
        disabled_logs = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/enabled")
        enabled_logs.append({"method": "Create", "path": "//tmp/access_log/enabled", "type": "table"})

        set("//sys/@config/security_manager/enable_access_log", False)

        ts = str(generate_timestamp())
        create("table", "//tmp/access_log/{}".format(ts))
        disabled_logs.append({"method": "Create", "path": "//tmp/access_log/{}".format(ts), "type": "table"})

        set("//sys/@config/security_manager/enable_access_log", True)

        self._validate_entries_against_log(present_entries=enabled_logs, missing_entries=disabled_logs)

    def test_original_path(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("map_node", "//tmp/access_log/original")

        link("//tmp/access_log/original", "//tmp/access_log/linked")

        create("table", "//tmp/access_log/linked/t")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t",
                "method": "Create",
                "original_path": "//tmp/access_log/linked/t",
            }
        )

        copy("//tmp/access_log/linked/t", "//tmp/access_log/linked/t2")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t",
                "method": "Copy",
                "original_path": "//tmp/access_log/linked/t",
                "destination_path": "//tmp/access_log/original/t2",
                "original_destination_path": "//tmp/access_log/linked/t2",
            }
        )

        set("//tmp/access_log/linked/t/@test", "test")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t/@test",
                "method": "Set",
                "original_path": "//tmp/access_log/linked/t/@test",
            }
        )

        self._validate_entries_against_log(log_list)

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

        self._validate_entries_against_log(log_list)

    def test_alter(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("table", "//tmp/access_log/table")
        schema1 = make_schema(
            [{"name": "a", "type": "string", "required": False}],
            strict=False,
            unique_keys=False,
        )
        alter_table("//tmp/access_log/table", schema=schema1)

        log_list.append({"path": "//tmp/access_log/table", "method": "Alter"})
        self._validate_entries_against_log(log_list)

    def test_expiration_time(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create(
            "table",
            "//tmp/access_log/table",
            attributes={"expiration_time": str(get_current_time())}
        )

        wait(lambda: not exists("//tmp/access_log/table"))

        log_list.append({"path": "//tmp/access_log/table", "method": "TtlRemove"})
        self._validate_entries_against_log(log_list)

    def test_write_table(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        table_id = create("table", "//tmp/access_log/t1")

        write_table("//tmp/access_log/t1", {"foo": "bar"})

        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "type": "table",
                "id": table_id,
                "method": "BeginUpload",
                "mode": "overwrite"
            }
        )

        self._validate_entries_against_log(log_list)
        write_table("<append=%true>//tmp/access_log/t1", {"foo2": "bar2"})

        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "type": "table",
                "id": table_id,
                "method": "BeginUpload",
                "mode": "append"
            }
        )

        self._validate_entries_against_log(log_list)

    def test_sort(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("table", "//tmp/access_log/t1")
        table_id2 = create("table", "//tmp/access_log/t2")

        write_table("<append=%true>//tmp/access_log/t1", {"a": 25, "b": "foo"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 100, "b": "bar"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 41, "b": "foobar"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 23, "b": "barfoo"})

        sort(in_="//tmp/access_log/t1", out="//tmp/access_log/t2", sort_by=["a"])

        log_list.append(
            {
                "path": "//tmp/access_log/t2",
                "type": "table",
                "id": table_id2,
                "method": "BeginUpload",
                "operation_type": "sort"
            }
        )

        self._validate_entries_against_log(log_list)

    def test_revise(self):
        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/t")
        id = get("//tmp/access_log/t/@id")

        write_table("<append=%true>//tmp/access_log/t", {"a": 25, "b": "foo"})
        log_list = [
            {
                "path": "//tmp/access_log/t",
                "type": "table",
                "id": id,
                "method": "Revise",
                "revision_type": "content"
            }
        ]
        self._validate_entries_against_log(log_list)

        set("//tmp/access_log/t/@qqq", 12)
        log_list = [
            {
                "path": "//tmp/access_log/t",
                "type": "table",
                "id": id,
                "method": "Revise",
                "revision_type": "attributes"
            }
        ]
        self._validate_entries_against_log(log_list)

        tx = start_transaction()
        set("//tmp/access_log/t/@qqq", 56, tx=tx)
        abort_transaction(tx)
        log_list = [
            {
                "path": "//tmp/access_log/t",
                "type": "table",
                "id": id,
                "method": "Revise",
                "transaction_id": tx,
                "revision_type": "attributes"
            }
        ]
        self._validate_entries_against_log(log_list)

    @authors("rebenkoy")
    def test_mutation_id(self):
        document_path = "//tmp/access_log/document"
        create("map_node", "//tmp/access_log")
        create("document", document_path)
        set(document_path, 0)
        set(document_path, 1)
        set(document_path, 2)
        set(document_path, 3)
        get(document_path)
        mutation_id_to_line = collections.defaultdict(list)
        for line in self._collect_log_lines():
            if not line.get("path", None).startswith("//tmp/access_log/document"):
                continue
            mutation_id_to_line[line.get("mutation_id")].append(line)
        for mutation_id in mutation_id_to_line:
            if mutation_id is None:
                assert len(mutation_id_to_line[mutation_id]) == 1
                assert mutation_id_to_line[mutation_id][0]["method"] == "Get"
            else:
                assert len(mutation_id_to_line[mutation_id]) % (self.NUM_MASTERS - 1) == 0, str(mutation_id_to_line[mutation_id])

    @authors("kvk1920")
    def test_transaction_actions_log(self):
        create("map_node", "//tmp/access_log")
        parameters = {"replicate_to_master_cell_tags": [10, 11, 12]}
        tx = start_transaction(parameters=parameters)
        tx_a = start_transaction(tx=tx, parameters=parameters)
        tx_b = start_transaction(tx=tx, parameters=parameters)
        tx_c = start_transaction(tx=tx_b, parameters=parameters)
        abort_transaction(tx=tx_a, parameters=parameters)
        tx_d = start_transaction(tx=tx, parameters=parameters)
        commit_transaction(tx=tx_d, parameters=parameters)
        commit_transaction(tx=tx_c, parameters=parameters)
        commit_transaction(tx=tx, parameters=parameters)

        def make_entry(entry):
            action, transaction, chain = entry
            last_transaction = {"transaction_id": transaction}
            info = {"method": f"{action}Transaction", "transaction_info": last_transaction}
            for ancestor in chain:
                last_transaction["parent"] = {"transaction_id": ancestor}
                last_transaction = last_transaction["parent"]
            return info

        expected = list(map(make_entry, [
            ("Start", tx, []),
            ("Start", tx_a, [tx]),
            ("Start", tx_b, [tx]),
            ("Start", tx_c, [tx_b, tx]),
            ("Abort", tx_a, [tx]),
            ("Start", tx_d, [tx]),
            ("Commit", tx_d, [tx]),
            ("Commit", tx_c, [tx_b, tx]),
            ("Abort", tx_b, [tx]),
            ("Commit", tx, []),
        ]))
        self._validate_entries_against_log(expected)

##################################################################


class TestAccessLogPortal(TestAccessLog):
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_TMP_PORTAL = True

    CELL_TAG_TO_DIRECTORIES = {11: "//tmp/access_log"}

    @authors("kvk1920")
    def test_transaction_actions_log(self):
        pass

    @authors("shakurov")
    def test_logs_portal(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        doc_id = create("document", "//tmp/access_log/doc")

        create("portal_entrance", "//portals/p1", attributes={"exit_cell_tag": 12})
        moved_doc_id = move("//tmp/access_log/doc", "//portals/p1/doc")
        log_list.append(
            {
                "method": "BeginCopy",
                "type": "document",
                "id": doc_id,
                "path": "//tmp/access_log/doc",
            })
        log_list.append(
            {
                "method": "EndCopy",
                "type": "document",
                "id": moved_doc_id,
                "path": "//portals/p1/doc",
            }
        )

        self._validate_entries_against_log(log_list, cell_tag_to_directory={
            11: "//tmp/access_log",
            12: "//portals/p1"})
