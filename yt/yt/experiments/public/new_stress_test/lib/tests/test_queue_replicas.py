from lib import test_queue_and_hunk_storage as stress
from lib.spec import Spec, spec_template

import pytest

from contextlib import nullcontext
import copy
import re
from types import SimpleNamespace
from unittest.mock import Mock


@pytest.fixture
def client(monkeypatch):
    client = Mock(spec=stress.yt)
    client.TablePath = stress.yt.TablePath
    client.config = {
        "dynamic_table_retries": {},
        "tablets_ready_timeout": 1000,
        "tablets_check_interval": 1,
    }
    client.Transaction.side_effect = lambda **kwargs: nullcontext()
    monkeypatch.setattr(stress, "yt", client)

    def wait(predicate, error_message, timeout, sleep_backoff):
        assert timeout > 0
        assert sleep_backoff > 0
        for _ in range(5):
            if predicate():
                return
        raise stress.YtError(error_message)

    monkeypatch.setattr(stress, "wait", wait)
    return client


def make_queue(modes=("sync", "sync", "async"), replicated_table_hunks=False):
    queue = stress.Queue(
        "//test", "queue", 2,
        replicas_plan=[
            {"mode": mode, "hunks": index > 0}
            for index, mode in enumerate(modes)
        ],
        cluster_name="local-test",
        replicated_table_hunks=replicated_table_hunks,
    )
    queue.replicas = [
        dict(plan, index=index, path=queue._replica_path(index))
        for index, plan in enumerate(queue.replicas_plan)
    ]
    queue.mount_state.mount(None)
    return queue


@pytest.mark.parametrize("replica_cell", [12, None])
@pytest.mark.parametrize("erasure", [False, True])
@pytest.mark.parametrize("source_hunks", [False, True])
def test_creation_and_relink(client, replica_cell, erasure, source_hunks):
    nodes = {}

    def create(kind, path=None, attributes=None):
        path = path or f"#replica-{len(nodes)}"
        assert "external" not in attributes
        # Without secondary cells, neither tables nor hunk storages are external.
        default_cell = 11 if replica_cell is not None else None
        cell = replica_cell if kind == "table" else default_cell
        nodes[path] = {
            "kind": kind,
            "attributes": copy.deepcopy(attributes),
            "cell": attributes.get("external_cell_tag", cell),
            "mounted": False,
        }
        return path

    def mount(path, sync):
        assert sync
        nodes[path]["mounted"] = True

    def unmount(path, sync):
        assert sync
        nodes[path]["mounted"] = False

    def set_attribute(path, value):
        table_path, attribute = path.split("/@")
        assert attribute == "hunk_storage_id"
        # These are the master's preconditions for changing a table's hunk storage.
        assert not nodes[table_path]["mounted"]
        assert nodes[table_path]["cell"] == nodes[value]["cell"]
        assert nodes[value]["mounted"]
        nodes[table_path]["attributes"][attribute] = value
        nodes[value]["owner"] = table_path

    def remove(path):
        assert nodes[path]["kind"] == "hunk_storage"
        assert nodes[path]["mounted"]
        assert not nodes[nodes[path]["owner"]]["mounted"]
        assert all(node["attributes"].get("hunk_storage_id") != path for node in nodes.values())
        del nodes[path]

    client.create.side_effect = create
    client.mount_table.side_effect = mount
    client.unmount_table.side_effect = unmount
    client.set.side_effect = set_attribute
    client.remove.side_effect = remove

    def get_attributes(path, attributes):
        assert path.endswith("/@")
        assert attributes == ["external_cell_tag"]
        cell = nodes[path[:-2]]["cell"]
        return {"external_cell_tag": cell} if cell is not None else {}

    client.get.side_effect = get_attributes

    attributes = {
        "compression_codec": "zstd_1",
        "tablet_cell_bundle": "test_bundle",
        "erasure_codec": "none",
        "mount_config": {"min_data_ttl": 1000},
        "preserve_tablet_index": False,
    }
    original_attributes = copy.deepcopy(attributes)
    queue = make_queue(replicated_table_hunks=source_hunks)
    queue.replicas = []
    queue.create(attributes, erasure=erasure)

    assert attributes == original_attributes
    for path in [queue.path] + [replica["path"] for replica in queue.replicas]:
        assert nodes[path]["cell"] == nodes[queue.path]["cell"]
        created = nodes[path]["attributes"]
        assert created["compression_codec"] == attributes["compression_codec"]
        assert created["tablet_cell_bundle"] == attributes["tablet_cell_bundle"]
        assert created["mount_config"]["min_data_ttl"] == 1000
        assert created["tablet_count"] == 2
        assert created["erasure_codec"] == ("isa_reed_solomon_6_3" if erasure else "none")
    assert nodes[queue.path]["attributes"]["mount_config"]["preserve_tablet_index"]
    assert "preserve_tablet_index" not in nodes[queue.path]["attributes"]

    def check_hunk_storage(storage_path):
        storage_attributes = nodes[storage_path]["attributes"]
        if erasure:
            assert storage_attributes["erasure_codec"] == "reed_solomon_3_3"
            assert storage_attributes["replication_factor"] == 1
            assert storage_attributes["read_quorum"] == 4
            assert storage_attributes["write_quorum"] == 5
        else:
            assert storage_attributes.get("erasure_codec", "none") == "none"
            assert "replication_factor" not in storage_attributes
            assert "read_quorum" not in storage_attributes
            assert "write_quorum" not in storage_attributes

    for table in [queue.replication_source, *queue.replicas]:
        table_attributes = nodes[table["path"]]["attributes"]
        value_schema = next(c for c in table_attributes["schema"] if c["name"] == "value")
        assert ("max_inline_hunk_size" in value_schema) == table["hunks"]
        if "replica_id" in table:
            assert not nodes[table["replica_id"]]["attributes"]["enable_replicated_table_tracker"]
        if not table["hunks"]:
            assert "hunk_storage_id" not in table_attributes
            assert table["hunk_storage_name"] is None
            continue

        check_hunk_storage(table_attributes["hunk_storage_id"])
        for _ in range(2):
            old_storage = table_attributes["hunk_storage_id"]
            queue.relink_table_hunk_storage(table)
            assert nodes[table["path"]]["mounted"]
            assert old_storage not in nodes
            new_storage = table_attributes["hunk_storage_id"]
            assert new_storage == f"{queue.base_path}/{table['hunk_storage_name']}"
            check_hunk_storage(new_storage)

    assert queue.hunk_storage_name is None


@pytest.mark.parametrize("in_memory_mode", ["none", "compressed", "uncompressed"])
@pytest.mark.parametrize("in_mount_config", [False, True])
def test_replicated_table_disables_in_memory_mode(client, in_memory_mode, in_mount_config):
    attributes = {
        "in_memory_mode": "none" if in_mount_config else in_memory_mode,
        "mount_config": {"min_data_ttl": 1000},
    }
    if in_mount_config:
        attributes["mount_config"]["in_memory_mode"] = in_memory_mode
    original_attributes = copy.deepcopy(attributes)
    client.get.return_value = {"external_cell_tag": 11}
    queue = make_queue(modes=("sync",))
    queue.replicas = []

    queue.create(attributes, erasure=False)

    created = {
        call.args[1]: call.kwargs["attributes"]
        for call in client.create.call_args_list
        if call.args[0] in ("replicated_table", "table")
    }
    replicated_attributes = created[queue.path]
    assert replicated_attributes["in_memory_mode"] == "none"
    assert replicated_attributes["mount_config"].get("in_memory_mode", "none") == "none"
    assert replicated_attributes["mount_config"]["min_data_ttl"] == 1000
    for replica in queue.replicas:
        replica_attributes = created[replica["path"]]
        assert replica_attributes["in_memory_mode"] == attributes["in_memory_mode"]
        assert replica_attributes["mount_config"] == attributes["mount_config"]
    assert attributes == original_attributes


@pytest.mark.parametrize("source_hunks", [False, True])
def test_removal_unlinks_replicas_and_deletes_mounted_storages(client, source_hunks):
    queue = make_queue(replicated_table_hunks=source_hunks)
    mounted = {queue.path, queue.data_path}
    owners = {}
    linked = {}
    for table in [queue.replication_source, *queue.replicas]:
        path = table["path"]
        mounted.add(path)
        table["hunk_storage_name"] = None
        if table["hunks"]:
            name = f"{path.rsplit('/', 1)[-1]}.hunk_storage"
            table["hunk_storage_name"] = name
            storage_path = f"{queue.base_path}/{name}"
            mounted.add(storage_path)
            owners[storage_path] = path
            linked[path] = name
    remaining = set(mounted)

    def unmount(path, sync):
        assert sync
        # Graceful storage unmounts may wait for delayed hunk unlocks during cleanup.
        assert path not in owners
        mounted.discard(path)

    def remove(path):
        if path.endswith("/@hunk_storage_id"):
            owner = path.split("/@")[0]
            assert owner not in mounted
            del linked[owner]
            return
        assert path not in linked
        if path == queue.path:
            assert not any(replica["path"] in remaining for replica in queue.replicas)
        if path in owners:
            assert owners[path] not in remaining
            assert path in mounted
        else:
            assert path not in mounted
        remaining.remove(path)
        mounted.discard(path)

    client.unmount_table.side_effect = unmount
    client.remove.side_effect = remove

    queue.remove()

    assert not remaining
    assert not mounted
    assert not linked
    client.mount_table.assert_not_called()


@pytest.mark.parametrize("source_probability", [0, 1])
@pytest.mark.parametrize("replica_probability", [0, 1])
def test_hunk_storage_probabilities_and_relinking(
    client, monkeypatch, source_probability, replica_probability,
):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({
        "create_replicated_probability": 1,
        "replicated_table_hunks_probability": source_probability,
        "replica_hunks_probability": replica_probability,
        "change_hunk_storage_probability": 1,
    })
    raw_spec["size"]["iterations"] = 1
    client.get.side_effect = lambda path, **kwargs: (
        "local-test" if path == "//sys/@cluster_name" else {"external_cell_tag": 11})
    queues = []
    relinked_paths = []

    def create_queue(queue, attributes, erasure):
        assert queue.replicated
        assert queue.replication_source["hunks"] == bool(source_probability)
        assert all(plan["hunks"] == bool(replica_probability) for plan in queue.replicas_plan)
        queue.replicas = [
            dict(plan, path=queue._replica_path(index))
            for index, plan in enumerate(queue.replicas_plan)
        ]
        queues.append(queue)

    monkeypatch.setattr(stress.Queue, "create", create_queue)
    monkeypatch.setattr(
        stress.Queue, "relink_table_hunk_storage",
        lambda self, table: relinked_paths.append(table["path"]),
    )
    monkeypatch.setattr(stress.HunkStorage, "create", lambda *args, **kwargs: None)
    monkeypatch.setattr(stress.HunkStorage, "mount", lambda *args, **kwargs: None)

    stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)

    assert len(queues) == 3
    assert relinked_paths == [
        table["path"]
        for queue in queues
        for table in [queue.replication_source, *queue.replicas]
        if table["hunks"]
    ]


@pytest.mark.parametrize("replication", ["mixed", "async", "plain"])
def test_write_replica_requirements_and_visibility(client, monkeypatch, replication):
    if replication == "plain":
        queue = stress.Queue("//test", "queue", 2)
        queue.mount_state.mount(None)
    else:
        queue = make_queue(("async", "async")) if replication == "async" else make_queue()
    polls = {}

    def get_tablet_infos(path, tablet_indexes):
        polls[path] = polls.get(path, 0) + 1
        ready = polls[path] >= 2
        return {"tablets": [
            {"total_row_count": queue.written_row_count[index] if ready else 0}
            for index in tablet_indexes
        ]}

    client.get_tablet_infos.side_effect = get_tablet_infos
    monkeypatch.setattr(stress, "run_with_retries", lambda action, **kwargs: action())
    cfg = SimpleNamespace(
        write_min_batch_size=2, write_max_batch_size=2,
        write_min_row_size=1024, write_max_row_size=1024,
        write_insert_chunk_bytes=4096,
    )

    queue.write(True, SimpleNamespace(queue_and_hunk_storage=cfg), retry_count=1)

    assert sum(queue.written_row_count) == 2
    expected_paths = [queue.path] if replication == "plain" else [
        replica["path"] for replica in queue.replicas if replica["mode"] == "sync"
    ]
    assert set(polls) == set(expected_paths)
    assert all(count >= 2 for count in polls.values())
    assert {call.args[0] for call in client.insert_rows.call_args_list} == {
        queue.path, queue.data_path,
    }
    for call in client.insert_rows.call_args_list:
        if call.args[0] == queue.path:
            assert call.kwargs["require_sync_replica"] == (replication != "async")
        else:
            assert "require_sync_replica" not in call.kwargs


@pytest.mark.parametrize("replication", ["mixed", "async", "single_async", "stuck_async", "plain"])
def test_operation_input_replica_visibility(client, monkeypatch, replication):
    if replication == "plain":
        queue = stress.Queue("//test", "queue", 2)
    elif replication == "mixed":
        queue = make_queue()
    else:
        modes = ("async",) if replication == "single_async" else ("async", "async")
        queue = make_queue(modes)
    queue.written_row_count = [2, 1]
    monkeypatch.setattr(stress.random, "choice", lambda replicas: replicas[-1])
    polls = []

    def get_tablet_infos(path, tablet_indexes):
        polls.append(path)
        ready = len(polls) >= 2 and replication != "stuck_async"
        return {"tablets": [
            {"total_row_count": queue.written_row_count[index] if ready else 0}
            for index in tablet_indexes
        ]}

    client.get_tablet_infos.side_effect = get_tablet_infos
    expected_path = queue.path
    if replication == "mixed":
        expected_path = queue.replicas[1]["path"]
    elif replication != "plain":
        expected_path = queue.replicas[-1]["path"]

    if replication == "stuck_async":
        with pytest.raises(stress.YtError, match=re.escape(expected_path)):
            queue._input_path()
    else:
        assert str(queue._input_path()) == expected_path
        if replication in ("mixed", "plain"):
            assert not polls
        else:
            assert polls == [expected_path, expected_path]


@pytest.mark.parametrize("problem", [None, "key", "value", "missing"])
@pytest.mark.parametrize("all_async", [False, True])
def test_read_checks_async_replicas(client, monkeypatch, problem, all_async):
    modes = ("async", "async") if all_async else ("sync", "sync", "async")
    expected = [
        [{"row_index": 0, "key": "a", "value": "first"},
         {"row_index": 1, "key": "b", "value": "second"}],
        [{"row_index": 0, "key": "c", "value": "third"}],
    ]
    rows = {}
    asynchronous = f"//test/queue_0.replica_{len(modes) - 1}"
    polls = {}
    reads = set()

    def create_queue(queue, attributes, erasure):
        queue.tablet_count = 2
        queue.mount_state = stress.MountState(2)
        queue.mount_state.mount(None)
        queue.written_row_count = [2, 1]
        queue.replicas = [
            {"path": queue._replica_path(index), "mode": mode, "hunks": False}
            for index, mode in enumerate(modes)
        ]
        for replica in queue.replicas:
            path = replica["path"]
            rows[path] = copy.deepcopy(expected)
            polls[path] = 0
            if path == asynchronous:
                if problem in ("key", "value"):
                    rows[path][1][0][problem] = "corrupted"
                elif problem == "missing":
                    rows[path] = [[], []]

    def get_tablet_infos(path, tablet_indexes):
        polls[path] += 1
        ready = path != asynchronous or polls[path] >= 2
        return {"tablets": [
            {"total_row_count": len(rows[path][index]) if ready else 0}
            for index in tablet_indexes
        ]}

    def pull_queue(path, offset, partition_index, max_data_weight):
        assert path != asynchronous or polls[path] >= 2
        reads.add(path)
        return rows[path][partition_index][offset:offset + 1]

    def select_rows(query):
        tablet_index = int(re.search(r"where tablet_index = (\d+)", query)[1])
        offset, limit = map(int, re.search(r"offset (\d+) limit (\d+)", query).groups())
        return expected[tablet_index][offset:offset + limit]

    client.get_tablet_infos.side_effect = get_tablet_infos
    client.pull_queue.side_effect = pull_queue
    client.select_rows.side_effect = select_rows
    client.get.side_effect = lambda path, **kwargs: (
        "local-test" if path == "//sys/@cluster_name" else {"external_cell_tag": 11})
    monkeypatch.setattr(stress.Queue, "create", create_queue)
    monkeypatch.setattr(stress.HunkStorage, "create", lambda *args, **kwargs: None)
    monkeypatch.setattr(stress.HunkStorage, "mount", lambda *args, **kwargs: None)

    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({
        "create_replicated_probability": 1,
        "read_probability": 1,
        "read_page_max_data_weight": 1024,
    })
    raw_spec["size"]["iterations"] = 1

    if problem:
        with pytest.raises(stress.YtError, match=re.escape(asynchronous)):
            stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)
        if problem == "missing":
            assert asynchronous not in reads
    else:
        stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)
        assert reads == set(rows)
