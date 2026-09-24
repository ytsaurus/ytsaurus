from lib import test_queue_and_hunk_storage as stress
from lib.spec import Spec, spec_template

import pytest

from contextlib import nullcontext
import copy
from unittest.mock import Mock


@pytest.fixture
def client(monkeypatch):
    client = Mock(spec=stress.yt)
    client.config = {
        "dynamic_table_retries": {},
        "tablets_ready_timeout": 100,
        "tablets_check_interval": 1,
    }
    monkeypatch.setattr(stress, "yt", client)
    return client


@pytest.mark.parametrize("failure", [None, "linked", "unexpected"])
def test_storage_removal_preserves_owner_mount_state(client, failure):
    queue = stress.Queue("//test", "queue", 3)
    queue.mount_state.is_mounted_tablet = [True, False, True]
    queue.mount_state.is_sync = [True, True, False]
    storage = stress.HunkStorage("//test", "storage", {queue.name: queue}, tablet_count=3)
    storage.mount_state.is_mounted_tablet = [True, False, False]
    storage.mount_state.is_sync = [True, True, False]
    if failure == "linked":
        storage.linked_queue_names.add(queue.name)
        queue.hunk_storage_name = storage.name
        error = stress.YtError("Cannot remove a hunk storage that is being used by nodes")
    elif failure == "unexpected":
        error = stress.YtError("Removal failed")
    else:
        error = None
    client.remove.side_effect = error
    queue_state = copy.deepcopy(vars(queue.mount_state))
    storage_state = copy.deepcopy(vars(storage.mount_state))

    with pytest.raises(stress.YtError) if error else nullcontext():
        storage.remove()

    # A rejected deletion must not leave storage or owner tablets unmounted.
    assert vars(queue.mount_state) == queue_state
    assert vars(storage.mount_state) == storage_state
    client.remove.assert_called_once_with(storage.path)
    client.unmount_table.assert_not_called()
    client.mount_table.assert_not_called()
    client.get.assert_not_called()


@pytest.mark.parametrize("replicated", [False, True])
@pytest.mark.parametrize("unlink", [False, True])
def test_queue_removal_skips_shared_hunk_storage_links(client, monkeypatch, replicated, unlink):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({
        "create_replicated_probability": int(replicated),
        "replicated_table_hunks_probability": 1,
        "remove_probability": 1,
        "change_hunk_storage_probability": int(unlink),
        "unlink_hunk_storage_probability": 1,
    })
    raw_spec["size"]["iterations"] = 2
    client.get.side_effect = lambda path, **kwargs: (
        "local-test" if path == "//sys/@cluster_name" else {"external_cell_tag": 11})
    queues = []
    removed = []

    def create_queue(queue, attributes, erasure):
        if queue.replicated:
            queue.replication_source["hunk_storage_name"] = f"{queue.name}.hunk_storage"
        queues.append(queue)

    def remove_queue(queue):
        assert queue.hunk_storage_name is None
        if queue.replicated:
            assert queue.replication_source["hunk_storage_name"] is not None
        removed.append((queue.name, stress.logger.iteration))

    def remove_storage(storage):
        if storage.linked_queue_names:
            raise stress.YtError("Cannot remove a hunk storage that is being used by nodes")

    monkeypatch.setattr(stress.Queue, "create", create_queue)
    monkeypatch.setattr(stress.Queue, "remove", remove_queue)
    monkeypatch.setattr(stress.HunkStorage, "create", lambda *args, **kwargs: None)
    monkeypatch.setattr(stress.HunkStorage, "remove", remove_storage)

    stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)

    if replicated or unlink:
        assert removed == [(queue.name, 0 if replicated else 1) for queue in queues]
    else:
        assert not removed
        assert all(queue.hunk_storage_name is not None for queue in queues)


@pytest.mark.parametrize("operation", ["link", "unlink", "relink"])
@pytest.mark.parametrize("fail_change", [False, True])
@pytest.mark.parametrize("initial_states", [
    ["unmounted", "unmounted", "unmounted"],
    ["mounted", "mounted", "mounted"],
    ["mounted", "unmounted", "mounted"],
    ["mounting", "unmounting", "mounted"],
    ["unmounting", "unmounting", "unmounting"],
])
def test_link_changes_preserve_mount_state(client, operation, fail_change, initial_states):
    queue = stress.Queue("//test", "queue", len(initial_states))
    queue.mount_state.is_mounted_tablet = [
        state in ("mounted", "mounting") for state in initial_states
    ]
    queue.mount_state.is_sync = [
        state in ("mounted", "unmounted") for state in initial_states
    ]
    states = list(initial_states)
    mounted_tablet_indexes = [
        index for index, state in enumerate(initial_states) if state in ("mounted", "mounting")
    ]
    mount_requests = []
    old = stress.HunkStorage("//test", "old", {queue.name: queue}, tablet_count=1)
    new = stress.HunkStorage("//test", "new", {queue.name: queue}, tablet_count=1)
    old.hunk_storage_id = "old-id"
    new.hunk_storage_id = "new-id"
    attributes = {}
    if operation != "link":
        queue.hunk_storage_name = old.name
        old.linked_queue_names.add(queue.name)
        attributes["hunk_storage_id"] = old.hunk_storage_id

    def get_tablets(path):
        assert path == f"{queue.path}/@tablets"
        if mount_requests:
            # All restoration requests must be issued before waiting for any one tablet.
            assert mount_requests == mounted_tablet_indexes
        states[:] = ["mounted" if state == "mounting" else state for state in states]
        return [{"state": state} for state in states]

    def unmount(path, sync):
        assert path == queue.path
        assert sync
        assert "mounting" not in states
        states[:] = ["unmounted"] * queue.tablet_count

    def mount(path, first_tablet_index, last_tablet_index, sync):
        assert path == queue.path
        assert first_tablet_index == last_tablet_index
        assert not sync
        assert states[first_tablet_index] == "unmounted"
        mount_requests.append(first_tablet_index)
        states[first_tablet_index] = "mounting"

    def set_attribute(path, value):
        assert path == f"{queue.path}/@hunk_storage_id"
        assert all(state == "unmounted" for state in states)
        if fail_change:
            raise stress.YtError("Link change failed")
        attributes["hunk_storage_id"] = value

    def remove_attribute(path):
        assert path == f"{queue.path}/@hunk_storage_id"
        assert all(state == "unmounted" for state in states)
        if fail_change:
            raise stress.YtError("Link change failed")
        del attributes["hunk_storage_id"]

    client.get.side_effect = get_tablets
    client.unmount_table.side_effect = unmount
    client.mount_table.side_effect = mount
    client.set.side_effect = set_attribute
    client.remove.side_effect = remove_attribute

    error_context = pytest.raises(stress.YtError, match="Link change failed") \
        if fail_change else nullcontext()
    with error_context:
        if operation == "link":
            stress.link(queue, new)
        elif operation == "unlink":
            stress.unlink(queue, old)
        else:
            with stress.sync_unmount_queue_temporarily(queue):
                stress.unlink(queue, old)
                stress.link(queue, new)

    assert states == [
        "mounted" if index in mounted_tablet_indexes else "unmounted"
        for index in range(queue.tablet_count)
    ]
    assert queue.mount_state.is_mounted_tablet == [state == "mounted" for state in states]
    assert all(queue.mount_state.is_sync)
    assert mount_requests == mounted_tablet_indexes
    assert client.unmount_table.call_count == int(any(s != "unmounted" for s in initial_states))
    if fail_change:
        if operation == "link":
            assert not attributes
            assert queue.hunk_storage_name is None
            assert not old.linked_queue_names
        else:
            assert attributes == {"hunk_storage_id": old.hunk_storage_id}
            assert queue.hunk_storage_name == old.name
            assert old.linked_queue_names == {queue.name}
        assert not new.linked_queue_names
    elif operation == "unlink":
        assert not attributes
        assert queue.hunk_storage_name is None
        assert not old.linked_queue_names
        assert not new.linked_queue_names
    else:
        assert attributes == {"hunk_storage_id": new.hunk_storage_id}
        assert queue.hunk_storage_name == new.name
        assert not old.linked_queue_names
        assert new.linked_queue_names == {queue.name}
    client.remount_table.assert_not_called()


@pytest.mark.parametrize("replicated", [False, True])
def test_stress_run_uses_one_external_cell(client, replicated):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({
        "create_probability": 1,
        "change_hunk_storage_probability": 1,
        "create_replicated_probability": int(replicated),
        "replicated_table_hunks_probability": 1,
        "replica_hunks_probability": 1,
    })
    raw_spec["size"]["iterations"] = 1
    nodes = {}
    links = []

    def create(kind, path=None, attributes=None):
        path = path or f"#replica-{len(nodes)}"
        assert "external" not in attributes
        nodes[path] = {
            "kind": kind,
            "attributes": copy.deepcopy(attributes),
            # Unpinned creation deliberately chooses different cells.
            "cell": attributes.get("external_cell_tag", 10 + len(nodes) % 3),
        }
        return path

    def get(path, attributes=None):
        if path == "//sys/@cluster_name":
            return "local-test"
        assert path.endswith("/@")
        assert attributes == ["external_cell_tag"]
        return {"external_cell_tag": nodes[path[:-2]]["cell"]}

    def set_link(path, storage):
        assert path.endswith("/@hunk_storage_id")
        owner = path.split("/@")[0]
        assert nodes[owner]["cell"] == nodes[storage]["cell"]
        links.append((owner, storage))

    client.create.side_effect = create
    client.get.side_effect = get
    client.set.side_effect = set_link

    stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)

    for node in nodes.values():
        if node["kind"] == "hunk_storage" or node["attributes"].get("schema") in (
            stress.QUEUE_SCHEMA, stress.QUEUE_SCHEMA_NO_HUNKS,
        ):
            assert node["cell"] == 10
    assert "//test/queue_3" in nodes
    assert "//test/hunk_storage_3" in nodes
    assert any(owner == "//test/queue_3" for owner, storage in links)


@pytest.mark.parametrize("operation", ["merge", "merge_with"])
@pytest.mark.parametrize("cell_tag", [12, None])
def test_operation_output_preserves_cell_when_converted_to_queue(client, operation, cell_tag):
    nodes = {"//test/source": {"cell": cell_tag}}

    def create(kind, path, attributes):
        assert kind == "table"
        assert "external" not in attributes
        nodes[path] = {
            "cell": attributes.get("external_cell_tag", 13 if cell_tag is not None else None),
            "schema": attributes["schema"],
        }

    def get(path, attributes=None):
        table_path, attribute = path.split("/@")
        if attribute == "schema":
            return nodes[table_path]["schema"]
        if attribute == "chunk_list_id":
            return "root"
        if path == "#root/@child_ids":
            return ["tablet"]
        if path == "#tablet/@statistics":
            return {"logical_data_weight": 0, "logical_hunk_data_weight": 0}
        assert attribute == ""
        assert attributes == ["external_cell_tag"]
        cell = nodes[table_path]["cell"]
        return {"external_cell_tag": cell} if cell is not None else {}

    client.create.side_effect = create
    client.get.side_effect = get
    client.read_table.return_value = []
    client.move.side_effect = lambda source, destination: nodes.update(
        {destination: nodes.pop(source)})
    client.Transaction.side_effect = lambda **kwargs: nullcontext()
    source = stress.StaticTable("//test", "source")

    if operation == "merge":
        result = source._run_merge()
    else:
        result = source.merge_with(stress.StaticTable("//test", "other"))
    queue = result.alter_to_queue("queue")

    assert nodes[queue.path]["cell"] == cell_tag


@pytest.mark.parametrize("operation", ["relink", "unlink", "alter_to_static"])
def test_link_changes_in_stress_loop(client, monkeypatch, operation):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg["unlink_hunk_storage_probability"] = int(operation == "unlink")
    cfg["alter_to_static_probability"] = int(operation == "alter_to_static")
    raw_spec["size"]["iterations"] = 2

    queues = {}
    states = {}
    cell_tags = {}
    storage_cell_tags = {}
    attributes = {}
    unmounted_queues = []
    altered = []
    events = []

    def create_queue(queue, attributes, erasure):
        queues[queue.path] = queue
        states[queue.path] = ["unmounted"] * queue.tablet_count
        cell_tags[queue.path] = attributes.get("external_cell_tag", 10 + len(queues))

    def create_storage(storage, erasure):
        storage.hunk_storage_id = storage.name
        assert storage.cell_tag is not None
        storage_cell_tags[storage.name] = storage.cell_tag

    def mount(path, sync, first_tablet_index=None, last_tablet_index=None):
        indexes = range(len(states[path])) if first_tablet_index is None else range(
            first_tablet_index, last_tablet_index + 1)
        for index in indexes:
            states[path][index] = "mounted" if sync else "mounting"
        events.append(("mount", path))
        # Change links on the second iteration, after queues have been mounted.
        cfg["change_hunk_storage_probability"] = int(operation != "alter_to_static")

    def unmount(path, sync):
        assert sync
        if any(state != "unmounted" for state in states[path]):
            unmounted_queues.append(path)
        states[path] = ["unmounted"] * len(states[path])
        events.append(("unmount", path))

    def get(path, attributes=None):
        if path == "//sys/@cluster_name":
            return "local-test"
        if path.endswith("/@"):
            assert attributes == ["external_cell_tag"]
            return {"external_cell_tag": cell_tags[path[:-2]]}
        table_path, attribute = path.split("/@")
        assert attribute == "tablets"
        states[table_path] = [
            "mounted" if state == "mounting" else state for state in states[table_path]
        ]
        return [{"state": state} for state in states[table_path]]

    def set_attribute(path, value):
        table_path, attribute = path.split("/@")
        assert attribute == "hunk_storage_id"
        assert all(state == "unmounted" for state in states[table_path])
        assert cell_tags[table_path] == storage_cell_tags[value]
        attributes[table_path] = value
        events.append(("set", table_path))

    def remove_attribute(path):
        table_path, attribute = path.split("/@")
        assert attribute == "hunk_storage_id"
        assert all(state == "unmounted" for state in states[table_path])
        del attributes[table_path]
        events.append(("remove", table_path))

    def alter_to_static(queue, name):
        assert all(state == "unmounted" for state in states[queue.path])
        assert queue.path not in attributes
        assert queue.hunk_storage_name is None
        altered.append(queue.path)
        return stress.StaticTable(queue.base_path, name)

    client.get.side_effect = get
    client.mount_table.side_effect = mount
    client.unmount_table.side_effect = unmount
    client.set.side_effect = set_attribute
    client.remove.side_effect = remove_attribute
    monkeypatch.setattr(stress.Queue, "create", create_queue)
    monkeypatch.setattr(stress.HunkStorage, "create", create_storage)
    monkeypatch.setattr(stress.HunkStorage, "mount", lambda *args, **kwargs: None)
    monkeypatch.setattr(stress.Queue, "alter_to_static", alter_to_static)

    stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)

    assert len(queues) == 3
    assert set(cell_tags.values()) == {11}
    assert set(storage_cell_tags.values()) == {11}
    assert sorted(unmounted_queues) == sorted(queues)
    if operation == "alter_to_static":
        assert set(altered) == set(queues)
        assert all(state == "unmounted" for tablets in states.values() for state in tablets)
    else:
        assert all(state == "mounted" for tablets in states.values() for state in tablets)
        assert all(all(queue.mount_state.is_mounted_tablet) for queue in queues.values())
    if operation == "relink":
        assert set(attributes) == set(queues)
        for index, (event, path) in enumerate(events):
            if event == "remove":
                assert events[index + 1] == ("set", path)
    else:
        assert not attributes
    client.remount_table.assert_not_called()
