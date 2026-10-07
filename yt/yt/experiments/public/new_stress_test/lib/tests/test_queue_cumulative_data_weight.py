from lib import test_queue_and_hunk_storage as stress
from lib.spec import Spec, spec_template

import pytest
from yt.common import WaitFailed
from yt.wrapper import retries

from contextlib import contextmanager, nullcontext
import copy
import re
from types import SimpleNamespace
from unittest.mock import Mock


@pytest.fixture
def client(monkeypatch):
    client = Mock(spec=stress.yt)
    client.TablePath = stress.yt.TablePath
    client.config = {"tablets_ready_timeout": 100, "tablets_check_interval": 1}
    client.Transaction.side_effect = lambda **kwargs: nullcontext()
    client.get.return_value = {"external_cell_tag": 11}
    monkeypatch.setattr(stress, "master_client", client)
    monkeypatch.setattr(stress, "tablet_client", client)
    monkeypatch.setattr(stress, "create_master_client", lambda config: client)
    monkeypatch.setattr(stress, "create_tablet_client", lambda config: client)
    return client


@pytest.fixture
def loop_spec(client, monkeypatch):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({"write_retry_count": 3, "write_min_batch_size": 1, "write_max_batch_size": 1})
    raw_spec["size"]["iterations"] = 1
    client.get.side_effect = lambda path, **kwargs: (
        "local-test" if path == "//sys/@cluster_name" else {"external_cell_tag": 11})
    monkeypatch.setattr(stress.random, "choice", lambda choices: choices[-1])
    monkeypatch.setattr(stress.random, "shuffle", lambda items: None)
    monkeypatch.setattr(retries.time, "sleep", Mock())
    return raw_spec


def _write_rows(client, monkeypatch, queue, rows, retry_count=1, expected_unmounted=False):
    tablets = iter(row[0] for row in rows)
    monkeypatch.setattr(stress.random, "choice", lambda choices: next(tablets))
    monkeypatch.setattr(stress.RSG, "generate", Mock(side_effect=[
        value for _, key, payload in rows for value in (key, payload)
    ] * retry_count))
    client.get_tablet_infos.side_effect = lambda path, indexes: {
        "tablets": [{"total_row_count": queue.written_row_count[index]} for index in indexes],
    }
    spec = SimpleNamespace(queue_and_hunk_storage=SimpleNamespace(
        write_min_batch_size=len(rows), write_max_batch_size=len(rows),
        write_min_row_size=1, write_max_row_size=1, write_insert_chunk_bytes=1,
    ))
    queue.write(True, spec, retry_count=retry_count, expected_unmounted=expected_unmounted)


def _shadow_rows(client, queue):
    return [
        row
        for call in client.insert_rows.call_args_list if call.args[0] == queue.data_path
        for row in call.args[1]
    ]


def _select_shadow_rows(query, rows):
    assert "offset" not in query
    tablet, start = map(int, re.search(
        r"tablet_index = (\d+) and row_index >= (\d+)", query).groups())
    limit = int(re.search(r"limit (\d+)", query)[1])
    return [row for row in rows
            if row["tablet_index"] == tablet and row["row_index"] >= start][:limit]


def _serve_rows(client, queue, expected_rows, actual_trimmed_row_counts=None):
    actual_rows = [{
        "key": row["key"], "value": row["value"],
        "$row_index": row["row_index"], "$tablet_index": row["tablet_index"],
        "$cumulative_data_weight": row["cumulative_data_weight"],
    } for row in expected_rows]

    def pull_queue(path, offset, partition_index, max_data_weight):
        trimmed = actual_trimmed_row_counts
        if trimmed is None:
            trimmed = queue._get_trimmed_row_counts(path)
        start = max(offset, trimmed[partition_index])
        return [row for row in actual_rows
                if row["$tablet_index"] == partition_index and row["$row_index"] >= start][:2]

    client.pull_queue.side_effect = pull_queue
    client.select_rows.side_effect = lambda query: _select_shadow_rows(query, expected_rows)
    return actual_rows


def _read(queue, path=None):
    queue._read_table_and_check(path or queue.path, SimpleNamespace(
        queue_and_hunk_storage=SimpleNamespace(read_page_max_data_weight=4096),
    ))


def test_writes_track_logical_byte_weights_per_tablet(client, monkeypatch):
    queue = stress.Queue("//test", "queue", 2)
    queue.mount()

    _write_rows(client, monkeypatch, queue, [
        (0, "", ""), (0, "я", "🙂"), (1, "k", "a" * 511),
        (0, "z", "b" * 512), (1, "ab", "c" * 513), (1, b"x", b"\xff"),
    ])
    expected = _shadow_rows(client, queue)
    assert [row["cumulative_data_weight"] for row in expected] == [9, 24, 521, 546, 1045, 1056]
    assert queue.cumulative_data_weights == [546, 1056]
    assert queue.written_row_count == [3, 3]
    for call in client.insert_rows.call_args_list:
        if call.args[0] == queue.path:
            assert all("$cumulative_data_weight" not in row for row in call.args[1])
    _serve_rows(client, queue, expected)
    _read(queue)


@pytest.mark.parametrize("trim_all", [False, True])
def test_remount_trim_and_append_preserve_weights(client, monkeypatch, trim_all):
    queue = stress.Queue("//test", "queue", 2)
    queue.mount()
    _write_rows(client, monkeypatch, queue, [
        (0, "a", "b"), (0, "cc", "h" * 1024), (1, "x", ""),
    ])
    assert queue.cumulative_data_weights == [1046, 10]
    _serve_rows(client, queue, _shadow_rows(client, queue))
    _read(queue)

    queue.unmount()
    queue.mount()
    queue.trim(0, 2 if trim_all else 1)
    queue.trim(1, 1)
    assert queue.cumulative_data_weights == [1046, 10]
    client.trim_rows.assert_any_call(queue.path, 0, 2 if trim_all else 1)
    client.trim_rows.assert_any_call(queue.path, 1, 1)
    _read(queue)

    queue.unmount()
    queue.mount()
    _write_rows(client, monkeypatch, queue, [(0, "d", "ef"), (1, "z", "")])
    assert queue.written_row_count == [3, 2]
    assert queue.cumulative_data_weights == [1058, 20]
    expected = _shadow_rows(client, queue)
    assert [row["cumulative_data_weight"] for row in expected[-2:]] == [1058, 20]
    _serve_rows(client, queue, expected)
    _read(queue)


@pytest.fixture(params=[False, True], ids=["plain", "replicated"])
def queue_to_trim(client, request):
    plans = [{"mode": "sync", "hunks": False}, {"mode": "async", "hunks": True}]
    queue = stress.Queue("//test", "queue", 1, replicas_plan=plans if request.param else None)
    queue.mount_state.mount(None)
    queue.written_row_count = [4]
    queue.trimmed_row_counts = [1]
    queue.cumulative_data_weights = [44]
    if queue.replicated:
        queue.replicas = [
            dict(plan, path=queue._replica_path(index), trimmed_row_counts=[1])
            for index, plan in enumerate(plans)
        ]
    client.get_tablet_infos.return_value = {"tablets": [{"total_row_count": 4}]}
    return queue


@pytest.mark.parametrize("error_code", [1702, 1744])
def test_trim_retries_tablet_state_errors(client, queue_to_trim, error_code):
    queue = queue_to_trim
    paths = [replica["path"] for replica in queue.replicas] if queue.replicated else [queue.path]
    attempts = []

    def trim_rows(path, tablet_index, trimmed_row_count):
        assert (tablet_index, trimmed_row_count) == (0, 4)
        assert queue.trimmed_row_counts == [1]
        attempts.append(path)
        if path == paths[-1] and attempts.count(path) == 1:
            raise stress.YtError("TrimTable failed", inner_errors=[stress.YtError(
                'Tablet is in "unfreezing" state while "mounted" expected', code=error_code)])

    client.trim_rows.side_effect = trim_rows
    queue.trim(0, 4)

    assert attempts == paths + [paths[-1]]
    assert queue.trimmed_row_counts == [4]
    assert all(replica["trimmed_row_counts"] == [4] for replica in queue.replicas)
    assert queue.cumulative_data_weights == [44]


@pytest.mark.parametrize("error_code", [42, 1702])
def test_failed_trim_preserves_counters(client, queue_to_trim, error_code):
    queue = queue_to_trim
    paths = [replica["path"] for replica in queue.replicas] if queue.replicated else [queue.path]
    attempts = []

    def trim_rows(path, tablet_index, trimmed_row_count):
        attempts.append(path)
        if path == paths[-1]:
            raise stress.YtError("Injected trim failure", code=error_code)

    client.trim_rows.side_effect = trim_rows
    if error_code == 1702:
        expected_error = pytest.raises(WaitFailed, match="did not become ready for trimming")
    else:
        expected_error = pytest.raises(stress.YtError, match="Injected trim failure")
    with expected_error:
        queue.trim(0, 4)

    assert queue.trimmed_row_counts == [1]
    assert queue.cumulative_data_weights == [44]
    if queue.replicated:
        assert queue.replicas[0]["trimmed_row_counts"] == [4]
        assert queue.replicas[1]["trimmed_row_counts"] == [1]
        assert attempts.count(paths[0]) == 1
    if error_code == 42:
        assert attempts == paths
    else:
        assert attempts.count(paths[-1]) > 1


@pytest.mark.parametrize("operation", ["copy", "move"])
def test_copy_and_move_preserve_weight_and_trim_state(client, monkeypatch, operation):
    queue = stress.Queue("//test", "queue", 2)
    queue.mount()
    queue.written_row_count = [2, 1]
    queue.trimmed_row_counts = [2, 0]
    queue.pruned_data_row_counts = [1, 0]
    queue.cumulative_data_weights = [1046, 10]
    result = getattr(queue, operation)("result")
    assert result.trimmed_row_counts == [2, 0]
    assert result.pruned_data_row_counts == [1, 0]
    assert result.pruned_data_row_counts is not queue.pruned_data_row_counts
    result.mount()
    _write_rows(client, monkeypatch, result, [(0, "d", "ef"), (1, "z", "")])
    assert result.cumulative_data_weights == [1058, 20]
    assert result.written_row_count == [3, 2]
    assert queue.cumulative_data_weights == [1046, 10]


@pytest.mark.parametrize("failure", ["queue", "shadow", "commit"])
def test_failed_write_does_not_advance_weight(client, monkeypatch, failure):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount()
    queue.written_row_count = [1]
    queue.cumulative_data_weights = [100]

    @contextmanager
    def transaction(**kwargs):
        yield
        if failure == "commit":
            raise stress.YtError("Commit failed")

    def insert(path, rows, **kwargs):
        if failure != "commit" and path == (queue.path if failure == "queue" else queue.data_path):
            raise stress.YtError("Insert failed")

    client.Transaction.side_effect = transaction
    client.insert_rows.side_effect = insert
    with pytest.raises(stress.YtError):
        _write_rows(client, monkeypatch, queue, [(0, "a", "b"), (0, "c", "d")])
    assert queue.written_row_count == [1]
    assert queue.cumulative_data_weights == [100]

    client.Transaction.side_effect = lambda **kwargs: nullcontext()
    client.insert_rows.side_effect = None
    client.insert_rows.reset_mock()
    _write_rows(client, monkeypatch, queue, [(0, "я", "🙂")])
    assert queue.cumulative_data_weights == [115]
    assert _shadow_rows(client, queue) == [{
        "key": "я", "value": "🙂", "tablet_index": 0, "row_index": 1,
        "cumulative_data_weight": 115,
    }]


@pytest.mark.parametrize("expected_unmounted", [False, True])
@pytest.mark.parametrize("error_code", [1702, 105])
def test_write_retry_limit_and_expected_unmount(client, monkeypatch, expected_unmounted, error_code):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount()
    queue.written_row_count = [1]
    queue.cumulative_data_weights = [100]
    sleep = Mock()
    monkeypatch.setattr(retries.time, "sleep", sleep)

    client.insert_rows.side_effect = stress.YtError("Injected write failure", code=error_code)
    with pytest.raises(stress.YtError, match="Injected write failure"):
        _write_rows(client, monkeypatch, queue, [(0, "a", "b")],
                    retry_count=3, expected_unmounted=expected_unmounted)

    expected_attempts = 1 if expected_unmounted and error_code == 1702 else 3
    assert client.Transaction.call_count == expected_attempts
    assert [call.args for call in sleep.call_args_list] == [(0.1,)] * (expected_attempts - 1)
    assert queue.written_row_count == [1]
    assert queue.cumulative_data_weights == [100]
    client.get_tablet_infos.assert_not_called()


@pytest.mark.parametrize("failure", ["second_queue", "hunk_commit", "persistent"])
def test_multi_queue_write_commits_and_retries_all_shadows(client, monkeypatch, failure):
    queues = [stress.Queue("//test", name, 2) for name in ("first", "second")]
    for queue in queues:
        queue.mount_state.mount(None)
        queue.written_row_count = [1, 2]
        queue.cumulative_data_weights = [10, 20]
    queues[1].replicated = True
    queues[1].replicas = [
        {"path": "//test/second.replica_0", "mode": "sync"},
        {"path": "//test/second.replica_1", "mode": "async"},
    ]
    placements = iter([0, 1, 0, 1, 0, 1])
    monkeypatch.setattr(stress.random, "choice", lambda choices: next(placements))
    monkeypatch.setattr(stress.RSG, "generate", lambda size: "x" * size)
    monkeypatch.setattr(retries.time, "sleep", Mock())
    spec = SimpleNamespace(queue_and_hunk_storage=SimpleNamespace(
        write_min_batch_size=3, write_max_batch_size=3,
        write_min_row_size=513, write_max_row_size=513, write_insert_chunk_bytes=1,
    ))
    attempts = []
    committed = {}
    active_transaction = False

    def check_old_counters():
        for queue in queues:
            assert queue.written_row_count == [1, 2]
            assert queue.cumulative_data_weights == [10, 20]

    @contextmanager
    def transaction(**kwargs):
        nonlocal active_transaction
        assert kwargs == {"type": "tablet"}
        assert not active_transaction
        check_old_counters()
        attempts.append([])
        active_transaction = True
        try:
            yield
            check_old_counters()
            if failure == "hunk_commit" and len(attempts) == 1:
                raise stress.YtError("Failed to write hunks", inner_errors=[stress.YtError("Unavailable", code=105)])
            for path, rows in attempts[-1]:
                committed.setdefault(path, []).extend(rows)
        finally:
            active_transaction = False

    def insert_rows(path, rows, **kwargs):
        assert active_transaction
        check_old_counters()
        attempts[-1].append((path, rows))
        if path == queues[1].path:
            if failure == "persistent" or (failure == "second_queue" and len(attempts) == 1):
                raise stress.YtError("Injected second queue failure", code=105)

    def get_tablet_infos(path, indexes):
        assert not active_transaction
        assert all(queue.written_row_count != [1, 2] for queue in queues)
        queue = queues[0] if path == queues[0].path else queues[1]
        return {"tablets": [{"total_row_count": queue.written_row_count[index]} for index in indexes]}

    client.Transaction.side_effect = transaction
    client.insert_rows.side_effect = insert_rows
    client.get_tablet_infos.side_effect = get_tablet_infos
    context = pytest.raises(stress.YtError) if failure == "persistent" else nullcontext()
    with context:
        writes = [(queue, queue._prepare_write(True, spec)) for queue in queues]
        stress.write_queues_under_transaction(writes, spec, retry_count=2)

    assert len(attempts) == 2
    if failure == "persistent":
        check_old_counters()
        assert not committed
        client.get_tablet_infos.assert_not_called()
        return

    assert queues[0].written_row_count == [3, 3]
    assert queues[1].written_row_count == [2, 4]
    assert queues[0].cumulative_data_weights == [1058, 544]
    assert queues[1].cumulative_data_weights == [534, 1068]
    for queue in queues:
        assert len(committed[queue.path]) == 3
        assert len(committed[queue.data_path]) == 3
        for tablet_index in range(2):
            rows = [row for row in committed[queue.data_path] if row["tablet_index"] == tablet_index]
            start = [1, 2][tablet_index]
            assert [row["row_index"] for row in rows] == list(range(start, start + len(rows)))
            assert [row["cumulative_data_weight"] for row in rows] == [
                [10, 20][tablet_index] + 524 * (i + 1) for i in range(len(rows))]
    assert len(attempts[-1]) == 12
    assert {call.args[0] for call in client.get_tablet_infos.call_args_list} == {
        queues[0].path, "//test/second.replica_0"}


def test_lost_commit_reply_does_not_duplicate_rows(client, monkeypatch):
    queues = [stress.Queue("//test", f"queue_{index}", 1) for index in range(2)]
    spec = SimpleNamespace(queue_and_hunk_storage=SimpleNamespace(
        write_min_row_size=1, write_max_row_size=1, write_insert_chunk_bytes=1024))
    monkeypatch.setattr(stress.RSG, "generate", lambda size: "x" * size)
    sleep = Mock()
    monkeypatch.setattr(retries.time, "sleep", sleep)
    pending = []
    stored = {}

    @contextmanager
    def transaction(**kwargs):
        yield
        for path, rows in pending:
            stored.setdefault(path, []).extend(rows)
        raise stress.YtError("Commit reply unavailable", code=105)

    client.Transaction.side_effect = transaction
    client.insert_rows.side_effect = lambda path, rows, **kwargs: pending.append((path, rows))
    with pytest.raises(stress.UnknownWriteCommitOutcome):
        stress.write_queues_under_transaction([(queue, [0]) for queue in queues], spec, retry_count=3, expected_unmounted=True)

    client.Transaction.assert_called_once_with(type="tablet")
    client.get_tablet_infos.assert_not_called()
    sleep.assert_not_called()
    for queue in queues:
        assert queue.written_row_count == [0]
        assert queue.cumulative_data_weights == [0]
        for path in (queue.path, queue.data_path):
            assert len(stored[path]) == 1


@pytest.mark.parametrize("rejection, expected_unmounted, should_fail", [
    (stress.YtError("Tablet is unmounted", code=1702), False, False),
    (stress.YtError("No such tablet 1-2-3-4", code=1701), False, False),
    (stress.YtError("Tablet routing rejected", code=1701), True, True),
    (stress.YtError("Table //test/queue has no mounted tablets"), False, False),
    (stress.YtError("Hunk storage //test/hunks has no mounted tablets"), False, False),
    (stress.YtError("No such transaction", code=11000, inner_errors=[
        stress.YtError("Failed to write hunks", inner_errors=[stress.YtError("Unavailable", code=105)]),
    ]), True, False),
])
def test_commit_rejections_retry_or_propagate_expected_unmounts(client, monkeypatch, rejection, expected_unmounted, should_fail):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount_state.mount(None)
    attempts = []
    sleep = Mock()
    monkeypatch.setattr(retries.time, "sleep", sleep)
    error = stress.YtError("Error committing transaction", inner_errors=[rejection])

    @contextmanager
    def transaction(**kwargs):
        attempts.append(None)
        yield
        if len(attempts) == 1:
            raise error

    client.Transaction.side_effect = transaction
    context = pytest.raises(stress.YtError) if should_fail else nullcontext()
    with context as result:
        _write_rows(client, monkeypatch, queue, [(0, "a", "h" * 1024)],
                    retry_count=3, expected_unmounted=expected_unmounted)
    if should_fail:
        assert result.value is error
    assert len(attempts) == (1 if should_fail else 2)
    assert sleep.call_count == (0 if should_fail else 1)
    assert queue.written_row_count == [0 if should_fail else 1]
    assert queue.cumulative_data_weights == [0 if should_fail else 1034]


@pytest.mark.parametrize("replicated", [False, True])
def test_pruning_preserves_all_physically_retained_prefixes(client, monkeypatch, replicated):
    queue = stress.Queue("//test", "queue", 2, replicas_plan=[] if replicated else None)
    queue.written_row_count = [10, 10]
    queue.trimmed_row_counts = [5, 4]
    queue.pruned_data_row_counts = [1, 0]
    queue.cumulative_data_weights = [100, 200]
    prefixes = [[3, 2]]
    paths = [queue.path]
    if replicated:
        queue.replicas = [
            {"path": queue._replica_path(0), "trimmed_row_counts": [5, 4], "mode": "sync"},
            {"path": queue._replica_path(1), "trimmed_row_counts": [5, 4], "mode": "async"},
            {"path": queue._replica_path(2), "trimmed_row_counts": [9, 9], "mode": "sync"},
        ]
        prefixes = [[3, 2], [4, 1], [9, 9]]
        paths = [replica["path"] for replica in queue.replicas]
    attributes = {}
    for i, path in enumerate(paths):
        attributes[f"{path}/@chunk_list_id"] = f"root-{i}"
        attributes[f"#root-{i}/@child_ids"] = [f"tablet-{i}-0", f"tablet-{i}-1"]
        for tablet_index, start in enumerate(prefixes[i]):
            attributes[f"#tablet-{i}-{tablet_index}/@statistics"] = {
                "logical_row_count": 10, "row_count": 10 - start}
    client.get.side_effect = lambda path: attributes[path]
    monkeypatch.setattr(stress, "DATA_TABLE_WRITE_BATCH_SIZE", 1)

    queue.prune_data_rows()

    retained = [3, 1 if replicated else 2]
    assert queue.pruned_data_row_counts == retained
    deleted = [row for call in client.delete_rows.call_args_list for row in call.args[1]]
    assert deleted == [
        {"tablet_index": tablet_index, "row_index": row_index}
        for tablet_index in range(2)
        for row_index in range([1, 0][tablet_index], retained[tablet_index])
    ]
    assert all(call.args[0] == queue.data_path for call in client.delete_rows.call_args_list)
    assert queue.written_row_count == [10, 10]
    assert queue.trimmed_row_counts == [5, 4]
    assert queue.cumulative_data_weights == [100, 200]
    client.select_rows.assert_not_called()
    client.read_table.assert_not_called()

    client.delete_rows.reset_mock()
    queue.prune_data_rows()
    client.delete_rows.assert_not_called()


def test_pruning_tracks_progress_and_stops_polling_when_caught_up(client, monkeypatch):
    queue = stress.Queue("//test", "queue", 1)
    queue._get_retained_row_indexes = Mock(return_value=[2])
    monkeypatch.setattr(stress, "DATA_TABLE_WRITE_BATCH_SIZE", 2)
    queue.prune_data_rows()
    queue._get_retained_row_indexes.assert_not_called()

    queue.written_row_count = [5]
    queue.trimmed_row_counts = [5]
    queue.cumulative_data_weights = [55]
    queue.prune_data_rows()
    queue.prune_data_rows()
    client.delete_rows.assert_called_once_with(queue.data_path, [
        {"tablet_index": 0, "row_index": index} for index in range(2)])
    assert queue.pruned_data_row_counts == [2]

    # Physical trimming can advance without another trim request.
    queue._get_retained_row_indexes.return_value = [5]
    client.delete_rows.side_effect = [None, stress.YtError("Injected delete failure")]
    with pytest.raises(stress.YtError, match="Injected delete failure"):
        queue.prune_data_rows()
    assert queue.pruned_data_row_counts == [4]

    client.delete_rows.reset_mock(side_effect=True)
    queue.prune_data_rows()
    client.delete_rows.assert_called_once_with(queue.data_path, [{"tablet_index": 0, "row_index": 4}])
    assert queue.pruned_data_row_counts == [5]
    queue._get_retained_row_indexes.reset_mock()
    queue.prune_data_rows()
    queue._get_retained_row_indexes.assert_not_called()
    assert client.delete_rows.call_count == 1
    assert queue.written_row_count == [5]
    assert queue.cumulative_data_weights == [55]


def test_operation_outputs_registered_before_later_failure(client):
    table = stress.StaticTable("//test", "input")
    result = stress.StaticTable("//test", "result")
    registered = {}

    def fail_merge():
        assert registered == {"result": result}
        raise stress.YtError("Injected operation failure")

    table._run_sort = Mock(return_value=result)
    table._run_merge = fail_merge
    table._run_map_reduce = Mock()
    table._run_map = Mock()
    spec = SimpleNamespace(queue_and_hunk_storage=SimpleNamespace(**{
        "run_" + operation + "_probability": 1 for operation in ("sort", "merge", "map_reduce", "map")}))
    with pytest.raises(stress.YtError, match="Injected operation failure"):
        for new_table in table.run_operations(spec):
            registered[new_table.name] = new_table
    table._run_map_reduce.assert_not_called()
    table._run_map.assert_not_called()


@pytest.mark.parametrize("known_weight, column, bad_value", [
    (True, "$cumulative_data_weight", 11),
    (True, "$cumulative_data_weight", stress.yson.YsonEntity()),
    (True, "$cumulative_data_weight", None),
    (True, "$row_index", 3),
    (False, "key", "wrong"),
    (False, "value", "wrong"),
    (False, "$row_index", 3),
])
def test_reads_reject_corruption(client, known_weight, column, bad_value):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount()
    queue.written_row_count = [4]
    queue.trimmed_row_counts = [2]
    expected = [
        {"tablet_index": 0, "row_index": index, "key": "k", "value": "v",
         "cumulative_data_weight": 11 * (index + 1) if known_weight else None}
        for index in range(4)
    ]
    actual = _serve_rows(client, queue, expected)
    if bad_value is None:
        del actual[2][column]
    else:
        actual[2][column] = bad_value
    with pytest.raises(stress.YtError, match="Unexpected .* in //test/queue, tablet 0"):
        _read(queue)


@pytest.mark.parametrize("trimmed", [2, 4])
def test_reads_reject_ignored_trim(client, trimmed):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount()
    queue.written_row_count = [4]
    rows = [
        {"tablet_index": 0, "row_index": index, "key": "k", "value": "v",
         "cumulative_data_weight": 11 * (index + 1)}
        for index in range(4)
    ]
    _serve_rows(client, queue, rows, actual_trimmed_row_counts=[0])
    queue.trim(0, trimmed)

    with pytest.raises(stress.YtError, match="//test/queue"):
        _read(queue)
    assert client.pull_queue.call_args_list[-1].kwargs["offset"] == 0


def test_expected_rows_page_by_row_index_across_tablets(client, monkeypatch):
    monkeypatch.setattr(stress, "DATA_TABLE_READ_BATCH_SIZE", 2)
    queue = stress.Queue("//test", "queue", 3)
    queue.trimmed_row_counts = [10000, 10, 20000]
    rows = [
        {"tablet_index": tablet, "row_index": start + index, "key": "k", "value": "v",
         "cumulative_data_weight": 11 * (start + index + 1)}
        for tablet, start, count in ((0, 0, 2), (0, 10000, 3), (2, 20000, 3))
        for index in range(count)
    ]
    client.select_rows.side_effect = lambda query: _select_shadow_rows(query, rows)

    assert queue.get_expected_rows() == rows[2:]
    bounds = [
        tuple(map(int, re.search(
            r"tablet_index = (\d+) and row_index >= (\d+)", call.args[0]).groups()))
        for call in client.select_rows.call_args_list
    ]
    assert bounds == [
        (0, 10000), (0, 10002), (0, 10003), (1, 10),
        (2, 20000), (2, 20002), (2, 20003),
    ]


def test_reads_page_by_row_index_after_large_trim(client):
    queue = stress.Queue("//test", "queue", 1)
    queue.mount()
    queue.written_row_count = [10005]
    queue.trimmed_row_counts = [10000]
    rows = [
        {"tablet_index": 0, "row_index": index, "key": "k", "value": "v",
         "cumulative_data_weight": 11 * (index + 1)}
        for index in range(10000, 10005)
    ]
    _serve_rows(client, queue, rows)

    _read(queue)

    assert [call.kwargs["offset"] for call in client.pull_queue.call_args_list] == [
        0, 10002, 10004, 10005,
    ]
    assert [int(re.search(r"row_index >= (\d+)", call.args[0])[1])
            for call in client.select_rows.call_args_list] == [10000, 10002, 10004]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_new_replica_continues_old_replica_weights(client, monkeypatch, mode):
    queue = stress.Queue(
        "//test", "queue", 2,
        replicas_plan=[{"mode": "sync", "hunks": False}], cluster_name="local-test",
    )
    queue.create({}, erasure=False)
    _write_rows(client, monkeypatch, queue, [(0, "a", "b"), (0, "c", "d")])
    queue.trim(0, 1)
    queue.add_replica({"mode": mode, "hunks": True})
    replica = queue.replicas[-1]
    created = [call.kwargs["attributes"] for call in client.create.call_args_list
               if call.args[0] == "table_replica"][-1]
    assert created["start_replication_row_indexes"] == [2, 0]
    replica_attributes = next(
        call.kwargs["attributes"] for call in client.create.call_args_list
        if call.args[:2] == ("table", replica["path"])
    )
    assert replica_attributes["trimmed_row_counts"] == [2, 0]
    assert replica_attributes["cumulative_data_weights"] == [22, 0]

    _write_rows(client, monkeypatch, queue, [(0, "я", "🙂"), (1, "h", "v" * 512)])
    assert queue.cumulative_data_weights == [37, 522]
    assert replica_attributes["cumulative_data_weights"] == [22, 0]
    expected = _shadow_rows(client, queue)
    _serve_rows(client, queue, expected)
    for table in queue.replicas:
        _read(queue, table["path"])
    new_offsets = [call.kwargs["offset"] for call in client.pull_queue.call_args_list
                   if call.args[0] == replica["path"]]
    assert new_offsets[0] == 0

    queue.trim(0, 3)
    assert all(table["trimmed_row_counts"] == [3, 0] for table in queue.replicas)
    assert queue.cumulative_data_weights == [37, 522]


def test_operations_use_new_replicas_before_queue_trim_catches_up(client, monkeypatch):
    queue = stress.Queue(
        "//test", "queue", 2, replicas_plan=[{"mode": "async", "hunks": False}],
        cluster_name="local-test",
    )
    queue.create({}, erasure=False)
    queue.written_row_count = [3, 2]
    queue.cumulative_data_weights = [33, 22]
    queue.add_replica({"mode": "async", "hunks": True})
    queue.written_row_count = [4, 3]
    client.get_tablet_infos.return_value = {
        "tablets": [{"total_row_count": 4}, {"total_row_count": 3}],
    }
    monkeypatch.setattr(stress.random, "choice", lambda choices: choices[-1])
    path = queue._input_path()
    assert str(path) == queue.replicas[1]["path"]
    client.get.side_effect = {
        f"{path}/@chunk_list_id": "root",
        "#root/@child_ids": ["tablet-0", "tablet-1"],
        "#tablet-0/@statistics": {"logical_row_count": 3, "row_count": 0},
        "#tablet-1/@statistics": {"logical_row_count": 2, "row_count": 0},
    }.__getitem__
    rows = [
        {"tablet_index": tablet, "row_index": index, "key": "k", "value": "v"}
        for tablet, count in enumerate(queue.written_row_count) for index in range(count)
    ]
    client.select_rows.side_effect = lambda query: _select_shadow_rows(query, rows)
    assert queue._get_operation_expected_rows(path) == [rows[3], rows[6]]
    assert queue.trimmed_row_counts == [0, 0]


@pytest.mark.parametrize("existing_weight, sorted_schema, hunk_weight, trim_all", [
    (None, False, 0, False), (100, False, 600, True), (None, True, 600, False),
])
def test_static_conversion_uses_logical_weight_for_new_rows(
    client, monkeypatch, existing_weight, sorted_schema, hunk_weight, trim_all,
):
    table = stress.StaticTable("//test", "static")
    row = {"key": "я", "value": "🙂"}
    if existing_weight is not None:
        row["$cumulative_data_weight"] = existing_weight
    data_row = {"key": row["key"], "value": row["value"]}
    client.read_table.side_effect = lambda path: [data_row if path == table.data_path else row]
    schema = stress.SORTED_KV_SCHEMA if sorted_schema else (
        stress.UNSORTED_KV_SCHEMA if existing_weight is None else stress.QUEUE_SCHEMA)
    statistics = {"logical_data_weight": 101, "logical_hunk_data_weight": hunk_weight}

    def get(path):
        if path == "#tablet/@statistics":
            client.alter_table.assert_any_call(
                "//test/queue", dynamic=True, schema=stress.QUEUE_SCHEMA)
            assert not any(call.args[0] == "//test/queue"
                           for call in client.mount_table.call_args_list)
            return statistics
        return {
            f"{table.path}/@schema": schema,
            "//test/queue/@chunk_list_id": "root",
            "#root/@child_ids": ["tablet"],
        }[path]

    client.get.side_effect = get

    queue = table.alter_to_queue("queue")
    assert queue.cumulative_data_weights == [101 + hunk_weight]
    expected = _shadow_rows(client, queue)
    assert expected[0]["cumulative_data_weight"] is None
    expected[0]["cumulative_data_weight"] = stress.yson.YsonEntity()
    actual = _serve_rows(client, queue, expected)
    actual[0]["$cumulative_data_weight"] = 10000
    _read(queue)

    _write_rows(client, monkeypatch, queue, [(0, "a", "b")])
    assert queue.cumulative_data_weights == [112 + hunk_weight]
    actual = _serve_rows(client, queue, _shadow_rows(client, queue))
    del actual[0]["$cumulative_data_weight"]
    _read(queue)

    queue.trim(0, 2 if trim_all else 1)
    queue.unmount()
    queue.mount()
    _read(queue)
    _write_rows(client, monkeypatch, queue, [(0, "c", "d")])
    assert queue.cumulative_data_weights == [123 + hunk_weight]
    actual = _serve_rows(client, queue, _shadow_rows(client, queue))
    _read(queue)
    actual[-1]["$cumulative_data_weight"] = 11
    with pytest.raises(stress.YtError, match="Unexpected cumulative data weight"):
        _read(queue)


def test_weight_initialization_keeps_tablet_totals_separate(client):
    queue = stress.Queue("//test", "queue", 3)
    client.get.side_effect = {
        f"{queue.path}/@chunk_list_id": "root",
        "#root/@child_ids": ["tablet-0", "tablet-1", "tablet-2"],
        "#tablet-0/@statistics": {"logical_data_weight": 123, "logical_hunk_data_weight": 600},
        "#tablet-1/@statistics": {"logical_data_weight": 99, "logical_hunk_data_weight": 0},
        "#tablet-2/@statistics": {"logical_data_weight": 0, "logical_hunk_data_weight": 0},
    }.__getitem__
    queue.initialize_cumulative_data_weights()
    assert queue.cumulative_data_weights == [723, 99, 0]


@pytest.mark.parametrize("corrupt", [False, True])
def test_static_conversion_reads_once_and_preserves_row_order(client, corrupt):
    table = stress.StaticTable("//test", "static")
    rows = [{"key": "z", "value": "last"}, {"key": "a", "value": "first"}]
    expected = sorted(copy.deepcopy(rows), key=lambda row: row["key"])
    if corrupt:
        rows[0]["value"] = "wrong"
    client.read_table.side_effect = {table.path: rows, table.data_path: expected}.__getitem__
    client.get.side_effect = {
        f"{table.path}/@schema": stress.UNSORTED_KV_SCHEMA,
        "//test/queue/@chunk_list_id": "root",
        "#root/@child_ids": ["tablet"],
        "#tablet/@statistics": {"logical_data_weight": 13, "logical_hunk_data_weight": 0},
    }.__getitem__

    if corrupt:
        with pytest.raises(stress.YtError, match="was expected"):
            table.alter_to_queue("queue")
        client.alter_table.assert_not_called()
    else:
        queue = table.alter_to_queue("queue")
        actual = _shadow_rows(client, queue)
        assert [row["key"] for row in actual] == ["z", "a"]
        assert [row["row_index"] for row in actual] == [0, 1]

    assert [call.args[0] for call in client.read_table.call_args_list] == [
        table.path, table.data_path,
    ]


@pytest.mark.parametrize("retained_counts", [(3, 2), (2, 0), (1, 0)])
def test_trimmed_queue_conversion_preserves_physically_retained_rows(client, retained_counts):
    queue = stress.Queue("//test", "queue", 2)
    queue.mount()
    queue.written_row_count = [3, 2]
    queue.trimmed_row_counts = [2, 2]
    retained = [
        {"key": str(tablet), "value": str(index), "row_index": index, "tablet_index": tablet,
         "cumulative_data_weight": 11 * (index + 1)}
        for tablet, count in enumerate(retained_counts)
        for index in range(queue.written_row_count[tablet] - count, queue.written_row_count[tablet])
    ]

    def select_rows(query):
        tablet, start = map(int, re.search(
            r"tablet_index = (\d+) and row_index >= (\d+)", query).groups())
        assert start >= queue.written_row_count[tablet] - retained_counts[tablet]
        return _select_shadow_rows(query, retained)

    client.select_rows.side_effect = select_rows
    client.get.side_effect = {
        f"{queue.path}/@chunk_list_id": "root",
        "#root/@child_ids": ["tablet-0", "tablet-1"],
        "#tablet-0/@statistics": {"logical_row_count": 3, "row_count": retained_counts[0]},
        "#tablet-1/@statistics": {"logical_row_count": 2, "row_count": retained_counts[1]},
        f"{queue.path}/@chunk_ids": [],
    }.__getitem__
    client.read_table.side_effect = lambda path: (
        client.write_table.call_args.args[1] if path.endswith(".data") else [
            {"key": row["key"], "value": row["value"],
             "$cumulative_data_weight": -100}
            for row in retained
        ]
    )
    table = queue.alter_to_static("static")
    client.alter_table.assert_called_once_with(table.path, dynamic=False)
    assert queue.trimmed_row_counts == [2, 2]
    assert client.write_table.call_args.args[1] == [
        {"key": row["key"], "value": row["value"]} for row in retained
    ]


@pytest.mark.parametrize("mode, operation", [
    ("plain", "merge"), ("sync", "merge"), ("async", "merge_with"),
])
def test_operations_include_retained_prefix_and_unflushed_rows(
    client, monkeypatch, mode, operation,
):
    transactions = []
    locks = {}
    source_rows = {}
    shadow_rows = {}
    attributes = {}
    replica_choices = []

    @contextmanager
    def transaction(transaction_id=None):
        transactions.append(transaction_id or "master")
        try:
            yield
        finally:
            transactions.pop()

    def lock(path, mode):
        assert mode == "snapshot"
        assert transactions == ["master"]
        locks[path] = transactions[-1]

    def choose(values):
        if isinstance(values[0], dict):
            replica_choices.append(values)
        return values[-1]

    def make_queue(name, trimmed, written, flushed, retained):
        plans = None if mode == "plain" else [{"mode": mode, "hunks": False}] * 2
        queue = stress.Queue("//test", name, 2, replicas_plan=plans, cluster_name="local-test")
        queue.create({}, erasure=False)
        queue.trimmed_row_counts = trimmed
        queue.written_row_count = written
        path = queue.path if mode == "plain" else queue.replicas[-1]["path"]
        rows = [
            {"tablet_index": tablet, "row_index": index,
             "key": f"{name}-{tablet}-{index}", "value": "v", "cumulative_data_weight": None}
            for tablet, count in enumerate(written) for index in range(count)
        ]
        shadow_rows[queue.data_path] = rows
        source_rows[path] = [
            {"key": row["key"], "value": row["value"]} for row in rows
            if row["row_index"] >= flushed[row["tablet_index"]] - retained[row["tablet_index"]]
        ]
        attributes[f"{path}/@chunk_list_id"] = name
        attributes[f"#{name}/@child_ids"] = [f"{name}-0", f"{name}-1"]
        for tablet in range(2):
            attributes[f"#{name}-{tablet}/@statistics"] = {
                "logical_row_count": flushed[tablet], "row_count": retained[tablet],
            }
        if mode != "plain":
            queue.replicas[-1]["trimmed_row_counts"] = list(trimmed)
        return queue

    queue = make_queue("queue", [4, 3], [9, 5], [6, 4], [3, 3])
    other = make_queue("other", [2, 1], [5, 2], [4, 2], [3, 2])

    def get_tablet_infos(path, indexes):
        assert not transactions
        source = other if str(path).startswith(other.path) else queue
        return {"tablets": [
            {"total_row_count": source.written_row_count[index]} for index in indexes
        ]}

    def get(path, **kwargs):
        if path.endswith("/@"):
            return {"external_cell_tag": 11}
        assert transactions == ["master"]
        if path.endswith("/@chunk_list_id"):
            assert path[:-len("/@chunk_list_id")] in locks
        return attributes[path]

    def select_rows(query):
        assert transactions == ["master", stress.YT_NULL_TRANSACTION_ID]
        path = re.search(r"from \[(.*?)\]", query)[1]
        return _select_shadow_rows(query, shadow_rows[path])

    tables = {}

    def write_table(path, rows):
        tables[path] = rows

    def run_merge(inputs, output, **kwargs):
        assert transactions == ["master"]
        inputs = inputs if isinstance(inputs, list) else [inputs]
        tables[output] = []
        for input_path in inputs:
            assert locks[str(input_path)] == transactions[-1]
            tables[output].extend(source_rows[str(input_path)])

    client.Transaction.side_effect = transaction
    client.lock.side_effect = lock
    client.get_tablet_infos.side_effect = get_tablet_infos
    client.get.side_effect = get
    client.select_rows.side_effect = select_rows
    client.write_table.side_effect = write_table
    client.read_table.side_effect = lambda path: tables[path]
    client.run_merge.side_effect = run_merge
    monkeypatch.setattr(stress.random, "choice", choose)

    result = queue.merge_with(other) if operation == "merge_with" else getattr(
        queue, "_run_" + operation)()
    input_count = 2 if operation == "merge_with" else 1
    assert len(locks) == input_count
    assert len(replica_choices) == (input_count if mode != "plain" else 0)
    assert len(tables[result.data_path]) == (16 if operation == "merge_with" else 10)
    assert queue.trimmed_row_counts == [4, 3]
    assert other.trimmed_row_counts == [2, 1]
    assert not transactions


@pytest.mark.parametrize("unused_state, only_in_sync_mounted", [
    ("unmounted", False), ("mounting", False), ("unmounting", True),
])
def test_unused_tablet_does_not_excuse_write_failure(client, monkeypatch, loop_spec, unused_state, only_in_sync_mounted):
    loop_spec["queue_and_hunk_storage"].update({"max_table_count": 1, "write_probability": 1})
    monkeypatch.setattr(stress.random, "choice", lambda choices: (
        only_in_sync_mounted if choices == [True, False] else choices[-1]))
    original_prepare_write = stress.Queue._prepare_write

    def prepare_write(queue, only_in_sync_mounted, spec):
        if unused_state == "mounting":
            queue.mount_state.mount(0, sync=False)
        else:
            queue.mount_state.unmount(0, sync=unused_state == "unmounted")
        plan = original_prepare_write(queue, only_in_sync_mounted, spec)
        assert plan == [4]
        return plan

    monkeypatch.setattr(stress.Queue, "_prepare_write", prepare_write)
    client.insert_rows.side_effect = stress.YtError("Unexpected unmount of selected tablet", code=1702)
    with pytest.raises(stress.YtError, match="Unexpected unmount of selected tablet"):
        stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)
    assert client.Transaction.call_count == 3


@pytest.mark.parametrize("error_code, message", [
    (1701, "No such tablet 1-2-3-4"),
    (1, "Table //test/queue_0 has no mounted tablets"),
    (11000, "Transaction is not known"),
])
def test_stress_loop_distinguishes_commit_rejections_from_unknown_outcomes(client, monkeypatch, loop_spec, error_code, message):
    loop_spec["queue_and_hunk_storage"].update({"max_table_count": 1, "write_probability": 1})
    original_prepare_write = stress.Queue._prepare_write
    planned_queues = []

    def prepare_write(queue, only_in_sync_mounted, spec):
        queue.mount_state.unmount(queue.tablet_count - 1, sync=False)
        planned_queues.append(queue)
        return original_prepare_write(queue, only_in_sync_mounted, spec)

    @contextmanager
    def transaction(**kwargs):
        yield
        raise stress.YtError("Error committing transaction", inner_errors=[stress.YtError(message, code=error_code)])

    client.Transaction.side_effect = transaction
    monkeypatch.setattr(stress.Queue, "_prepare_write", prepare_write)
    context = pytest.raises(stress.UnknownWriteCommitOutcome) if error_code == 11000 else nullcontext()
    with context:
        stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)

    assert client.Transaction.call_count == (1 if error_code == 11000 else 2)
    assert all(not any(queue.written_row_count) for queue in planned_queues)
    assert all(not any(queue.cumulative_data_weights) for queue in planned_queues)
    client.get_tablet_infos.assert_not_called()


@pytest.mark.parametrize("unmounted_index, unmounted_object", [(1, "queue"), (2, "hunk_storage")])
def test_grouped_writes_isolate_expected_unmounts(client, monkeypatch, loop_spec, unmounted_index, unmounted_object):
    loop_spec["queue_and_hunk_storage"].update({"write_probability": 1, "multi_queue_write_probability": 1})
    storage_indexes = iter(range(3))
    storages = {}

    def choose(choices):
        if isinstance(choices[0], stress.HunkStorage):
            storage = choices[next(storage_indexes)]
            storages[storage.name] = storage
            return storage
        return choices[-1]

    monkeypatch.setattr(stress.random, "choice", choose)
    queues = {}
    unmounted_name = f"queue_{unmounted_index}"
    healthy_names = [f"queue_{index}" for index in (2, 1, 0) if index != unmounted_index]
    original_prepare_write = stress.Queue._prepare_write

    def prepare_write(queue, only_in_sync_mounted, spec):
        queues[queue.path] = queue
        plan = original_prepare_write(queue, only_in_sync_mounted, spec)
        if queue.name == unmounted_name:
            if unmounted_object == "queue":
                queue.mount_state.unmount(plan[0], sync=False)
            else:
                storages[queue.hunk_storage_name].mount_state.unmount(None)
        return plan

    monkeypatch.setattr(stress.Queue, "_prepare_write", prepare_write)
    client.get_tablet_infos.side_effect = lambda path, indexes: {
        "tablets": [{"total_row_count": queues[path].written_row_count[index]} for index in indexes]}
    groups = []
    attempts = {}
    original_write_queues_under_transaction = stress.write_queues_under_transaction

    def write_queues_under_transaction(writes, spec, retry_count, expected_unmounted=False):
        names = [queue.name for queue, tablet_plan in writes]
        groups.append(names)
        assert names == ([unmounted_name] if expected_unmounted else healthy_names)
        original_write_queues_under_transaction(writes, spec, retry_count, expected_unmounted)

    def write_batches(queue, tablet_plan, spec):
        attempts[queue.name] = attempts.get(queue.name, 0) + 1
        if queue.name == unmounted_name or (queue.name == healthy_names[0] and attempts[queue.name] == 1):
            raise stress.YtError("Unmounted tablet", code=1702)
        counts = list(queue.written_row_count)
        weights = list(queue.cumulative_data_weights)
        counts[tablet_plan[0]] += 1
        weights[tablet_plan[0]] += 11
        return counts, weights

    def prune(queue):
        assert len(groups) == 4

    pruning = Mock(side_effect=prune)
    monkeypatch.setattr(stress, "write_queues_under_transaction", write_queues_under_transaction)
    monkeypatch.setattr(stress.Queue, "_write_batches", write_batches)
    monkeypatch.setattr(stress.Queue, "prune_data_rows", lambda queue: pruning(queue))
    stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)

    assert groups.count(healthy_names) == 2
    assert attempts == {unmounted_name: 2, healthy_names[0]: 3, healthy_names[1]: 2}
    assert [call.args[0].name for call in pruning.call_args_list] == ["queue_0", "queue_1", "queue_2"]
    for queue in queues.values():
        expected_rows = 0 if queue.name == unmounted_name else 2
        assert sum(queue.written_row_count) == expected_rows
        assert sum(queue.cumulative_data_weights) == 11 * expected_rows


def test_stress_loop_trims_and_adds_replicas_after_writes(client, monkeypatch, loop_spec):
    cfg = loop_spec["queue_and_hunk_storage"]
    cfg.update({
        "create_replicated_probability": 1, "replicated_min_replicas": 1,
        "replicated_max_replicas": 2, "write_probability": 1,
        "trim_probability": 1, "add_replica_probability": 1,
    })
    loop_spec["size"]["iterations"] = 2
    monkeypatch.setattr(stress.random, "choice", lambda choices: choices[0])
    monkeypatch.setattr(stress.random, "randint", lambda lower, upper: lower)
    monkeypatch.setattr(stress.Queue, "_get_retained_row_indexes", lambda queue, path: [0] * queue.tablet_count)
    queues = {}

    def write(writes, **kwargs):
        for queue, tablet_plan in writes:
            queues[queue.path] = queue
            queue.written_row_count[0] += 1
            queue.cumulative_data_weights[0] += 11

    def get_tablet_infos(path, indexes):
        queue = queues[path.split(".replica_")[0]]
        return {"tablets": [{"total_row_count": queue.written_row_count[i]} for i in indexes]}

    client.get_tablet_infos.side_effect = get_tablet_infos
    monkeypatch.setattr(stress, "write_queues_under_transaction", write)
    stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)
    assert len(queues) == 3
    for queue in queues.values():
        assert queue.cumulative_data_weights == [44]
        assert queue.trimmed_row_counts == [4]
        assert len(queue.replicas) == 2
        assert all(replica["trimmed_row_counts"] == [4] for replica in queue.replicas)


@pytest.mark.parametrize("growth", ["create", "copy", "operations", "merge", "static_copy", "static_operations"])
def test_stress_loop_bounds_live_tables(client, monkeypatch, loop_spec, growth):
    limit = 5
    cfg = loop_spec["queue_and_hunk_storage"]
    cfg["max_table_count"] = limit
    if growth == "create":
        cfg["create_probability"] = 1
    elif growth == "copy":
        cfg["copy_probability"] = 1
    elif growth == "merge":
        cfg["merge_two_tables_probability"] = 1
    elif growth == "static_copy":
        cfg["copy_static_table_probability"] = 1
    if growth in ("operations", "static_operations"):
        for operation in ("sort", "merge", "map_reduce", "map"):
            cfg[f"run_{operation}_probability"] = 1
    if growth.startswith("static"):
        cfg["alter_to_static_probability"] = 1
    loop_spec["size"]["iterations"] = 3
    live = {}
    outputs = []

    def register(table):
        assert table.path not in live
        live[table.path] = table
        assert len(live) <= limit
        return table

    def create(queue, attributes, erasure):
        register(queue)

    def copy_queue(queue, name):
        return register(stress.Queue(queue.base_path, name, queue.tablet_count))

    def copy_static(table, name):
        return register(stress.StaticTable(table.base_path, name))

    def to_static(queue, name):
        del live[queue.path]
        return register(stress.StaticTable(queue.base_path, name))

    def operation(table, *args):
        outputs.append(table)
        return register(stress.StaticTable(table.base_path, f"output_{len(outputs)}"))

    monkeypatch.setattr(stress.Queue, "create", create)
    monkeypatch.setattr(stress.Queue, "copy", copy_queue)
    monkeypatch.setattr(stress.StaticTable, "copy", copy_static)
    monkeypatch.setattr(stress.Queue, "alter_to_static", to_static)
    for name in ("_run_sort", "_run_merge", "_run_map_reduce", "_run_map", "merge_with"):
        monkeypatch.setattr(stress.TableBase, name, operation)
    if growth == "static_operations":
        monkeypatch.setattr(stress.Queue, "run_operations", lambda *args, **kwargs: iter(()))

    stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)
    assert len(live) == limit
    if growth == "static_operations" and outputs:
        assert all(isinstance(table, stress.StaticTable) for table in outputs)


def test_table_limit_allows_conversions_and_reuses_removed_slots(client, monkeypatch, loop_spec):
    cfg = loop_spec["queue_and_hunk_storage"]
    cfg.update({"max_table_count": 1, "create_probability": 1, "alter_to_static_probability": 1,
                "alter_to_queue_probability": 1, "remove_static_table_probability": 1})
    loop_spec["size"]["iterations"] = 3
    live = set()
    created = []
    removed = []
    conversions = []

    def create(queue, attributes, erasure):
        assert not live
        live.add(queue.path)
        created.append(queue.path)

    def convert(table, name):
        assert live == {table.path}
        live.remove(table.path)
        result = (stress.StaticTable(table.base_path, name) if isinstance(table, stress.Queue)
                  else stress.Queue(table.base_path, name, 1))
        live.add(result.path)
        conversions.append(result)
        return result

    def remove(table):
        live.remove(table.path)
        removed.append(table.path)

    monkeypatch.setattr(stress.Queue, "create", create)
    monkeypatch.setattr(stress.Queue, "alter_to_static", convert)
    monkeypatch.setattr(stress.StaticTable, "alter_to_queue", convert)
    monkeypatch.setattr(stress.StaticTable, "remove", remove)
    stress.test_queue_and_hunk_storage("//test", Spec(loop_spec), {}, args=None)

    assert len(created) == 2
    assert len(removed) == 1
    assert len(live) == 1
    assert any(isinstance(table, stress.Queue) for table in conversions)
    assert any(isinstance(table, stress.StaticTable) for table in conversions)
