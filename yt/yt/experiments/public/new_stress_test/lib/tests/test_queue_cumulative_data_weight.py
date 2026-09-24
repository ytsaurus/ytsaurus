from lib import test_queue_and_hunk_storage as stress
from lib.spec import Spec, spec_template

import pytest

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


def _write_rows(client, monkeypatch, queue, rows):
    tablets = iter(row[0] for row in rows)
    monkeypatch.setattr(stress.random, "choice", lambda choices: next(tablets))
    monkeypatch.setattr(stress.RSG, "generate", Mock(side_effect=[
        value for _, key, payload in rows for value in (key, payload)
    ]))
    client.get_tablet_infos.side_effect = lambda path, indexes: {
        "tablets": [{"total_row_count": queue.written_row_count[index]} for index in indexes],
    }
    spec = SimpleNamespace(queue_and_hunk_storage=SimpleNamespace(
        write_min_batch_size=len(rows), write_max_batch_size=len(rows),
        write_min_row_size=1, write_max_row_size=1, write_insert_chunk_bytes=1,
    ))
    queue.write(True, spec, retry_count=1)


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


@pytest.mark.parametrize("operation", ["copy", "move"])
def test_copy_and_move_preserve_weight_and_trim_state(client, monkeypatch, operation):
    queue = stress.Queue("//test", "queue", 2)
    queue.mount()
    queue.written_row_count = [2, 1]
    queue.trimmed_row_counts = [2, 0]
    queue.cumulative_data_weights = [1046, 10]
    result = getattr(queue, operation)("result")
    assert result.trimmed_row_counts == [2, 0]
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


def test_stress_loop_trims_and_adds_replicas_after_writes(client, monkeypatch):
    raw_spec = copy.deepcopy(spec_template)
    cfg = raw_spec["queue_and_hunk_storage"]
    for key in cfg:
        if key.endswith("_probability"):
            cfg[key] = 0
    cfg.update({
        "create_replicated_probability": 1, "replicated_min_replicas": 1,
        "replicated_max_replicas": 2, "write_probability": 1,
        "trim_probability": 1, "add_replica_probability": 1,
    })
    raw_spec["size"]["iterations"] = 2
    client.get.side_effect = lambda path, **kwargs: (
        "local-test" if path == "//sys/@cluster_name" else {"external_cell_tag": 11})
    monkeypatch.setattr(stress.random, "choice", lambda choices: choices[0])
    monkeypatch.setattr(stress.random, "randint", lambda lower, upper: lower)
    queues = {}

    def write(queue, **kwargs):
        queues[queue.path] = queue
        queue.written_row_count[0] += 1
        queue.cumulative_data_weights[0] += 11

    def get_tablet_infos(path, indexes):
        queue = queues[path.split(".replica_")[0]]
        return {"tablets": [{"total_row_count": queue.written_row_count[i]} for i in indexes]}

    client.get_tablet_infos.side_effect = get_tablet_infos
    monkeypatch.setattr(stress.Queue, "write", write)
    stress.test_queue_and_hunk_storage("//test", Spec(raw_spec), {}, args=None)
    assert len(queues) == 3
    for queue in queues.values():
        assert queue.cumulative_data_weights == [44]
        assert queue.trimmed_row_counts == [4]
        assert len(queue.replicas) == 2
        assert all(replica["trimmed_row_counts"] == [4] for replica in queue.replicas)
