from yt_queue_agent_test_base import TestQueueAgentBase

from yt_commands import (
    authors, create, create_queue_producer_session, get, insert_rows, remove,
    remove_queue_producer_session, select_rows, wait_for_tablet_state,
    push_queue_producer, raises_yt_error, start_transaction, commit_transaction)

from yt_type_helpers import normalize_schema, make_schema

import yt.environment.init_queue_agent_state as init_queue_agent_state

import yt_error_codes

import builtins

##################################################################


class TestCreateQueueProducer(TestQueueAgentBase):
    @authors("apachee")
    def test_create_producer(self):
        create("queue_producer", "//tmp/p")

        assert get("//tmp/p/@type") == "table"
        assert get("//tmp/p/@dynamic")

        expected_schema = make_schema(
            init_queue_agent_state.PRODUCER_OBJECT_TABLE_SCHEMA,
            strict=True,
            unique_keys=True,
        )
        assert normalize_schema(get("//tmp/p/@schema")) == expected_schema

        # Check that producer is mounted automatically.
        wait_for_tablet_state("//tmp/p", "mounted")


class TestCreateRemoveForQueueProducerSessions(TestQueueAgentBase):
    @authors("apachee")
    def test_basic(self):
        def check_producer_table(expected_key_columns, expected_value_columns):
            rows = select_rows("* from [//tmp/p]")
            assert len(rows) == 1
            row = rows[0]
            for key, value in expected_key_columns.items():
                assert row[key] == value
            for key, value in expected_value_columns.items():
                assert row[key] == value

        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        user_meta = {
            "foo": [1, 2, 3],
            "bar": {
                "foo": "bar",
                "baz": True
            }
        }

        expected_key_columns = {
            "queue_cluster": "primary",
            "queue_path": "//tmp/q",
            "session_id": "test",
        }

        expected_value_columns = {
            "sequence_number": -1,
            "epoch": 0,
            "user_meta": user_meta,
        }

        resp = create_queue_producer_session("//tmp/p", "//tmp/q", "test", user_meta)
        for key, value in expected_value_columns.items():
            assert resp[key] == value
        check_producer_table(expected_key_columns, expected_value_columns)

        # Simulate non-zero state for producer session.
        insert_rows("//tmp/p", [{
            "queue_cluster": "primary",
            "queue_path": "//tmp/q",
            "session_id": "test",
            "sequence_number": 100,
            "epoch": 200,
        }], update=True)

        user_meta = [1, 2, 3]
        expected_value_columns = {
            "sequence_number": 100,
            "epoch": 201,
            "user_meta": user_meta,
        }

        resp = create_queue_producer_session("//tmp/p", "//tmp/q", "test", user_meta)
        for key, value in expected_value_columns.items():
            assert resp[key] == value
        check_producer_table(expected_key_columns, expected_value_columns)

        # Check behavior without user_meta set.
        expected_value_columns["epoch"] += 1
        resp = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        for key, value in expected_value_columns.items():
            assert resp[key] == value
        check_producer_table(expected_key_columns, expected_value_columns)

        remove_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert len(select_rows("* from [//tmp/p]")) == 0

        # After removal of the session, epochs should start from 0.
        expected_value_columns = {
            "sequence_number": -1,
            "epoch": 0,
            "user_meta": None,
        }

        resp = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        for key, value in expected_value_columns.items():
            assert resp[key] == value
        check_producer_table(expected_key_columns, expected_value_columns)

        remove("//tmp/q")
        remove("//tmp/p")

    @authors("apachee")
    def test_multiple_sessions(self):
        self._create_queue("//tmp/q1")
        self._create_queue("//tmp/q2")
        self._create_producer("//tmp/p")

        def create_session(queue, id, user_meta=None):
            return create_queue_producer_session("//tmp/p", queue, id, user_meta)

        sessions = [
            ("//tmp/q1", "s1"),
            ("//tmp/q2", "s1"),
            ("//tmp/q1", "s3"),
        ]
        session_user_metas = {}
        for session in sessions:
            session_user_metas[session] = f"Session: {session}"

        for id in sessions:
            queue, session_name = id
            resp = create_session(queue, session_name, session_user_metas[id])
            assert resp["sequence_number"] == -1
            assert resp["epoch"] == 0
            assert resp["user_meta"] == session_user_metas[id]

        def recreate_and_check(id, epoch):
            resp = create_session(*id)
            assert resp["sequence_number"] == -1
            assert resp["user_meta"] == session_user_metas[id]
            assert resp["epoch"] == epoch

        recreate_and_check(sessions[1], 1)
        recreate_and_check(sessions[1], 2)
        recreate_and_check(sessions[1], 3)
        recreate_and_check(sessions[2], 1)
        recreate_and_check(sessions[2], 2)
        recreate_and_check(sessions[0], 1)

        session_epochs = {
            sessions[0]: 1,
            sessions[1]: 3,
            sessions[2]: 2,
        }

        rows = select_rows("* from [//tmp/p]")
        assert len(rows) == 3
        for row in rows:
            assert row["queue_cluster"] == "primary"
            id = (row["queue_path"], row["session_id"])
            assert row["sequence_number"] == -1
            assert row["user_meta"] == session_user_metas[id]
            assert row["epoch"] == session_epochs[id]

        while len(sessions) > 0:
            session = sessions[-1]
            queue, id = session
            sessions.pop(-1)
            remove_queue_producer_session("//tmp/p", queue, id)
            assert builtins.set((row["queue_path"], row["session_id"]) for row in select_rows("queue_path, session_id from [//tmp/p]")) == builtins.set(sessions)

        remove("//tmp/q1")
        remove("//tmp/q2")
        remove("//tmp/p")


class TestProducerApi(TestQueueAgentBase):
    def _check_session(self, producer_path, session_id, sequence_number, epoch, user_meta=None):
        rows = select_rows(f"* from [{producer_path}] where [session_id] = '{session_id}'")
        assert len(rows) == 1
        assert rows[0]["session_id"] == session_id
        assert rows[0]["sequence_number"] == sequence_number
        assert rows[0]["epoch"] == epoch
        if user_meta is not None:
            assert rows[0]["user_meta"] == user_meta

    def _check_queue(self, queue_path, row_count):
        rows = select_rows(f"* from [{queue_path}]")
        assert len(rows) == row_count
        for row_index, row in enumerate(rows):
            assert row["data"] == f"row{row_index + 1}"

    @authors("nadya73")
    def test_push_queue_producer_with_start_sequence_number(self):
        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["sequence_number"] == -1
        assert session["epoch"] == 0

        with raises_yt_error(code=yt_error_codes.InvalidRowSequenceNumbers):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row2"}], epoch=0)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1"}], epoch=0, sequence_number=0)
        assert push_result["last_sequence_number"] == 0
        assert push_result["skipped_row_count"] == 0

        self._check_session("//tmp/p", "test", 0, 0)
        self._check_queue("//tmp/q", 1)

        with raises_yt_error(code=yt_error_codes.InvalidEpoch):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row2"}], epoch=1, sequence_number=1)

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["sequence_number"] == 0
        assert session["epoch"] == 1

        with raises_yt_error(code=yt_error_codes.ZombieEpoch):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row2"}], epoch=0, sequence_number=1)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1"}, {"data": "row2"}], epoch=1, sequence_number=0)
        assert push_result["last_sequence_number"] == 1
        assert push_result["skipped_row_count"] == 1

        self._check_session("//tmp/p", "test", 1, 1)
        self._check_queue("//tmp/q", 2)

    @authors("nadya73")
    def test_push_queue_producer_with_explicit_sequence_numbers(self):
        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["sequence_number"] == -1
        assert session["epoch"] == 0

        with raises_yt_error("Error parsing sequence number from row"):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1", "$sequence_number": "0"}], epoch=0)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1", "$sequence_number": 0}], epoch=0)
        assert push_result["last_sequence_number"] == 0
        assert push_result["skipped_row_count"] == 0
        self._check_session("//tmp/p", "test", 0, 0)
        self._check_queue("//tmp/q", 1)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[
            {"data": "row2", "$sequence_number": 6},
            {"data": "row3", "$sequence_number": 10},
        ], epoch=0)
        assert push_result["last_sequence_number"] == 10
        assert push_result["skipped_row_count"] == 0
        self._check_session("//tmp/p", "test", 10, 0)
        self._check_queue("//tmp/q", 3)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[
            {"data": "row2", "$sequence_number": 6},
            {"data": "row3", "$sequence_number": 10},
            {"data": "row4", "$sequence_number": 14},
        ], epoch=0)
        assert push_result["last_sequence_number"] == 14
        assert push_result["skipped_row_count"] == 2
        self._check_session("//tmp/p", "test", 14, 0)
        self._check_queue("//tmp/q", 4)

        with raises_yt_error(code=yt_error_codes.InvalidRowSequenceNumbers):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row5", "$sequence_number": 17}], epoch=0, sequence_number=17)
        self._check_session("//tmp/p", "test", 14, 0)
        self._check_queue("//tmp/q", 4)

        with raises_yt_error(code=yt_error_codes.InvalidRowSequenceNumbers):
            push_queue_producer("//tmp/p", "//tmp/q", "test", data=[
                {"data": "row5", "$sequence_number": 17},
                {"data": "row6"},
            ], epoch=0)
        self._check_session("//tmp/p", "test", 14, 0)
        self._check_queue("//tmp/q", 4)

        push_queue_producer("//tmp/p", "//tmp/q", "test", data=[
            {"data": "row5"},
        ], epoch=0, sequence_number=17)
        self._check_session("//tmp/p", "test", 17, 0)
        self._check_queue("//tmp/q", 5)

    @authors("nadya73")
    def test_push_queue_producer_twice_in_transaction(self):
        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["epoch"] == 0
        assert session["sequence_number"] == -1

        tx = start_transaction(type="tablet")

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1", "$sequence_number": 0}], epoch=0, tx=tx)

        assert push_result["last_sequence_number"] == 0
        assert push_result["skipped_row_count"] == 0

        # Transaction was not committed.
        self._check_session("//tmp/p", "test", -1, 0)
        self._check_queue("//tmp/q", 0)

        push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row2"}], epoch=0, sequence_number=5, tx=tx)

        commit_transaction(tx)

        self._check_session("//tmp/p", "test", 5, 0)
        self._check_queue("//tmp/q", 2)

    @authors("nadya73")
    def test_concurrent_sessions(self):
        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        session1 = create_queue_producer_session("//tmp/p", "//tmp/q", "test1")
        assert session1["epoch"] == 0
        assert session1["sequence_number"] == -1

        session2 = create_queue_producer_session("//tmp/p", "//tmp/q", "test2")
        assert session2["epoch"] == 0
        assert session2["sequence_number"] == -1

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        push_result1 = push_queue_producer("//tmp/p", "//tmp/q", "test1", data=[
            {"data": "row1", "$sequence_number": 0},
        ], epoch=0, tx=tx1)
        assert push_result1["last_sequence_number"] == 0

        push_result2 = push_queue_producer("//tmp/p", "//tmp/q", "test2", data=[
            {"data": "row2", "$sequence_number": 0},
        ], epoch=0, tx=tx2)
        assert push_result2["last_sequence_number"] == 0

        commit_transaction(tx1)
        commit_transaction(tx2)

        self._check_session("//tmp/p", "test1", 0, 0)
        self._check_session("//tmp/p", "test2", 0, 0)
        self._check_queue("//tmp/q", 2)

    @authors("nadya73")
    def test_two_sessions_one_transaction(self):
        self._create_queue("//tmp/q")
        self._create_producer("//tmp/p")

        session1 = create_queue_producer_session("//tmp/p", "//tmp/q", "test1")
        assert session1["epoch"] == 0
        assert session1["sequence_number"] == -1

        session2 = create_queue_producer_session("//tmp/p", "//tmp/q", "test2")
        assert session2["epoch"] == 0
        assert session2["sequence_number"] == -1

        tx = start_transaction(type="tablet")
        push_result1 = push_queue_producer("//tmp/p", "//tmp/q", "test1", data=[
            {"data": "row1", "$sequence_number": 3},
        ], epoch=0, tx=tx)
        assert push_result1["last_sequence_number"] == 3

        push_result2 = push_queue_producer("//tmp/p", "//tmp/q", "test2", data=[
            {"data": "row2", "$sequence_number": 1},
        ], epoch=0, tx=tx)
        assert push_result2["last_sequence_number"] == 1

        commit_transaction(tx)

        self._check_session("//tmp/p", "test1", 3, 0)
        self._check_session("//tmp/p", "test2", 1, 0)
        self._check_queue("//tmp/q", 2)

    @authors("nadya73")
    def test_several_tablets(self):
        self._create_queue("//tmp/q", partition_count=2)
        self._create_producer("//tmp/p")

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["epoch"] == 0

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[
            {"data": "row1", "$tablet_index": 0, "$sequence_number": 0},
            {"data": "row2", "$tablet_index": 1, "$sequence_number": 2},
        ], epoch=0)

        assert push_result["last_sequence_number"] == 2
        assert push_result["skipped_row_count"] == 0

        self._check_session("//tmp/p", "test", 2, 0)
        self._check_queue("//tmp/q", 2)

        rows = select_rows("* from [//tmp/q] where [$tablet_index] = 0")
        assert len(rows) == 1
        assert rows[0]["data"] == "row1"

        rows = select_rows("* from [//tmp/q] where [$tablet_index] = 1")
        assert len(rows) == 1
        assert rows[0]["data"] == "row2"

    @authors("nadya73")
    def test_user_meta(self):
        self._create_queue("//tmp/q", partition_count=2)
        self._create_producer("//tmp/p")

        user_meta = {
            "filename": "file1",
        }

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test", user_meta)
        assert session["user_meta"] == user_meta
        self._check_session("//tmp/p", "test", -1, 0, user_meta=user_meta)

        push_result = push_queue_producer("//tmp/p", "//tmp/q", "test", data=[{"data": "row1"}], epoch=0, sequence_number=0)
        assert push_result["last_sequence_number"] == 0
        assert push_result["skipped_row_count"] == 0

        self._check_session("//tmp/p", "test", 0, 0, user_meta=user_meta)

        # Update usermeta.
        user_meta["filename"] = "file2"

        push_result = push_queue_producer(
            "//tmp/p", "//tmp/q", "test", data=[{"data": "row1"}],
            epoch=0, sequence_number=1, user_meta=user_meta)
        assert push_result["last_sequence_number"] == 1
        assert push_result["skipped_row_count"] == 0

        self._check_session("//tmp/p", "test", 1, 0, user_meta=user_meta)

        session = create_queue_producer_session("//tmp/p", "//tmp/q", "test")
        assert session["user_meta"] == user_meta
