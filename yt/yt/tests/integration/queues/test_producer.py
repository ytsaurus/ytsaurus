from yt_queue_agent_test_base import TestQueueAgentBase

from yt_commands import (authors, create, create_queue_producer_session, get, insert_rows, remove, remove_queue_producer_session,
                         select_rows, wait_for_tablet_state)

from yt_type_helpers import normalize_schema, make_schema

import yt.environment.init_queue_agent_state as init_queue_agent_state

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
            "sequence_number": 0,
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
            "sequence_number": 0,
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
            assert resp["sequence_number"] == 0
            assert resp["epoch"] == 0
            assert resp["user_meta"] == session_user_metas[id]

        def recreate_and_check(id, epoch):
            resp = create_session(*id)
            assert resp["sequence_number"] == 0
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
            assert row["sequence_number"] == 0
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
