from .conftest import authors

from yt.wrapper.driver import get_api_version

import yt.environment.init_queue_agent_state as init_queue_agent_state

import yt.wrapper as yt

from .helpers import TEST_DIR, wait

from yt.ypath import parse_ypath

import pytest

CONSUMER_REGISTRATIONS = "//sys/queue_agents/consumer_registrations"


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestQueueCommands(object):
    def setup_method(self):
        self._sync_create_tablet_cell()

        if not yt.exists("//sys/queue_agents"):
            yt.create("map_node", "//sys/queue_agents")
            self._create_dynamic_table(CONSUMER_REGISTRATIONS, schema=init_queue_agent_state.REGISTRATION_TABLE_SCHEMA)

        yt.mount_table(CONSUMER_REGISTRATIONS, sync=True)

    def _sync_create_tablet_cell(self):
        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        return cell_id

    def _create_dynamic_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
            ]})
        attributes.update({"dynamic": True})
        yt.create("table", path, attributes=attributes)

    def check_single_registration(self, queue, consumer, vital, partitions=None):
        assert list(yt.select_rows("* from [//sys/queue_agents/consumer_registrations]")) == [
            {
                "queue_cluster": "primary", "queue_path": queue,
                "consumer_cluster": "primary", "consumer_path": consumer,
                "vital": vital,
                "partitions": partitions,
            }
        ]

        expected_registration = {
            "queue_path": parse_ypath("primary:" + queue),
            "consumer_path": parse_ypath("primary:" + consumer),
            "vital": vital,
            "partitions": partitions,
        }

        by_queue_and_consumer = yt.list_queue_consumer_registrations(queue_path=queue, consumer_path=consumer)
        by_queue = yt.list_queue_consumer_registrations(queue_path=queue)
        by_consumer = yt.list_queue_consumer_registrations(consumer_path=consumer)
        all_registrations = yt.list_queue_consumer_registrations()

        assert all_registrations == by_consumer == by_queue == by_queue_and_consumer == [expected_registration]

    def check_empty_registrations(self, queue, consumer):
        assert list(yt.select_rows("* from [//sys/queue_agents/consumer_registrations]")) == []

        by_queue_and_consumer = yt.list_queue_consumer_registrations(queue_path=queue, consumer_path=consumer)
        by_queue = yt.list_queue_consumer_registrations(queue_path=queue)
        by_consumer = yt.list_queue_consumer_registrations(consumer_path=consumer)
        all_registrations = yt.list_queue_consumer_registrations()

        assert all_registrations == by_consumer == by_queue == by_queue_and_consumer == []

    DEFAULT_QUEUE_SCHEMA = [
        {"name": "data", "type": "string"}
    ]

    @staticmethod
    def _create_queue(path, schema=None, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": schema if schema is not None else TestQueueCommands.DEFAULT_QUEUE_SCHEMA,
        }
        attributes.update(kwargs)
        yt.create("table", path, attributes=attributes)
        yt.mount_table(path, sync=True)

    @staticmethod
    def _create_consumer(path, **kwargs):
        yt.create("queue_consumer", path=path, attributes=kwargs)

    @staticmethod
    def _create_producer(path, **kwargs):
        yt.create("queue_producer", path=path, attributes=kwargs)

    @authors("achulkov2")
    # This is a very basic test, just to check that there are no bugs in the client api implementation.
    # Tests for actual logic can be found in tests/integration/queues.
    def test_register_queue_consumer(self):
        if get_api_version() != "v4":
            pytest.skip()

        queue = TEST_DIR + "/q"
        self._create_dynamic_table(queue)

        consumer = TEST_DIR + "/c"
        self._create_dynamic_table(consumer, schema=init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA)

        yt.register_queue_consumer(queue, consumer, vital=True)
        self.check_single_registration(queue, consumer, vital=True)

        yt.unregister_queue_consumer(queue, consumer)
        self.check_empty_registrations(queue, consumer)

        yt.register_queue_consumer(queue, consumer, vital=False, partitions=[1, 5, 4, 3])
        self.check_single_registration(queue, consumer, vital=False, partitions=[1, 5, 4, 3])

        yt.unregister_queue_consumer(queue, consumer)
        self.check_empty_registrations(queue, consumer)

    @authors("achulkov2")
    def test_pull_queue(self):
        if get_api_version() != "v4":
            pytest.skip()

        queue = TEST_DIR + "/q"

        self._create_queue(queue)

        yt.insert_rows(queue, [{"data": "foo"}])
        yt.insert_rows(queue, [{"data": "bar"}])

        assert list(yt.pull_queue(queue, offset=1, partition_index=0)) == [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ]

    @authors("achulkov2")
    def test_pull_consumer(self):
        if get_api_version() != "v4":
            pytest.skip()

        queue = TEST_DIR + "/q"
        consumer = TEST_DIR + "/c"

        self._create_queue(queue)
        self._create_consumer(consumer)

        yt.insert_rows(queue, [{"data": "foo"}])
        yt.insert_rows(queue, [{"data": "bar"}])

        yt.register_queue_consumer(queue, consumer, vital=False)

        assert list(yt.pull_consumer(consumer, queue, offset=1, partition_index=0)) == [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ]

    @authors("achulkov2")
    def test_advance_consumer(self):
        if get_api_version() != "v4":
            pytest.skip()

        consumer = TEST_DIR + "/c"
        queue = TEST_DIR + "/q"

        self._create_consumer(consumer)

        def get_offset(queue, partition_index=0):
            rows = list(yt.select_rows(
                "* from [{}] where [queue_path] = \"{}\" and [partition_index] = {}"
                .format(consumer, queue, partition_index)))
            assert len(rows) <= 1
            if rows:
                return rows[0]["offset"]
            return None

        assert get_offset(queue) is None

        yt.advance_consumer(consumer, queue, partition_index=0, old_offset=0, new_offset=3)
        assert get_offset(queue) == 3

    @authors("nadya73")
    def test_queue_producer_commands(self):
        if get_api_version() != "v4":
            pytest.skip()

        producer = TEST_DIR + "/p"
        queue = TEST_DIR + "/q"
        session_id = "session_1"

        self._create_queue(queue)
        self._create_producer(producer)

        session_info = yt.create_queue_producer_session(producer, queue, session_id)
        assert session_info["epoch"] == 0
        assert session_info["sequence_number"] == -1

        push_result = yt.push_queue_producer(producer, queue, session_id, 0, [{"data": "foo", "$sequence_number": 0}])
        assert push_result["last_sequence_number"] == 0
        assert push_result["skipped_row_count"] == 0

        push_result = yt.push_queue_producer(producer, queue, session_id, 0, [{"data": "foobar", "$sequence_number": 0}, {"data": "bar", "$sequence_number": 2}])
        assert push_result["last_sequence_number"] == 2
        assert push_result["skipped_row_count"] == 1

        assert list(yt.select_rows(f"data from [{queue}]")) == [{"data": "foo"}, {"data": "bar"}]

        with yt.Transaction(type="tablet"):
            push_result = yt.push_queue_producer(producer, queue, session_id, 0, [{"data": "foo", "$sequence_number": 2}, {"data": "baz", "$sequence_number": 3}])
            assert push_result["last_sequence_number"] == 3
            assert push_result["skipped_row_count"] == 1

            # It's not committed yet.
            assert list(yt.select_rows(f"data from [{queue}]")) == [{"data": "foo"}, {"data": "bar"}]

        assert list(yt.select_rows(f"data from [{queue}]")) == [
            {"data": "foo"},
            {"data": "bar"},
            {"data": "baz"},
        ]

        session_info = yt.create_queue_producer_session(producer, queue, session_id)
        assert session_info["epoch"] == 1
        assert session_info["sequence_number"] == 3

        with pytest.raises(yt.YtResponseError):
            yt.push_queue_producer(producer, queue, session_id, 0, [{"data": "abc", "$sequence_number": 4}])

        assert list(yt.select_rows(f"data from [{queue}]")) == [
            {"data": "foo"},
            {"data": "bar"},
            {"data": "baz"},
        ]

        push_result = yt.push_queue_producer(producer, queue, session_id, 1, [{"data": "abc", "$sequence_number": 4}])
        assert push_result["last_sequence_number"] == 4
        assert push_result["skipped_row_count"] == 0

        assert list(yt.select_rows(f"data from [{queue}]")) == [
            {"data": "foo"},
            {"data": "bar"},
            {"data": "baz"},
            {"data": "abc"},
        ]

        yt.remove_queue_producer_session(producer, queue, session_id)

        with pytest.raises(yt.YtResponseError, match="Unknown queue producer session"):
            yt.push_queue_producer(producer, queue, session_id, 1, [{"data": "abc", "$sequence_number": 4}])
