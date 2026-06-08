from yt_queue_agent_test_base import TestQueueAgentBase, QueueAgentOrchid, GenericObjectPath

from yt_commands import (
    authors,
    insert_rows,
    select_rows,
    delete_rows,
    set,
    remove,
    sync_unmount_table,
    raises_yt_error,
    assert_yt_error,
    wait,
)

from yt.common import YtError

##################################################################


class TestMultiConsumerController(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("panesher")
    def test_consistent_orchid_and_table(self):
        path = self.create_consumer_path("multi_consumer")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        orchid = QueueAgentOrchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))
        assert orchid.get_queue_consumer_names() == []
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

        # Check controller will insert to multi_consumer_names table
        names = ["my_1", "my_2", "my_3"]
        insert_rows(path, [
            {
                "queue_consumer_name": name,
                "queue_cluster": "primary",
                "queue_path": "//tmp/any_queue",
                "partition_index": 0,
                "offset": 0,
            }
            for name in names
        ])
        orchid.wait_fresh_pass()

        assert sorted(orchid.get_queue_consumer_names()) == sorted(names)
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == sorted(
            [{"cluster": "primary", "path": path, "name": name, "queue_agent_stage": "production"} for name in names],
            key=lambda r: r["name"],
        )

        # Check controller deletes from multi_consumer_names table (stale row for unknown name)
        insert_rows(
            "//sys/queue_agents/multi_consumer_names",
            [{"cluster": "primary", "path": path, "name": "stale_name"}],
        )
        orchid.wait_fresh_pass()

        assert sorted(orchid.get_queue_consumer_names()) == sorted(names)
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == sorted(
            [{"cluster": "primary", "path": path, "name": name, "queue_agent_stage": "production"} for name in names],
            key=lambda r: r["name"],
        )

        # Delete all rows and check table is cleaned up
        for name in names:
            delete_rows(path, [{
                "queue_consumer_name": name,
                "queue_cluster": "primary",
                "queue_path": "//tmp/any_queue",
                "partition_index": 0,
            }])

        orchid.wait_fresh_pass()

        assert orchid.get_queue_consumer_names() == []
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

    @authors("panesher")
    def test_invalid_multi_consumer(self):
        path = self.create_consumer_path("invalid_multi_consumer")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        sync_unmount_table(path)

        orchid = QueueAgentOrchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        orchid.wait_fresh_pass()

        assert_yt_error(YtError.from_dict(orchid.get_status()["error"]), "is in \"unmounted\" state")
        wait(lambda: orchid.get_alerts().check_matching(
            "queue_agent_multi_consumer_controller_pass_failed",
            text="is in \"unmounted\" state",
        ), timeout=5, ignore_exceptions=True)

        # No rows should be written to multi_consumer_names table for an invalid consumer.
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

    @authors("panesher")
    def test_queue_agent_stage_change(self):
        path = self.create_consumer_path("consumer_stage")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="testing")

        self._wait_for_component_passes()

        orchid = QueueAgentOrchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []
        insert_rows(path, [{
            "queue_consumer_name": "my_1",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        with raises_yt_error("is not mapped to any queue agent"):
            orchid.wait_fresh_pass()

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

        set(f"{path}/@queue_agent_stage", "production")
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

    @authors("panesher")
    def test_banned_multi_consumer(self):
        path = self.create_consumer_path("consumer_banned")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        orchid = QueueAgentOrchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        insert_rows(path, [{
            "queue_consumer_name": "my_1",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        orchid.wait_fresh_pass()

        assert orchid.get_queue_consumer_names() == ["my_1"]
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

        # Ban the consumer — controller should stop syncing and report error.
        set(f"{path}/@queue_agent_banned", True)
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()

        status = orchid.get_status()
        assert_yt_error(YtError.from_dict(orchid.get_status()["error"]), "banned")

        # Insert a new name while banned — it should NOT appear in multi_consumer_names.
        insert_rows(path, [{
            "queue_consumer_name": "my_2",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        orchid.wait_fresh_pass()

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

        # Unban — controller resumes, my_2 should now appear.
        remove(f"{path}/@queue_agent_banned")
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()

        status = orchid.get_status()
        assert "error" not in status

        assert sorted(orchid.get_queue_consumer_names()) == ["my_1", "my_2"]
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"},
            {"cluster": "primary", "path": path, "name": "my_2", "queue_agent_stage": "production"},
        ]
