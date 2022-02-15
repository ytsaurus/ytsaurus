from yt_env_setup import YTEnvSetup

from yt_commands import (authors, get, create, remove, start_transaction, commit_transaction, copy, alter_table)

from yt.yson import YsonEntity


class QueueAgentHelpers:
    @staticmethod
    def get_objects():
        return get("//sys/@queue_agent_object_revisions")

    @staticmethod
    def assert_registered_queues_are(*paths):
        queues = QueueAgentHelpers.get_objects()["queues"]
        assert queues.keys() == set(paths)
        for path in paths:
            assert queues[path] == get(path + "/@revision")


class TestQueueAgentObjectRevisions(YTEnvSetup):
    USE_DYNAMIC_TABLES = 1

    @authors("achulkov2")
    def test_attribute_opaqueness(self):
        full_attributes = get("//sys/@")
        assert full_attributes["queue_agent_object_revisions"] == YsonEntity()

    @authors("achulkov2")
    def test_create(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/qq", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]},
               recursive=True)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q", "//tmp/qq")

        remove("//tmp/q")
        remove("//tmp/qq")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("achulkov2")
    def test_transactional_create(self):
        tx1 = start_transaction()
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx1)
        QueueAgentHelpers.assert_registered_queues_are()

        commit_transaction(tx1)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        tx2 = start_transaction()
        remove("//tmp/q", tx=tx2)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        commit_transaction(tx2)
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("achulkov2")
    def test_alter(self):
        create("table", "//tmp/q", attributes={"dynamic": False, "schema": [{"name": "data", "type": "string"}]})
        QueueAgentHelpers.assert_registered_queues_are()

        alter_table("//tmp/q", dynamic=True)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        alter_table("//tmp/q", schema=[{"name": "data", "type": "string"}, {"name": "kek", "type": "string"}])
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        alter_table("//tmp/q", dynamic=False)
        QueueAgentHelpers.assert_registered_queues_are()

        remove("//tmp/q")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("achulkov2")
    def test_copy(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        copy("//tmp/q", "//tmp/qq")
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q", "//tmp/qq")

        remove("//tmp/q")
        remove("//tmp/qq")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("achulkov2")
    def test_transactional_copy(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        tx = start_transaction()
        copy("//tmp/q", "//tmp/qq", tx=tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q")

        commit_transaction(tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q", "//tmp/qq")

        remove("//tmp/q")
        remove("//tmp/qq")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("achulkov2")
    def test_transactional_fun(self):
        tx = start_transaction()
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx)
        QueueAgentHelpers.assert_registered_queues_are()

        copy("//tmp/q", "//tmp/qq", tx=tx)
        copy("//tmp/qq", "//tmp/qqq", tx=tx)
        QueueAgentHelpers.assert_registered_queues_are()

        remove("//tmp/qq", tx=tx)

        commit_transaction(tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q", "//tmp/qqq")

        remove("//tmp/q")
        remove("//tmp/qqq")
        QueueAgentHelpers.assert_registered_queues_are()


class TestQueueAgentObjectsRevisionsPortal(TestQueueAgentObjectRevisions):
    NUM_SECONDARY_MASTER_CELLS = 2
    ENABLE_TMP_PORTAL = True

    @authors("achulkov2")
    def test_objects_from_different_cells(self):
        create("portal_entrance", "//portals/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//portals/p2", attributes={"exit_cell_tag": 2})

        create("table", "//portals/p1/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("table", "//portals/p2/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        # Should live on the native cell.
        create("table", "//portals/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})

        QueueAgentHelpers.assert_registered_queues_are("//portals/p1/q", "//portals/p2/q", "//portals/q")
