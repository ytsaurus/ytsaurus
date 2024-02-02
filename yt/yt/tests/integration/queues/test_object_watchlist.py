from yt_chaos_test_base import ChaosTestBase

from yt_env_setup import YTEnvSetup

from yt_commands import (authors, get, create, remove, start_transaction, commit_transaction, copy, alter_table,
                         raises_yt_error, set)

from yt.yson import YsonEntity

import builtins


class QueueAgentHelpers:
    @staticmethod
    def get_objects():
        return get("//sys/@queue_agent_object_revisions")

    @staticmethod
    def assert_registered_queues_are(*paths):
        queues = QueueAgentHelpers.get_objects()["queues"]
        assert queues.keys() == builtins.set(paths)
        for path in paths:
            assert queues[path] == get(path + "/@attribute_revision")

    @staticmethod
    def assert_registered_consumers_are(*paths):
        consumers = QueueAgentHelpers.get_objects()["consumers"]
        assert consumers.keys() == builtins.set(paths)
        for path in paths:
            assert consumers[path] == get(path + "/@attribute_revision")


class TestQueueAgentObjectRevisions(ChaosTestBase, YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    @authors("nadya73")
    def test_attribute_opaqueness(self):
        full_attributes = get("//sys/@")
        assert full_attributes["queue_agent_object_revisions"] == YsonEntity()

    @authors("nadya73")
    def test_create(self):
        create("table", "//tmp/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/q2", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/q3", attributes={"dynamic": False, "schema": [{"name": "data", "type": "string"}]})

        # Possible to create replicated queues.
        create("replicated_table", "//tmp/rep_q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        replica_id = create("table_replica",
                            "//tmp/rep_q1-rep1",
                            attributes={"table_path": "//tmp/rep_q1",
                                        "cluster_name": "primary",
                                        "replica_path": "//tmp/rep_q1-rep1",
                                        "schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/rep_q1-rep1", attributes={"dynamic": True, "upstream_replica_id": replica_id, "schema": [{"name": "data", "type": "string"}]})

        # Possible to create chaos replicated queues.
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table",
               "//tmp/chaos_rep_q1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": [{"name": "data", "type": "string"}]})

        # Object without schema is not a queue or consumer.
        create("chaos_replicated_table",
               "//tmp/chaos_rep_q2",
               attributes={"chaos_cell_bundle": "chaos_bundle"})
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/q2", "//tmp/rep_q1", "//tmp/rep_q1-rep1", "//tmp/chaos_rep_q1")
        QueueAgentHelpers.assert_registered_consumers_are()

        consumer_schema = [{"name": "data", "type": "string", "sort_order": "ascending"},
                           {"name": "test", "type": "string"}]
        create("table",
               "//tmp/c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        create("table",
               "//tmp/c2",
               attributes={"dynamic": True,
                           "schema": consumer_schema})
        with raises_yt_error('Builtin attribute "treat_as_queue_consumer" cannot be set'):
            create("table",
                   "//tmp/c3",
                   attributes={"dynamic": True,
                               "schema": [{"name": "data", "type": "string"},
                                          {"name": "test", "type": "string"}],
                               "treat_as_queue_consumer": True})
        with raises_yt_error('Builtin attribute "treat_as_queue_consumer" cannot be set'):
            create("table",
                   "//tmp/c4",
                   attributes={"dynamic": False,
                               "schema": consumer_schema,
                               "treat_as_queue_consumer": True})

        # Possible to create replicated consumers.
        create("replicated_table",
               "//tmp/rep_c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        with raises_yt_error('Builtin attribute "treat_as_queue_consumer" cannot be set'):
            create("replicated_table",
                   "//tmp/rep_c2",
                   attributes={"dynamic": True,
                               "schema": [{"name": "data", "type": "string"},
                                          {"name": "test", "type": "string"}],
                               "treat_as_queue_consumer": True})
        with raises_yt_error('Either "schema" or "schema_id" must be specified for dynamic tables'):
            create("replicated_table",
                   "//tmp/rep_c3",
                   attributes={"dynamic": True})

        # Possible to create chaos replicated consumers.
        create("chaos_replicated_table",
               "//tmp/chaos_rep_c1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        with raises_yt_error('Builtin attribute "treat_as_queue_consumer" cannot be set'):
            create("chaos_replicated_table",
                   "//tmp/chaos_rep_c2",
                   attributes={"chaos_cell_bundle": "chaos_bundle",
                               "treat_as_queue_consumer": True})
        with raises_yt_error('Builtin attribute "treat_as_queue_consumer" cannot be set'):
            create("chaos_replicated_table",
                   "//tmp/chaos_rep_c2",
                   attributes={"chaos_cell_bundle": "chaos_bundle",
                               "schema": [{"name": "data", "type": "string"},
                                          {"name": "test", "type": "string"}],
                               "treat_as_queue_consumer": True})
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1", "//tmp/chaos_rep_c1")

        remove("//tmp/q1")
        remove("//tmp/q2")
        remove("//tmp/rep_q1")
        remove("//tmp/rep_q1-rep1")
        remove("//tmp/chaos_rep_q1")
        QueueAgentHelpers.assert_registered_queues_are()

        remove("//tmp/c1")
        remove("//tmp/c2")
        remove("//tmp/rep_c1")
        remove("//tmp/chaos_rep_c1")
        QueueAgentHelpers.assert_registered_consumers_are()

    @authors("nadya73")
    def test_transactional_create(self):
        # TODO: add tests for chaos under transactions, when create chaos table will be supported under transactions.
        tx1 = start_transaction()
        create("table", "//tmp/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx1)
        create("table",
               "//tmp/q2",
               attributes={"dynamic": False, "schema": [{"name": "data", "type": "string"}]}, tx=tx1)
        consumer_schema = [{"name": "data", "type": "string", "sort_order": "ascending"},
                           {"name": "test", "type": "string"}]
        create("table",
               "//tmp/c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True},
               tx=tx1)
        create("table",
               "//tmp/c2",
               attributes={"dynamic": True,
                           "schema": consumer_schema},
               tx=tx1)

        create("replicated_table", "//tmp/rep_q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx1)
        create("replicated_table",
               "//tmp/rep_c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True},
               tx=tx1)

        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

        commit_transaction(tx1)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/rep_q1")
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1")

        tx2 = start_transaction()
        remove("//tmp/q1", tx=tx2)
        remove("//tmp/q2", tx=tx2)
        remove("//tmp/c1", tx=tx2)
        remove("//tmp/c2", tx=tx2)
        remove("//tmp/rep_q1", tx=tx2)
        remove("//tmp/rep_c1", tx=tx2)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/rep_q1")
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1")

        commit_transaction(tx2)
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

    @authors("nadya73")
    def test_alter(self):
        create("table", "//tmp/q1", attributes={"dynamic": False, "schema": [{"name": "data", "type": "string"}]})
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table",
               "//tmp/chaos_rep_q1",
               attributes={"chaos_cell_bundle": "chaos_bundle"})

        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

        alter_table("//tmp/q1", dynamic=True)
        alter_table("//tmp/chaos_rep_q1", schema=[{"name": "data", "type": "string"}])
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/chaos_rep_q1")
        QueueAgentHelpers.assert_registered_consumers_are()

        alter_table("//tmp/q1", schema=[{"name": "data", "type": "string"}, {"name": "kek", "type": "string"}])
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/chaos_rep_q1")
        QueueAgentHelpers.assert_registered_consumers_are()

        alter_table("//tmp/q1", dynamic=False)
        alter_table("//tmp/chaos_rep_q1", schema=[])
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

        remove("//tmp/q1")
        remove("//tmp/chaos_rep_q1")
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

        create("chaos_replicated_table",
               "//tmp/chaos_rep_c1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": [{"name": "data", "type": "string", "sort_order": "ascending"},
                                      {"name": "test", "type": "string"}],
                           "treat_as_queue_consumer": True})
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/chaos_rep_c1")

        # Impossible to alter chaos consumer to queue via changing schema.
        with raises_yt_error("Chaos replicated table object cannot be both a queue and a consumer."):
            alter_table("//tmp/chaos_rep_c1", schema=[{"name": "data", "type": "string"}])
        # Impossible to alter chaos consumer with empty schema.
        with raises_yt_error("Chaos replicated table object cannot be both a queue and a consumer."):
            alter_table("//tmp/chaos_rep_c1", schema=[])
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/chaos_rep_c1")

        set("//tmp/chaos_rep_c1/@treat_as_queue_consumer", False)
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

        alter_table("//tmp/chaos_rep_c1", schema=[{"name": "data", "type": "string"}])
        QueueAgentHelpers.assert_registered_queues_are("//tmp/chaos_rep_c1")
        QueueAgentHelpers.assert_registered_consumers_are()

        remove("//tmp/chaos_rep_c1")
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

    @authors("nadya73")
    def test_copy(self):
        create("table", "//tmp/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("replicated_table", "//tmp/rep_q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table",
               "//tmp/chaos_rep_q1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": [{"name": "data", "type": "string"}]})
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/rep_q1", "//tmp/chaos_rep_q1")

        consumer_schema = [{"name": "data", "type": "string", "sort_order": "ascending"},
                           {"name": "test", "type": "string"}]
        create("table",
               "//tmp/c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        create("replicated_table",
               "//tmp/rep_c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        create("chaos_replicated_table",
               "//tmp/chaos_rep_c1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": consumer_schema,
                           "treat_as_queue_consumer": True})
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1", "//tmp/chaos_rep_c1")

        for queue_name_prefix in ("q", "rep_q", "chaos_rep_q"):
            copy(f"//tmp/{queue_name_prefix}1", f"//tmp/{queue_name_prefix}2")
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/q2", "//tmp/rep_q1", "//tmp/rep_q2", "//tmp/chaos_rep_q1", "//tmp/chaos_rep_q2")

        # Copying doesn't carry over the treat_as_queue_consumer flag.
        for consumer_name_prefix in ("c", "rep_c", "chaos_rep_c"):
            copy(f"//tmp/{consumer_name_prefix}1", f"//tmp/{consumer_name_prefix}2")
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1", "//tmp/chaos_rep_c1")

        for object_name_prefix in ("", "rep_", "chaos_rep_"):
            for index in (1, 2):
                remove(f"//tmp/{object_name_prefix}q{index}")
                remove(f"//tmp/{object_name_prefix}c{index}")
        QueueAgentHelpers.assert_registered_queues_are()
        QueueAgentHelpers.assert_registered_consumers_are()

    @authors("nadya73")
    def test_transactional_copy(self):
        # TODO: add tests for chaos under transactions, when copy chaos table will be supported under transactions.
        create("table", "//tmp/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("replicated_table", "//tmp/rep_q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/rep_q1")

        tx = start_transaction()
        copy("//tmp/q1", "//tmp/q2", tx=tx)
        copy("//tmp/rep_q1", "//tmp/rep_q2", tx=tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/rep_q1")

        commit_transaction(tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/q2", "//tmp/rep_q1", "//tmp/rep_q2")

        for queue_name_prefix in ("q", "rep_q"):
            for index in (1, 2):
                remove(f"//tmp/{queue_name_prefix}{index}")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("nadya73")
    def test_transactional_fun(self):
        # TODO: add tests for chaos under transactions, when copy && create chaos table will be supported under transactions.
        tx = start_transaction()
        create("table", "//tmp/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx)
        create("replicated_table", "//tmp/rep_q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx)

        QueueAgentHelpers.assert_registered_queues_are()

        for queue_name_prefix in ("q", "rep_q"):
            copy(f"//tmp/{queue_name_prefix}1", f"//tmp/{queue_name_prefix}2", tx=tx)
            copy(f"//tmp/{queue_name_prefix}2", f"//tmp/{queue_name_prefix}3", tx=tx)
        QueueAgentHelpers.assert_registered_queues_are()

        remove("//tmp/q2", tx=tx)
        remove("//tmp/rep_q2", tx=tx)

        commit_transaction(tx)
        QueueAgentHelpers.assert_registered_queues_are("//tmp/q1", "//tmp/q3", "//tmp/rep_q1", "//tmp/rep_q3")

        for queue_name_prefix in ("q", "rep_q"):
            for index in (1, 3):
                remove(f"//tmp/{queue_name_prefix}{index}")
        QueueAgentHelpers.assert_registered_queues_are()

    @authors("nadya73")
    def test_treat_as_queue_consumer_modifications(self):
        consumer_schema = [{"name": "data", "type": "string", "sort_order": "ascending"},
                           {"name": "test", "type": "string"}]
        create("table",
               "//tmp/c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema})
        create("replicated_table",
               "//tmp/rep_c1",
               attributes={"dynamic": True,
                           "schema": consumer_schema})
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table",
               "//tmp/chaos_rep_c1",
               attributes={"chaos_cell_bundle": "chaos_bundle",
                           "schema": consumer_schema})
        QueueAgentHelpers.assert_registered_consumers_are()

        for consumer_name in ("c1", "rep_c1", "chaos_rep_c1"):
            set(f"//tmp/{consumer_name}/@treat_as_queue_consumer", True)
        QueueAgentHelpers.assert_registered_consumers_are("//tmp/c1", "//tmp/rep_c1", "//tmp/chaos_rep_c1")

        for consumer_name in ("c1", "rep_c1", "chaos_rep_c1"):
            set(f"//tmp/{consumer_name}/@treat_as_queue_consumer", False)
        QueueAgentHelpers.assert_registered_consumers_are()

        tx = start_transaction()
        for consumer_name in ("c1", "rep_c1", "chaos_rep_c1"):
            with raises_yt_error("Operation cannot be performed in transaction"):
                set(f"//tmp/{consumer_name}/@treat_as_queue_consumer", True, tx=tx)


class TestQueueAgentObjectsRevisionsPortal(TestQueueAgentObjectRevisions):
    NUM_SECONDARY_MASTER_CELLS = 2
    ENABLE_TMP_PORTAL = True

    @authors("nadya73")
    def test_objects_from_different_cells(self):
        create("portal_entrance", "//portals/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//portals/p2", attributes={"exit_cell_tag": 12})

        create("table", "//portals/p1/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        create("table", "//portals/p2/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        # Should live on the native cell.
        create("table", "//portals/q1", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})

        QueueAgentHelpers.assert_registered_queues_are("//portals/p1/q1", "//portals/p2/q1", "//portals/q1")
