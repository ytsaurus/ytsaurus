import pytest

from yt.yt_sync.core.model.replica import YtReplica
from yt.yt_sync.core.model.types import Types

from .helpers import YtReplicaTestBase


class TestYtReplica(YtReplicaTestBase):
    def test_key(self, replica_id: str, table_path: str):
        replica = YtReplica(
            replica_id=replica_id,
            cluster_name="remote0",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.SYNC,
        )
        assert ("remote0", table_path) == replica.key

    def test_make_minimal(self, replica_id: str, table_path: str, replica_attributes: Types.Attributes):
        replica = YtReplica.make(replica_id, replica_attributes)
        assert replica_id == replica.replica_id
        assert "remote0" == replica.cluster_name
        assert table_path == replica.replica_path
        assert replica.enabled is True
        assert YtReplica.Mode.SYNC == replica.mode
        assert not replica.content_type
        assert not replica.attributes
        assert replica.exists is True
        assert replica.is_temporary is False

    def test_make_full(self, replica_id: str, table_path: str, full_replica_attributes: Types.Attributes):
        replica = YtReplica.make(replica_id, full_replica_attributes)
        assert replica_id == replica.replica_id
        assert "remote0" == replica.cluster_name
        assert table_path == replica.replica_path
        assert replica.enabled is True
        assert YtReplica.Mode.SYNC == replica.mode
        assert YtReplica.ContentType.DATA == replica.content_type
        assert {"my_attr": 1} == replica.attributes

    def test_eq(self, replica_id: str, full_replica_attributes: Types.Attributes):
        replica1 = YtReplica.make(replica_id, full_replica_attributes)
        replica2 = YtReplica.make(replica_id, full_replica_attributes)
        replica2.replica_id = None
        replica2.attributes["my_custom_attr"] = "vvv"
        assert replica1 == replica2
        assert replica1.compare(replica2, True)

    def test_not_eq(self, replica_id: str, full_replica_attributes: Types.Attributes):
        replica1 = YtReplica.make(replica_id, full_replica_attributes)
        replica2 = YtReplica.make(replica_id, full_replica_attributes)
        replica2.mode = "async"
        assert replica1 != replica2
        assert not replica1.compare(replica2, True)
        assert replica1.compare(replica2, False)

    def test_eq_for_chaos_queue_without_rtt(self, replica_id: str, full_replica_attributes: Types.Attributes):
        full_replica_attributes["content_type"] = YtReplica.ContentType.QUEUE
        full_replica_attributes["enable_replicated_table_tracker"] = False
        replica1 = YtReplica.make(replica_id, full_replica_attributes)
        replica2 = YtReplica.make(replica_id, full_replica_attributes)
        replica2.mode = "async"
        assert replica1 != replica2

    def test_eq_for_chaos_queue_with_rtt(self, replica_id: str, full_replica_attributes: Types.Attributes):
        full_replica_attributes["content_type"] = YtReplica.ContentType.QUEUE
        replica1 = YtReplica.make(replica_id, full_replica_attributes)
        replica2 = YtReplica.make(replica_id, full_replica_attributes)
        replica2.mode = "async"
        assert replica1 == replica2

    def test_yt_attributes(self, replica_id: str, table_path: str, full_replica_attributes: Types.Attributes):
        yt_attributes = YtReplica.make(replica_id, full_replica_attributes).yt_attributes
        expected = {
            "my_attr": 1,
            "cluster_name": "remote0",
            "replica_path": table_path,
            "mode": "sync",
            "enable_replicated_table_tracker": True,
            "enabled": True,
            "content_type": "data",
        }
        assert expected == yt_attributes

    def test_make_no_cluster(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes.pop("cluster_name")
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_make_no_path(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes.pop("replica_path")
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_make_no_state(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes.pop("state")
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_make_no_mode(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes.pop("mode")
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_make_bad_mode(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes["mode"] = "unknown"
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_make_bad_content_type(self, replica_id: str, replica_attributes: Types.Attributes):
        replica_attributes["content_type"] = "unknown"
        with pytest.raises(AssertionError):
            YtReplica.make(replica_id, replica_attributes)

    def test_changed_attributes(self, replica_id: str, replica_attributes: Types.Attributes):
        replica1 = YtReplica.make(replica_id, replica_attributes)
        replica1.attributes["attr1"] = 1
        replica1.attributes["attr2"] = 1

        replica2 = YtReplica.make(replica_id, replica_attributes)
        replica2.attributes["attr1"] = 2
        replica2.attributes["attr3"] = 2

        expected_diff = {"attr1": (1, 2), "attr2": (1, None)}
        actual_diff = dict()
        for path, desired, actual in replica1.changed_attributes(replica2):
            actual_diff[path] = (desired, actual)
        assert expected_diff == actual_diff

    @pytest.mark.parametrize("bool_state", [True, False])
    def test_update_from_attributes(self, replica_id: str, replica_attributes: Types.Attributes, bool_state: bool):
        replica = YtReplica.make(replica_id, replica_attributes)

        patch = {
            "mode": YtReplica.Mode.ASYNC,
            "enable_replicated_table_tracker": False,
            "content_type": YtReplica.ContentType.DATA,
            "my_attr": "my_value",
        }
        if bool_state:
            patch["enabled"] = False
        else:
            patch["state"] = YtReplica.State.DISABLED

        replica.update_from_attributes(patch)
        assert replica.enabled is False
        assert YtReplica.Mode.ASYNC == replica.mode
        assert replica.enable_replicated_table_tracker is False
        assert YtReplica.ContentType.DATA == replica.content_type
        assert "my_value" == replica.attributes.get("my_attr")

    def test_update_from_attributes_bad_state(self, replica_id: str, replica_attributes: Types.Attributes):
        replica = YtReplica.make(replica_id, replica_attributes)
        with pytest.raises(AssertionError):
            replica.update_from_attributes({"state": "_unknown_"})

    def test_update_from_attributes_bad_mode(self, replica_id: str, replica_attributes: Types.Attributes):
        replica = YtReplica.make(replica_id, replica_attributes)
        with pytest.raises(AssertionError):
            replica.update_from_attributes({"mode": "_unknown_"})

    def test_update_from_attributes_bad_content_type(self, replica_id: str, replica_attributes: Types.Attributes):
        replica = YtReplica.make(replica_id, replica_attributes)
        with pytest.raises(AssertionError):
            replica.update_from_attributes({"content_type": "_unknown_"})
