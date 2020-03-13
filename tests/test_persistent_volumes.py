from yp.common import YtResponseError

import pytest

from .conftest import (
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
)


@pytest.mark.usefixtures("yp_env")
class TestPersistentVolumes(object):
    RBIND_DISK_SPEC = {
        "storage_class": "hdd",
        "rbind_policy": {
        }
    }

    MANAGED_DISK_SPEC = {
        "storage_class": "hdd",
        "managed_policy": {
            "total_capacity": 100
        }
    }

    MANAGED_VOLUME_SPEC = {
        "managed_policy": {
            "capacity": 100
        }
    }

    RBIND_VOLUME_SPEC = {
        "rbind_policy": {
            "mount_path": "/"
        }
    }

    def test_create_disk_spec_validation(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_disk")
        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_disk", attributes={"spec": {"storage_class": "hdd"}})
        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_disk", attributes={"spec": {"storage_class": "hdd", "rbind_policy": {}, "managed_policy": {}}})

        yp_client.create_object("persistent_disk", attributes={"spec": self.RBIND_DISK_SPEC})
        yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})

    def test_create_volume_spec_validation(self, yp_env):
        yp_client = yp_env.yp_client

        rbind_disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.RBIND_DISK_SPEC})
        managed_disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})

        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": rbind_disk_id}, "spec": self.MANAGED_VOLUME_SPEC})
        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": managed_disk_id}, "spec": self.RBIND_VOLUME_SPEC})

        yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": managed_disk_id}, "spec": self.MANAGED_VOLUME_SPEC})
        yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": rbind_disk_id}, "spec": self.RBIND_VOLUME_SPEC})

    def test_cannot_remove_disk_with_volumes(self, yp_env):
        yp_client = yp_env.yp_client

        disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})
        yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": disk_id}, "spec": self.MANAGED_VOLUME_SPEC})

        with pytest.raises(YtResponseError):
            yp_client.remove_object("persistent_disk", disk_id)


    def test_cannot_create_existing_volume_claim_for_nonexisting_volume(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("persistent_volume_claim", attributes={"spec": {"existing_volume_policy": {"volume_id": "nonexisting"}}})

    def test_existing_volume_claim_binds_to_volume(self, yp_env):
        yp_client = yp_env.yp_client

        disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})
        volume_id = yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": disk_id}, "spec": self.MANAGED_VOLUME_SPEC})
        claim_id = yp_client.create_object("persistent_volume_claim", attributes={"spec": {"existing_volume_policy": {"volume_id": volume_id}}})
        assert yp_client.get_object("persistent_volume_claim", claim_id, selectors=["/status/bound_volume_id"])[0] == volume_id

    def test_cannot_remove_bound_volume(self, yp_env):
        yp_client = yp_env.yp_client

        disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})
        volume_id = yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": disk_id}, "spec": self.MANAGED_VOLUME_SPEC})
        claim_id = yp_client.create_object("persistent_volume_claim", attributes={"spec": {"existing_volume_policy": {"volume_id": volume_id}}})

        with pytest.raises(YtResponseError):
            yp_client.remove_object("persistent_volume", volume_id)

        yp_client.remove_object("persistent_volume_claim", claim_id)
        yp_client.remove_object("persistent_volume", volume_id)

    def test_attach_detach_disk(self, yp_env):
        yp_client = yp_env.yp_client

        disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})

        node_ids = create_nodes(yp_client, 2)

        with pytest.raises(YtResponseError):
            yp_client.detach_persistent_disk(node_ids[0], disk_id)

        yp_client.attach_persistent_disk(node_ids[0], disk_id)

        with pytest.raises(YtResponseError):
            yp_client.detach_persistent_disk(node_ids[1], disk_id)

        with pytest.raises(YtResponseError):
            yp_client.attach_persistent_disk(node_ids[1], disk_id)

        yp_client.detach_persistent_disk(node_ids[0], disk_id)

        yp_client.attach_persistent_disk(node_ids[1], disk_id)


    def test_mount_volume_into_pod(self, yp_env):
        yp_client = yp_env.yp_client

        disk_id = yp_client.create_object("persistent_disk", attributes={"spec": self.MANAGED_DISK_SPEC})
        volume_id = yp_client.create_object("persistent_volume", attributes={"meta": {"disk_id": disk_id}, "spec": self.MANAGED_VOLUME_SPEC})
        claim_id = yp_client.create_object("persistent_volume_claim", attributes={"spec": {"existing_volume_policy": {"volume_id": volume_id}}})

        node_ids = create_nodes(yp_client, 1)
        node_id = node_ids[0]

        pod_set_id = create_pod_set(yp_client)

        LABELS = {"label_key": "label_value"}
        def create_pod():
            return create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": False,
                "node_id": node_id,
                "disk_volume_claims": [
                    {
                        "name": "my_volume1",
                        "claim_id": claim_id,
                        "labels": LABELS
                    }
                ]
            })

        with pytest.raises(YtResponseError):
            create_pod()

        yp_client.attach_persistent_disk(node_id, disk_id)

        pod_id = create_pod()

        mounts = yp_client.get_object("pod", pod_id, selectors=["/status/disk_volume_mounts"])[0]
        assert len(mounts) == 1
        assert mounts[0]["name"] == "my_volume1"
        assert mounts[0]["labels"] == LABELS
        assert mounts[0]["volume_id"] == volume_id
