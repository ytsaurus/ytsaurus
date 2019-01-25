from yt.yson import YsonEntity, YsonUint64

import pytest


@pytest.mark.usefixtures("yp_env")
class TestResourceCache(object):
    def test_resource_cache(self, yp_env):
        yp_client = yp_env.yp_client

        cached_resources = [
            {
                "id": "my_layer",
                "layer": {
                    "id": "my_layer"
                }
            },
            {
                "id": "my_static_resource",
                "static_resource": {
                    "id": "my_static_resource"
                }
            }
        ]

        replica_set_id = yp_client.create_object(object_type="replica_set", attributes={})
        resource_cache_id = yp_client.create_object(
            object_type="resource_cache",
            attributes={
                "meta": {
                    "replica_set_id": replica_set_id
                },
                "spec": {
                    "revision": 42,
                    "cached_resources": cached_resources
                }
            }
        )

        result = yp_client.get_object("resource_cache", resource_cache_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == resource_cache_id
        assert result[0]["replica_set_id"] == replica_set_id
        assert result[1]["revision"] == 42
        assert result[1]["cached_resources"] == cached_resources

        status = {
            "revision": 42,
            "all_in_progress": {
                "pod_count": 31,
            },
            "all_ready": {
                "pod_count": 1,
            },
            "latest_in_progress": {
                "pod_count": 21,
            },
            "latest_ready": {
                "pod_count": 11,
            },
            "cached_resource_status": [
                {
                    "id": "my_layer",
                    "revisions": [
                        {
                            "revision": 41,
                            "in_progress": {
                                "pod_count": 77
                            },
                            "ready": {
                                "pod_count": 229
                            },
                            "layer": {
                                "id": "my_layer"
                            }
                        }
                    ]
                },
                {
                    "id": "my_static_resource",
                    "revisions": [
                        {
                            "revision": 45,
                            "in_progress": {
                                "pod_count": 771
                            },
                            "ready": {
                                "pod_count": 2291
                            },
                            "static_resource": {
                                "id": "my_static_resource",
                            }
                        }
                    ]
                }
            ]
        }

        yp_client.update_object("resource_cache", resource_cache_id, set_updates=[{"path": "/status", "value": status}])

        result = yp_client.get_object("resource_cache", resource_cache_id, selectors=["/status"])[0]
        assert result == status

    def test_remove_replica_set(self, yp_env):
        yp_client = yp_env.yp_client

        replica_set_id = yp_client.create_object(object_type="replica_set", attributes={})
        resource_cache_id = yp_client.create_object(object_type="resource_cache",
            attributes={
                "meta": {
                    "replica_set_id": replica_set_id
                }
            }
        )

        assert yp_client.get_object("resource_cache", resource_cache_id, selectors=["/meta/id"])[0] == resource_cache_id
        yp_client.remove_object("replica_set", replica_set_id)
        assert yp_client.select_objects("replica_set", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("resource_cache", selectors=["/meta/id"]) == []
