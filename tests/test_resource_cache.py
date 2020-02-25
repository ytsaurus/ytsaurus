from yt.yson import YsonEntity, YsonUint64

import pytest


@pytest.mark.usefixtures("yp_env")
class TestResourceCache(object):
    def test_resource_cache(self, yp_env):
        yp_client = yp_env.yp_client

        cached_resources = [
            {"id": "my_layer", "layer": {"id": "my_layer"}},
            {"id": "my_static_resource", "static_resource": {"id": "my_static_resource"}},
        ]

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        resource_cache_id = yp_client.create_object(
            object_type="resource_cache",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {"revision": 42, "cached_resources": cached_resources},
            },
        )

        result = yp_client.get_object(
            "resource_cache", resource_cache_id, selectors=["/meta", "/spec"]
        )
        assert result[0]["id"] == resource_cache_id
        assert result[0]["pod_set_id"] == pod_set_id
        assert result[1]["revision"] == 42
        assert result[1]["cached_resources"] == cached_resources

        status = {
            "revision": 42,
            "all_in_progress": {"pod_count": 31,},
            "all_ready": {"pod_count": 1,},
            "latest_in_progress": {"pod_count": 21,},
            "latest_ready": {"pod_count": 11,},
            "cached_resource_status": [
                {
                    "id": "my_layer",
                    "revisions": [
                        {
                            "revision": 41,
                            "in_progress": {"pod_count": 77},
                            "ready": {"pod_count": 229},
                            "layer": {"id": "my_layer"},
                        }
                    ],
                },
                {
                    "id": "my_static_resource",
                    "revisions": [
                        {
                            "revision": 45,
                            "in_progress": {"pod_count": 771},
                            "ready": {"pod_count": 2291},
                            "static_resource": {"id": "my_static_resource",},
                        }
                    ],
                },
            ],
        }

        yp_client.update_object(
            "resource_cache", resource_cache_id, set_updates=[{"path": "/status", "value": status}]
        )

        result = yp_client.get_object("resource_cache", resource_cache_id, selectors=["/status"])[0]
        assert result == status

    def test_remove_pod_set(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        resource_cache_id = yp_client.create_object(
            object_type="resource_cache", attributes={"meta": {"pod_set_id": pod_set_id}}
        )

        assert (
            yp_client.get_object("resource_cache", resource_cache_id, selectors=["/meta/id"])[0]
            == resource_cache_id
        )
        yp_client.remove_object("pod_set", pod_set_id)
        assert yp_client.select_objects("pod_set", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("resource_cache", selectors=["/meta/id"]) == []

    def test_pod_with_resource_cache(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        pod_id = yp_client.create_object(
            object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id,}, "spec": {},}
        )

        resource_cache_spec = {
            "spec": {
                "layers": [{"revision": 1, "layer": {"id": "my_layer"}}],
                "static_resources": [{"revision": 1, "resource": {"id": "my_static_resource"}}],
            }
        }

        result = yp_client.get_object("pod", pod_id, selectors=["/meta", "/spec/resource_cache"])
        assert result[0]["id"] == pod_id
        assert result[1] == {}

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[{"path": "/spec/resource_cache", "value": resource_cache_spec},],
        )

        result = yp_client.get_object("pod", pod_id, selectors=["/meta", "/spec/resource_cache"])
        assert result[0]["id"] == pod_id
        assert result[1] == resource_cache_spec
