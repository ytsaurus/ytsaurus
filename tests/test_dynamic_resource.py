import pytest


@pytest.mark.usefixtures("yp_env")
class TestDynamicResource(object):
    def test_dynamic_resource(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        dynamic_resource_id = yp_client.create_object(
            object_type="dynamic_resource",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {"revision": 42, "deploy_groups": [{"urls": ["nonexistent://"]}]},
                "status": {
                    "revisions": [
                        {
                            "revision": 1,
                            "ready": {"pod_count": 2, "condition": {"status": "true"}},
                            "error": {"pod_count": 2, "condition": {"status": "false"}},
                        },
                        {
                            "revision": 4,
                            "ready": {"pod_count": 5, "condition": {"status": "true"}},
                            "error": {"pod_count": 6, "condition": {"status": "false"}},
                        },
                    ]
                },
            },
        )

        result = yp_client.get_object(
            "dynamic_resource", dynamic_resource_id, selectors=["/meta", "/spec", "/status"]
        )
        assert result[0]["id"] == dynamic_resource_id
        assert result[0]["pod_set_id"] == pod_set_id
        assert result[1]["revision"] == 42
        assert result[1]["deploy_groups"][0]["urls"] == ["nonexistent://"]
        assert result[1].get("verification") is None
        assert len(result[2]["revisions"]) == 2

        spec = {
            "revision": 43,
            "deploy_groups": [{"urls": ["something://"]}],
        }

        yp_client.update_object(
            "dynamic_resource", dynamic_resource_id, set_updates=[{"path": "/spec", "value": spec}]
        )

        result = yp_client.get_object("dynamic_resource", dynamic_resource_id, selectors=["/spec"])[
            0
        ]
        assert result == spec

        spec = {
            "revision": 44,
            "deploy_groups": [
                {
                    "urls": ["something://"],
                    "storage_options": {
                        "verification": {"checksum": "123", "check_period_ms": 1000},
                    },
                }
            ],
        }

        yp_client.update_object(
            "dynamic_resource", dynamic_resource_id, set_updates=[{"path": "/spec", "value": spec}]
        )

        result = yp_client.get_object("dynamic_resource", dynamic_resource_id, selectors=["/spec"])[
            0
        ]
        assert result == spec

    def test_remove_pod_set(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        dynamic_resource_id1 = yp_client.create_object(
            object_type="dynamic_resource", attributes={"meta": {"pod_set_id": pod_set_id}}
        )
        dynamic_resource_id2 = yp_client.create_object(
            object_type="dynamic_resource", attributes={"meta": {"pod_set_id": pod_set_id}}
        )

        assert dynamic_resource_id1 != dynamic_resource_id2
        assert (
            yp_client.get_object("dynamic_resource", dynamic_resource_id1, selectors=["/meta/id"])[
                0
            ]
            == dynamic_resource_id1
        )
        assert (
            yp_client.get_object("dynamic_resource", dynamic_resource_id2, selectors=["/meta/id"])[
                0
            ]
            == dynamic_resource_id2
        )
        yp_client.remove_object("pod_set", pod_set_id)
        assert yp_client.select_objects("pod_set", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("dynamic_resource", selectors=["/meta/id"]) == []

    def test_pod_with_resource(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        dynamic_resource_id = yp_client.create_object(
            object_type="dynamic_resource", attributes={"meta": {"pod_set_id": pod_set_id}}
        )

        assert (
            yp_client.get_object("dynamic_resource", dynamic_resource_id, selectors=["/meta/id"])[0]
            == dynamic_resource_id
        )

        spec = [
            {"id": dynamic_resource_id, "revision": 2, "mark": "mymark"},
        ]
        status = [
            {
                "id": dynamic_resource_id,
                "revision": 3,
                "ready": {"status": "false"},
                "in_progress": {"status": "true"},
                "error": {"status": "true", "reason": "oops!", "message": "Absolute fail"},
                "mark": "mymark",
            }
        ]
        pod_id = yp_client.create_object(
            object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}, "spec": {}}
        )

        result = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/meta", "/spec/dynamic_resources", "/status/dynamic_resources"],
        )
        assert result[0]["id"] == pod_id
        assert result[1] == []
        assert result[2] == []

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                {"path": "/spec/dynamic_resources", "value": spec},
                {"path": "/status/dynamic_resources", "value": status},
            ],
        )
        result = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/meta", "/spec/dynamic_resources", "/status/dynamic_resources"],
        )
        assert result[0]["id"] == pod_id
        assert result[1][0]["id"] == dynamic_resource_id
        assert result[2][0] == status[0]

    def test_pod_with_direct_resource_attribute_setup(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set", attributes={})
        dynamic_resource_id = yp_client.create_object(
            object_type="dynamic_resource", attributes={"meta": {"pod_set_id": pod_set_id}}
        )

        assert (
            yp_client.get_object("dynamic_resource", dynamic_resource_id, selectors=["/meta/id"])[0]
            == dynamic_resource_id
        )

        spec = [
            {"id": dynamic_resource_id, "revision": 2, "mark": "mymark"},
        ]
        status = [
            {
                "id": dynamic_resource_id,
                "revision": 3,
                "ready": {"status": "false"},
                "in_progress": {"status": "true"},
                "error": {"status": "true", "reason": "oops!", "message": "Absolute fail"},
                "mark": "mymark",
            }
        ]

        pod_id = yp_client.create_object(
            object_type="pod",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {"dynamic_resources": spec},
                "status": {"dynamic_resources": status},
            },
        )

        result = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/meta", "/spec/dynamic_resources", "/status/dynamic_resources"],
        )
        assert result[0]["id"] == pod_id
        assert result[1][0]["id"] == dynamic_resource_id
        assert result[2][0] == status[0]
