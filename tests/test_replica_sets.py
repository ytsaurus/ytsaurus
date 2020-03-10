from . import templates

from yt.yson.yson_types import YsonEntity

import pytest


@pytest.mark.usefixtures("yp_env")
class TestReplicaSets(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "replica_set", account_is_mandatory=True)

    def test_update_spec(self, yp_env):
        account_id = yp_env.yp_client.create_object("account")
        templates.update_spec_test_template(
            yp_env.yp_client,
            "replica_set",
            {"account_id": account_id, "revision_id": "1"},
            "/spec/revision_id",
            "2",
        )

    def test_update_spec_pod_disks_validation(self, yp_env):
        yp_client = yp_env.yp_client
        replica_set_id = yp_client.create_object(
            object_type="replica_set", attributes=dict(spec=dict(account_id="tmp")),
        )
        templates.update_spec_pod_disks_validation_test_template(
            yp_client, "replica_set", replica_set_id,
        )

    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={
                "spec": {
                    "account_id": account_id,
                    "revision_id": "42",
                    "replica_count": 32,
                    "deployment_strategy": {
                        "min_available": 21,
                        "max_unavailable": 11,
                        "max_surge": 13,
                    },
                },
            },
        )

        result = yp_client.get_object("replica_set", rs_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == rs_id
        assert result[1]["revision_id"] == "42"
        assert result[1]["replica_count"] == 32
        assert result[1]["deployment_strategy"]["min_available"] == 21
        assert result[1]["deployment_strategy"]["max_unavailable"] == 11
        assert result[1]["deployment_strategy"]["max_surge"] == 13

        status = {
            "in_progress": {"pod_count": 31},
            "ready": {"pod_count": 1},
            "revisions": {
                "123456": {
                    "revision_id": "123456",
                    "in_progress": {
                        "pod_count": 2,
                        "condition": {
                            "status": "FAILED",
                            "reason": "not_implemented",
                            "message": "Not implemented",
                        },
                    },
                },
            },
        }

        yp_client.update_object(
            "replica_set", rs_id, set_updates=[{"path": "/status", "value": status}]
        )

        result = yp_client.get_object("replica_set", rs_id, selectors=["/status"])[0]
        assert result == status

    def test_extensible_spec(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={"spec": {"replica_count": 1, "account_id": account_id}},
        )

        yp_client.update_object(
            "replica_set", rs_id, set_updates=[{"path": "/spec/hello", "value": "world"}]
        )
        assert yp_client.get_object("replica_set", rs_id, selectors=["/spec/hello"])[0] == "world"

        assert yp_client.select_objects(
            "replica_set", selectors=["/spec/replica_count", "/spec/hello", "/spec/another_unknown"]
        ) == [[1, "world", YsonEntity()]]

        yp_client.update_object("replica_set", rs_id, remove_updates=[{"path": "/spec/hello"}])
        assert (
            yp_client.get_object("replica_set", rs_id, selectors=["/spec/hello"])[0] == YsonEntity()
        )

        yp_client.update_object(
            "replica_set",
            rs_id,
            set_updates=[
                {
                    "path": "/spec/pod_template_spec/labels",
                    "value": {"key": "value"},
                    "recursive": True,
                }
            ],
        )
        assert (
            yp_client.get_object(
                "replica_set", rs_id, selectors=["/spec/pod_template_spec/labels/key"]
            )[0]
            == "value"
        )

        yp_client.update_object(
            "replica_set", rs_id, remove_updates=[{"path": "/spec/pod_template_spec/labels"}]
        )
        assert (
            yp_client.get_object(
                "replica_set", rs_id, selectors=["/spec/pod_template_spec/labels/key"]
            )[0]
            == YsonEntity()
        )

    def test_extensible_status(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={
                "spec": {"account_id": account_id},
                "status": {"in_progress": {"pod_count": 3}},
            },
        )

        yp_client.update_object(
            "replica_set", rs_id, set_updates=[{"path": "/status/hello", "value": "world"}]
        )
        assert yp_client.get_object("replica_set", rs_id, selectors=["/status/hello"])[0] == "world"

        assert yp_client.select_objects(
            "replica_set",
            selectors=["/status/in_progress", "/status/hello", "/status/another_unknown"],
        ) == [[{"pod_count": 3}, "world", YsonEntity()]]

        yp_client.update_object("replica_set", rs_id, remove_updates=[{"path": "/status/hello"}])
        assert (
            yp_client.get_object("replica_set", rs_id, selectors=["/status/hello"])[0]
            == YsonEntity()
        )

    def test_network_project_permissions(self, yp_env):
        templates.replica_set_network_project_permissions_test_template(yp_env, "replica_set")

    def test_custom_pods_labels_annotations(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={"spec": {"replica_count": 1, "account_id": account_id}},
        )

        yp_client.update_object(
            "replica_set",
            rs_id,
            set_updates=[
                {
                    "path": "/spec/pod_template_spec/labels",
                    "value": {"label_key": "label_value"},
                    "recursive": True,
                },
                {
                    "path": "/spec/pod_template_spec/annotations",
                    "value": {"annotation_key": "annotation_value"},
                    "recursive": True,
                },
            ],
        )

        assert yp_client.get_object(
            "replica_set", rs_id, selectors=["/spec/pod_template_spec/labels"]
        ) == [{"label_key": "label_value"}]
        assert yp_client.get_object(
            "replica_set", rs_id, selectors=["/spec/pod_template_spec/annotations"]
        ) == [{"annotation_key": "annotation_value"}]

    def test_node_segment_id_update(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        segment_id_1 = yp_client.create_object(
            "node_segment", attributes={"spec": {"node_filter": ""}}
        )
        segment_id_2 = yp_client.create_object(
            "node_segment", attributes={"spec": {"node_filter": ""}}
        )
        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={
                "spec": {
                    "replica_count": 1,
                    "account_id": account_id,
                    "node_segment_id": segment_id_1,
                }
            },
        )
        yp_client.update_object(
            "replica_set",
            rs_id,
            set_updates=[{"path": "/spec/node_segment", "value": segment_id_2}],
        )
