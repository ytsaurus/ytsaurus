from yt.yson import YsonEntity, YsonUint64

from datetime import timedelta
import pytest


@pytest.mark.usefixtures("yp_env")
class TestHorizontalPodAutoscaler(object):
    def test_horizontal_pod_autoscaler(self, yp_env):
        yp_client = yp_env.yp_client

        replica_set_autoscale = {
            "min_replicas": 1,
            "max_replicas": 3,
            "cpu": {"lower_bound": 0.25, "upper_bound": 0.5,},
            "upscale_delay": {"seconds": 900,},
            "downscale_delay": {"seconds": 900,},
            "check_extensible_field": {"random_key_1243": "value",},
        }

        account_id = yp_client.create_object("account")
        replica_set_id = yp_client.create_object(
            object_type="replica_set", attributes={"spec": {"account_id": account_id,}}
        )
        horizontal_pod_autoscaler_id = yp_client.create_object(
            object_type="horizontal_pod_autoscaler",
            attributes={
                "meta": {"replica_set_id": replica_set_id},
                "spec": {"replica_set": replica_set_autoscale},
            },
        )

        result = yp_client.get_object(
            "horizontal_pod_autoscaler", horizontal_pod_autoscaler_id, selectors=["/meta", "/spec"]
        )
        assert result[0]["id"] == horizontal_pod_autoscaler_id
        assert result[0]["replica_set_id"] == replica_set_id
        assert result[1]["replica_set"] == replica_set_autoscale

        status = {
            "current_replicas": 1,
            "desired_replicas": 2,
            "metric_value": 0.5,
            "last_upscale_time": {"seconds": 1, "nanos": 2,},
            "last_downscale_time": {"seconds": 3, "nanos": 4,},
            "check_extensible_field": {"random_key_1243": "value",},
        }

        yp_client.update_object(
            "horizontal_pod_autoscaler",
            horizontal_pod_autoscaler_id,
            set_updates=[{"path": "/status", "value": status}],
        )

        result = yp_client.get_object(
            "horizontal_pod_autoscaler", horizontal_pod_autoscaler_id, selectors=["/status"]
        )[0]
        assert result == status

    def test_remove_replica_set(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        replica_set_id = yp_client.create_object(
            object_type="replica_set", attributes={"spec": {"account_id": account_id,}}
        )
        horizontal_pod_autoscaler_id = yp_client.create_object(
            object_type="horizontal_pod_autoscaler",
            attributes={"meta": {"replica_set_id": replica_set_id}},
        )

        assert (
            yp_client.get_object(
                "horizontal_pod_autoscaler", horizontal_pod_autoscaler_id, selectors=["/meta/id"]
            )[0]
            == horizontal_pod_autoscaler_id
        )
        yp_client.remove_object("replica_set", replica_set_id)
        assert yp_client.select_objects("replica_set", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("horizontal_pod_autoscaler", selectors=["/meta/id"]) == []
