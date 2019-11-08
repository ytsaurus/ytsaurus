from .conftest import (
    DEFAULT_POD_SET_SPEC,
    assert_over_time,
    create_nodes,
    create_pod_set_with_quota,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
    is_error_pod_scheduling_status,
)

from yp.common import wait

from yt.yson import YsonEntity

import pytest
import time

@pytest.mark.usefixtures("yp_env")
class TestHistoryApi(object):
    def test_filters(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = yp_client.create_object(object_type="stage", attributes={"spec": {"revision": 42}})
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 123}])
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 456}])
        yp_client.remove_object("stage", stage_id)

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]
        assert len(history_events) == 4
        assert history_events[0]["event_type"] == 1 and history_events[0]["results"][0]["value"]["revision"] == 42
        assert history_events[1]["event_type"] == 3 and history_events[1]["results"][0]["value"]["revision"] == 123
        assert history_events[2]["event_type"] == 3 and history_events[2]["results"][0]["value"]["revision"] == 456
        assert history_events[3]["event_type"] == 2 and isinstance(history_events[3]["results"][0]["value"], YsonEntity)

        selection_result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"interval": (history_events[1]["time"], history_events[3]["time"])})["events"]
        assert selection_result == history_events[1:3]

        selection_result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"interval": (history_events[1]["time"], None)})["events"]
        assert selection_result == history_events[1:]

        selection_result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"interval": (None, history_events[3]["time"])})["events"]
        assert selection_result == history_events[:3]

        selection_result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"limit": 2})["events"]
        assert selection_result == history_events[:2]

    def test_continuation_token(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = yp_client.create_object(object_type="stage", attributes={"spec": {"revision": 42}})
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 123}])
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 456}])
        yp_client.remove_object("stage", stage_id)

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]

        result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"limit": 1})
        continuation_token = result["continuation_token"]
        assert result["events"] == [history_events[0]]

        result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"limit": 2, "continuation_token": continuation_token})
        continuation_token = result["continuation_token"]
        assert result["events"] == history_events[1:3]

        result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"limit": 42, "continuation_token": continuation_token})
        continuation_token = result["continuation_token"]
        assert result["events"] == [history_events[3]]

        result = yp_client.select_object_history("stage", stage_id, ["/spec"], {"limit": 42, "continuation_token": continuation_token})
        assert result["events"] == []

    def test_selectors(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = yp_client.create_object(object_type="stage", attributes={"spec": {"revision": 42}})
        history_events = yp_client.select_object_history("stage", stage_id, ["/spec", "/spec/revision", "/meta", "/spec/abc", "/spec/abc", "/spec/revision/abc"])["events"]
        assert len(history_events) == 1
        results = history_events[0]["results"]
        assert len(results) == 6
        # TODO(gritukan) fix for accounts
        #assert results[0]["value"]["revision"] == {"account": YsonEntity, "revision": 42}
        assert results[1]["value"] == 42
        for i in range(2, 6):
            assert isinstance(results[i]["value"], YsonEntity)

    def test_filter_uuid(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = "abacaba"
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id}})
        uuid1 = yp_client.get_object("stage", stage_id, selectors=["/meta/uuid"])[0]
        yp_client.remove_object("stage", stage_id)
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id}})
        uuid2 = yp_client.get_object("stage", stage_id, selectors=["/meta/uuid"])[0]
        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]
        assert len(history_events) == 3
        assert yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": uuid1})["events"] == history_events[0:2]
        assert yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": uuid2})["events"] == history_events[2:3]

    def test_create_update_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()
        stage_id = "42"
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id}}, transaction_id=transaction_id)
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 123}], transaction_id=transaction_id)
        yp_client.remove_object("stage", stage_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)
        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]
        assert len(history_events) == 0

    def test_create_remove_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        stage_id = "crc"
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "a"}}, transaction_id=transaction_id)
        yp_client.remove_object("stage", stage_id, transaction_id=transaction_id)
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "b"}}, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "a"})["events"]
        assert len(history_events) == 0
        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "b"})["events"]
        assert len(history_events) == 1
        assert history_events[0]["event_type"] == 1

    def test_remove_create(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = "rc"
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "a"}, "spec": {"revision": 1}})

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("stage", stage_id, transaction_id=transaction_id)
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "b"}, "spec": {"revision": 2}}, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "a"})["events"]
        assert len(history_events) == 2
        assert history_events[0]["event_type"] == 1 and history_events[0]["results"][0]["value"]["revision"] == 1
        assert history_events[1]["event_type"] == 2

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "b"})["events"]
        assert len(history_events) == 1
        assert history_events[0]["event_type"] == 1 and history_events[0]["results"][0]["value"]["revision"] == 2

    def test_remove_create_remove(self, yp_env):
        yp_client = yp_env.yp_client

        stage_id = "rcr"
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "a"}, "spec": {"revision": 1}})

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("stage", stage_id, transaction_id=transaction_id)
        yp_client.create_object(object_type="stage", attributes={"meta": {"id": stage_id, "uuid": "b"}, "spec": {"revision": 2}}, transaction_id=transaction_id)
        yp_client.remove_object("stage", stage_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "a"})["events"]
        assert len(history_events) == 2
        assert history_events[0]["event_type"] == 1 and history_events[0]["results"][0]["value"]["revision"] == 1
        assert history_events[1]["event_type"] == 2

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"], {"uuid": "b"})["events"]
        assert len(history_events) == 0

    def test_pod_status_scheduling_filter(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set_with_quota(
            yp_client,
            bandwidth_quota=2 ** 60)[0]

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {
            "resource_requests": {
                "network_bandwidth_guarantee": 2 ** 29,
                "network_bandwidth_limit": 2 ** 32,
            },
            "enable_scheduling": True
        })

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))
        assert len(yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]) == 2
        assert_over_time(lambda: len(yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]) == 2)
        events = yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]
        assert not events[0]["results"][0]["value"]["node_id"]
        assert "error" not in events[0]["results"][0]["value"]
        assert events[0]["results"][0]["value"]["state"] == "pending"
        assert not events[1]["results"][0]["value"]["node_id"]
        assert "error" in events[1]["results"][0]["value"]
        assert events[1]["results"][0]["value"]["state"] == "pending"

    def test_pod_status_scheduling_history(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        create_nodes(yp_client, 2)

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {
            "resource_requests": {
                "vcpu_guarantee": 1
            },
            "enable_scheduling": True
        })

        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        assert(len(yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]) == 2)
        generation_number = yp_client.get_object("pod", pod_id, ["/status/generation_number"])[0]

        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.acknowledge_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        wait(lambda: yp_client.get_object("pod", pod_id, ["/status/generation_number"])[0] != generation_number)

        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        events = yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]
        assert len(events) == 4
        assert not events[0]["results"][0]["value"]["node_id"]
        assert "error" not in events[0]["results"][0]["value"]
        assert events[0]["results"][0]["value"]["state"] == "pending"
        assert events[1]["results"][0]["value"]["node_id"]
        assert "error" not in events[1]["results"][0]["value"]
        assert events[1]["results"][0]["value"]["state"] == "assigned"
        assert not events[2]["results"][0]["value"]["node_id"]
        assert "error" not in events[2]["results"][0]["value"]
        assert events[2]["results"][0]["value"]["state"] == "pending"
        assert events[3]["results"][0]["value"]["node_id"]
        assert "error" not in events[3]["results"][0]["value"]
        assert events[3]["results"][0]["value"]["state"] == "assigned"
