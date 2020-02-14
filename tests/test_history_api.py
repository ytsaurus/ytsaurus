from .conftest import (
    assert_over_time,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_error_pod_scheduling_status,
    is_pod_assigned,
)

from yp.common import wait

from yt.yson import YsonEntity

import pytest


@pytest.mark.usefixtures("yp_env_configurable")
class TestHistoryApiDisabledTypes(object):
    YP_MASTER_CONFIG = dict(
        object_manager = dict(
            history_disabled_types = [
                "pod",
            ],
        ),
    )

    def test(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        # History is not disabled for stages.
        stage_id = yp_client.create_object(object_type="stage", attributes={"spec": {"revision": 42}})
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec/revision", "value": 123}])

        history_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]
        assert len(history_events) > 0

        # History is disabled for pods.
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=dict(enable_scheduling=True))

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))
        assert 0 == len(yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"])


@pytest.mark.usefixtures("yp_env")
class TestHistoryApi(object):
    YP_MASTER_CONFIG = dict(
        object_manager=dict(
            stage_type_handler=dict(
                enable_status_history=True,
            )
        ),
    )

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

        stage_id = yp_client.create_object("stage", attributes={"spec": {"revision": 42}})

        events = yp_client.select_object_history(
            "stage",
            stage_id,
            ["/spec", "/spec/revision", "/meta", "/spec/abc", "/spec/abc", "/spec/revision/abc", "/spec/account_id"],
        )["events"]
        assert len(events) == 1

        results = events[0]["results"]
        assert len(results) == 7

        assert results[1]["value"] == 42
        for i in range(2, 7):
            assert isinstance(results[i]["value"], YsonEntity)

        yp_client.update_object(
            "stage",
            stage_id,
            set_updates=[
                dict(
                    path="/spec/account_id",
                    value="tmp",
                ),
            ],
        )

        next_events = yp_client.select_object_history("stage", stage_id, ["/spec"])["events"]
        assert len(next_events) == 2

        assert events[0]["results"][0]["value"] == next_events[0]["results"][0]["value"]

        assert "tmp" == next_events[1]["results"][0]["value"]["account_id"]

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

    @pytest.mark.usefixtures("yp_env_configurable")
    def test_stage_status_deploy_unit_conditions_filter(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage = {
            "spec": {"account_id": "tmp"},
            "status": {"deploy_units": {"unit-id": {"in_progress": {"status": "false"}}}}
        }
        stage_id = yp_client.create_object(
            "stage",
            attributes=stage,
        )
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/status/revision", "value": 123}])
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/status/revision", "value": 456}])
        assert len(yp_client.select_object_history("stage", stage_id, ["/status"])["events"]) == 1
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/status/deploy_units/unit-id/in_progress/status", "value": "true"}])
        assert len(yp_client.select_object_history("stage", stage_id, ["/status"])["events"]) == 2

    def test_pod_status_scheduling_filter(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=dict(enable_scheduling=True))

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

        pod_set_id = create_pod_set(yp_client)
        create_nodes(yp_client, 2)

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {
            "resource_requests": {
                "vcpu_guarantee": 1
            },
            "enable_scheduling": True
        })

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        assert(len(yp_client.select_object_history("pod", pod_id, ["/status/scheduling"])["events"]) == 2)
        generation_number = yp_client.get_object("pod", pod_id, ["/status/generation_number"])[0]

        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.acknowledge_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        wait(lambda: yp_client.get_object("pod", pod_id, ["/status/generation_number"])[0] != generation_number)

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        events = yp_client.select_object_history("pod", pod_id, ["/status/scheduling", "/status/eviction"])["events"]
        assert 5 == len(events)

        # Pod created.
        assert not events[0]["results"][0]["value"]["node_id"]
        assert "error" not in events[0]["results"][0]["value"]
        assert events[0]["results"][0]["value"]["state"] == "pending"
        assert events[0]["results"][1]["value"]["state"] == "none"
        assert events[0]["results"][1]["value"]["reason"] == "none"

        # Pod assigned.
        assert events[1]["results"][0]["value"]["node_id"]
        assert "error" not in events[1]["results"][0]["value"]
        assert events[1]["results"][0]["value"]["state"] == "assigned"
        assert events[1]["results"][1]["value"]["state"] == "none"
        assert events[1]["results"][1]["value"]["reason"] == "none"

        # Pod eviction acknowledged.
        assert events[2]["results"][0]["value"]["node_id"]
        assert "error" not in events[2]["results"][0]["value"]
        assert events[2]["results"][0]["value"]["state"] == "assigned"
        assert events[2]["results"][1]["value"]["state"] == "acknowledged"
        assert events[2]["results"][1]["value"]["reason"] == "none"

        # Pod evicted.
        assert not events[3]["results"][0]["value"]["node_id"]
        assert "error" not in events[3]["results"][0]["value"]
        assert events[3]["results"][0]["value"]["state"] == "pending"
        assert events[3]["results"][1]["value"]["state"] == "none"
        assert events[3]["results"][1]["value"]["reason"] == "none"

        # Pod assigned.
        assert events[4]["results"][0]["value"]["node_id"]
        assert "error" not in events[4]["results"][0]["value"]
        assert events[4]["results"][0]["value"]["state"] == "assigned"
        assert events[4]["results"][1]["value"]["state"] == "none"
        assert events[4]["results"][1]["value"]["reason"] == "none"

    def test_either_all_or_none_history_attributes_are_stored(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        create_nodes(yp_client, 1)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=dict(enable_scheduling=True))

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        events = yp_client.select_object_history("pod", pod_id, ["/status/scheduling", "/status/eviction"])["events"]
        assert 2 == len(events)
        assert 2 == len(events[0]["history_enabled_attributes"])
        assert set(["/status/scheduling", "/status/eviction"]) == set(events[0]["history_enabled_attributes"])

        scheduling = events[0]["results"][0]["value"]
        assert not scheduling["node_id"]
        assert scheduling["state"] == "pending"
        assert "error" not in scheduling
        eviction = events[0]["results"][1]["value"]
        assert eviction["state"] == "none"
        assert eviction["reason"] == "none"
        assert eviction["last_updated"] > 0
        assert len(eviction["message"]) > 0

        scheduling = events[1]["results"][0]["value"]
        assert len(scheduling["node_id"]) > 0
        assert scheduling["state"] == "assigned"
        assert "error" not in scheduling
        eviction = events[1]["results"][1]["value"]
        assert eviction["state"] == "none"
        assert eviction["reason"] == "none"
        assert eviction["last_updated"] > 0
        assert len(eviction["message"]) > 0

        yp_client.request_pod_eviction(pod_id, "Test")

        next_events = yp_client.select_object_history("pod", pod_id, ["/status/scheduling", "/status/eviction"])["events"]
        assert 3 == len(next_events)

        assert events[0] == next_events[0]
        assert events[1] == next_events[1]

        assert scheduling == next_events[2]["results"][0]["value"]
        eviction = next_events[2]["results"][1]["value"]
        assert eviction["state"] == "requested"
        assert eviction["reason"] == "client"
        assert eviction["last_updated"] > 0
        assert len(eviction["message"]) > 0

    def test_pod_status_eviction_history(self, yp_env):
        yp_client = yp_env.yp_client

        def get_eviction_timestamp(pod_id):
            return yp_client.get_object("pod", pod_id, selectors=["/status/eviction/last_updated"])[0]

        pod_set_id = create_pod_set(yp_client)
        create_nodes(yp_client, 1)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=dict(enable_scheduling=True))

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        timestamp = get_eviction_timestamp(pod_id)

        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.abort_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        assert get_eviction_timestamp(pod_id) > timestamp

        events = yp_client.select_object_history("pod", pod_id, [""])["events"]
        assert 2 == len(events)

        for event in events:
            value = event["results"][0]["value"]
            assert set(value.keys()) == set(["status"])
            assert set(value["status"].keys()) == set(["scheduling", "eviction"])

    def test_descending_time_order(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=dict(enable_scheduling=True))

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        for _ in range(3):
            yp_client.request_pod_eviction(pod_id, "Test")
            yp_client.abort_pod_eviction(pod_id, "Test")

        def _get_events(limit, descending_time_order, continuation_token=None):
            options = {"descending_time_order": descending_time_order, "limit": limit}
            if continuation_token:
                options["continuation_token"] = continuation_token

            return yp_client.select_object_history("pod", pod_id, [""], options=options)

        def _get_all_events(limit, descending_time_order):
            events = []
            continuation_token = None
            current_events = []

            while continuation_token is None or len(current_events) == limit:
                response = _get_events(limit, descending_time_order, continuation_token)
                current_events = response["events"]
                continuation_token = response["continuation_token"]
                events.extend(current_events)

            return events

        limit = 2
        events = _get_all_events(limit, False)
        events_reversed = _get_all_events(limit, True)

        assert len(events) == 8
        assert events == list(reversed(events_reversed))

        event_times = list(map(lambda event: event["time"], events))
        assert event_times == list(sorted(event_times))
