from __future__ import print_function

from .conftest import (
    create_pod_with_boilerplate,
    create_nodes,
    DEFAULT_POD_SET_SPEC
)

from yp.local import DbManager

from yp.common import (
    YpInvalidContinuationTokenError,
    YpRowsAlreadyTrimmedError,
    YtResponseError,
    wait,
)

from yt.packages.six.moves import xrange

from datetime import timedelta
import pytest


@pytest.mark.usefixtures("yp_env")
class TestWatches(object):
    def test_time_limit(yp_client, yp_env):
        yp_client = yp_env.yp_client

        start_timestamp = yp_client.generate_timestamp()
        timestamp = start_timestamp + (10 << 30) # Add 10 seconds to start timestamp.

        with pytest.raises(YtResponseError):
            yp_client.watch_objects(
                "pod_set",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=1))

    def test_watches_simple(yp_client, yp_env):
        yt_client = yp_env.yt_client
        yp_client = yp_env.yp_client

        start_timestamp = yp_client.generate_timestamp()

        for pod_set_idx in xrange(10):
            yp_client.create_object("pod_set", {"meta": {"id": "pod_set_{}".format(pod_set_idx)}})

        for pod_set_idx in [3, 5, 7]:
            yp_client.remove_object("pod_set", "pod_set_{}".format(pod_set_idx))

        yp_client.update_object("pod_set", "pod_set_6", set_updates=[{
            "path": "/spec/antiaffinity_constraints",
            "value": [
                {"key": "node", "max_pods": 1},
            ]}])

        for pod_set_idx in [10, 11, 12]:
            yp_client.create_object("pod_set", {"meta": {"id": "pod_set_{}".format(pod_set_idx)}})

        end_timestamp = yp_client.generate_timestamp()

        result = yp_client.watch_objects(
            "pod_set",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5),
            enable_structured_response=True)
        events = result["events"]
        assert len(events) == 17

        for i in xrange(len(events)):
            timestamp = events[i]["timestamp"]
            assert len(yp_client.watch_objects("pod_set", start_timestamp=timestamp)) == len(events) - i
            assert len(yp_client.watch_objects("pod_set", start_timestamp=timestamp + 1)) == len(events) - i - 1

        result = yp_client.watch_objects("pod_set", start_timestamp=start_timestamp, event_count_limit=11, enable_structured_response=True)
        assert len(result["events"]) == 11

        next_result_with_token = yp_client.watch_objects(
            "pod_set",
            continuation_token=result["continuation_token"],
            timestamp=result["timestamp"],
            event_count_limit=8,
            enable_structured_response=True)
        assert len(next_result_with_token["events"]) == 6

        next_result = yp_client.watch_objects(
            "pod_set",
            start_timestamp=result["events"][-1]["timestamp"] + 1,
            timestamp=result["timestamp"],
            event_count_limit=8,
            enable_structured_response=True)
        assert next_result_with_token == next_result

    def test_watches_after_strange_transaction(yp_client, yp_env):
        yp_client = yp_env.yp_client

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.create_object("virtual_service", attributes={"meta": {"id": vs_id}}, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 2
        assert events[0]["object_id"] == vs_id
        assert events[1]["object_id"] == vs_id
        assert events[0]["timestamp"] == events[1]["timestamp"]
        assert events[0]["event_type"] == "object_removed"
        assert events[1]["event_type"] == "object_created"

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.create_object("virtual_service", attributes={"meta": {"id": vs_id}}, transaction_id=transaction_id)
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 1
        assert events[0]["object_id"] == vs_id
        assert events[0]["event_type"] == "object_removed"

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.create_object("virtual_service", attributes={"meta": {"id": vs_id}}, transaction_id=transaction_id)
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.create_object("virtual_service", attributes={"meta": {"id": vs_id}}, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 2
        assert events[0]["object_id"] == vs_id
        assert events[1]["object_id"] == vs_id
        assert events[0]["timestamp"] == events[1]["timestamp"]
        assert events[0]["event_type"] == "object_removed"
        assert events[1]["event_type"] == "object_created"

        start_timestamp = yp_client.generate_timestamp()
        transaction_id = yp_client.start_transaction()
        vs_id = yp_client.create_object("virtual_service", transaction_id=transaction_id)
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 0

        start_timestamp = yp_client.generate_timestamp()
        transaction_id = yp_client.start_transaction()
        vs_id = yp_client.create_object("virtual_service", transaction_id=transaction_id)
        yp_client.update_object("virtual_service", vs_id, set_updates=[
            {"path": "/spec", "value": {}},
        ], transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 1
        assert events[0]["event_type"] == "object_created"

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()
        transaction_id = yp_client.start_transaction()
        yp_client.update_object("virtual_service", vs_id, set_updates=[
            {"path": "/spec", "value": {}},
        ], transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 1
        assert events[0]["event_type"] == "object_updated"

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()
        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.create_object("virtual_service", attributes={"meta": {"id": vs_id}}, transaction_id=transaction_id)
        yp_client.update_object("virtual_service", vs_id, set_updates=[
            {"path": "/spec", "value": {}},
        ], transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 2
        assert events[0]["event_type"] == "object_removed"
        assert events[1]["event_type"] == "object_created"

        vs_id = yp_client.create_object("virtual_service")
        start_timestamp = yp_client.generate_timestamp()
        transaction_id = yp_client.start_transaction()
        yp_client.update_object("virtual_service", vs_id, set_updates=[
            {"path": "/spec", "value": {}},
        ], transaction_id=transaction_id)
        yp_client.remove_object("virtual_service", vs_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        end_timestamp = yp_client.generate_timestamp()
        events = yp_client.watch_objects(
            "virtual_service",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5))
        assert len(events) == 1
        assert events[0]["event_type"] == "object_removed"

    def test_invalid_continuation_token(yp_client, yp_env):
        yp_client = yp_env.yp_client

        start_timestamp = yp_client.generate_timestamp()
        for i in xrange(10):
            yp_client.create_object("pod_set")
        end_timestamp = yp_client.generate_timestamp()

        result = yp_client.watch_objects(
            "pod_set",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5),
            event_count_limit=4,
            enable_structured_response=True)

        with pytest.raises(YpInvalidContinuationTokenError):
            yp_client.watch_objects(
                "pod_set",
                continuation_token="42",
                timestamp=end_timestamp)

    def test_watches_after_scheduler_allocation(yp_client, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        create_nodes(yp_client, 1)

        start_timestamp = yp_client.generate_timestamp()
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {
            "resource_requests": {
                "vcpu_guarantee": 100
            },
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] != "" and
                     yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")
        end_timestamp = yp_client.generate_timestamp()

        assert len(yp_client.watch_objects("pod", start_timestamp=start_timestamp, timestamp=end_timestamp, time_limit=timedelta(seconds=5))) > 0
        assert len(yp_client.watch_objects("node", start_timestamp=start_timestamp, timestamp=end_timestamp, time_limit=timedelta(seconds=5))) > 0
        assert len(yp_client.watch_objects("resource", start_timestamp=start_timestamp, timestamp=end_timestamp, time_limit=timedelta(seconds=5))) > 0


@pytest.mark.usefixtures("yp_env_configurable")
class TestTrimmedWatchLog(object):
    START = False

    def test_trimmed_watch_log(yp_client, yp_env_configurable):
        yp_instance = yp_env_configurable.yp_instance
        yt_client = yp_instance.create_yt_client()

        db_manager = DbManager(yt_client, "//yp")
        db_manager.unmount_table("pod_sets_watch_log")
        yt_client.reshard_table("//yp/db/pod_sets_watch_log", tablet_count=1)
        # TODO(avitella): Use just table name.
        db_manager.mount_tables(["//yp/db/pod_sets_watch_log"])

        yp_env_configurable._start()
        yp_client = yp_env_configurable.yp_client

        for pod_set_idx in xrange(1):
            yp_client.create_object("pod_set")
        start_timestamp = yp_client.generate_timestamp()
        for pod_set_idx in xrange(20):
            yp_client.create_object("pod_set")
        end_timestamp = yp_client.generate_timestamp()

        result = yp_client.watch_objects(
            "pod_set",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=5),
            event_count_limit=12,
            enable_structured_response=True)

        rows = list(yt_client.select_rows("""
            [object_id], [$row_index]
            from
                [//yp/db/pod_sets_watch_log]
            where
                [$timestamp] >= {} and [$timestamp] <= {}"""
            .format(start_timestamp, end_timestamp)))

        object_to_row_index = {}
        for row in rows:
            object_to_row_index[row["object_id"]] = row["$row_index"]

        barrier_event = None
        for event in result["events"]:
            if event["object_id"] in object_to_row_index:
                barrier_event = event

        if barrier_event is None:
            print("result: {}".format(result))
            print("selected rows: {}".format(rows))
            print("object_to_row_index: {}".format(object_to_row_index))
        assert barrier_event is not None

        barrier_row_index = object_to_row_index[barrier_event["object_id"]]
        assert barrier_row_index > 0

        last_timestamp = result["events"][-1]["timestamp"]

        yt_client.trim_rows("//yp/db/pod_sets_watch_log", 0, barrier_row_index)

        events = yp_client.watch_objects(
            "pod_set",
            start_timestamp=last_timestamp + 1,
            timestamp=end_timestamp)
        assert len(events) == 8

        events = yp_client.watch_objects(
            "pod_set",
            continuation_token=result["continuation_token"],
            timestamp=end_timestamp)
        assert len(events) == 8

        yt_client.trim_rows("//yp/db/pod_sets_watch_log", 0, barrier_row_index + 1)

        with pytest.raises(YpRowsAlreadyTrimmedError):
            yp_client.watch_objects(
                "pod_set",
                start_timestamp=last_timestamp + 1,
                timestamp=end_timestamp)

        events = yp_client.watch_objects(
            "pod_set",
            continuation_token=result["continuation_token"],
            timestamp=end_timestamp)
        assert len(events) == 8

        yt_client.trim_rows("//yp/db/pod_sets_watch_log", 0, barrier_row_index + 2)

        with pytest.raises(YpRowsAlreadyTrimmedError):
            yp_client.watch_objects(
                "pod_set",
                start_timestamp=last_timestamp + 1,
                timestamp=end_timestamp)

        with pytest.raises(YpRowsAlreadyTrimmedError):
            yp_client.watch_objects(
                "pod_set",
                continuation_token=result["continuation_token"],
                timestamp=end_timestamp)
