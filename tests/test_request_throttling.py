from yp import common as yp_common

import pytest

from concurrent import futures

import time


@pytest.mark.usefixtures("yp_env_configurable")
class TestRequestTracker(object):
    YP_MASTER_CONFIG = {
        "access_control_manager": {
            "request_tracker": {
                "enabled": True,
            }
        },
    }

    def test_throttlers(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        user_id = "robin"
        sample_batch_size = 10
        sample_batch_call_response = [[user_id]] * sample_batch_size

        def sample_batch_call():
            user_client = yp_env_configurable.yp_instance.create_client(config={"user": user_id})
            try:
                return user_client.get_objects("user", [user_id] * sample_batch_size, selectors=["/meta/id"])
            except Exception as err:
                return err

        def sample_call():
            user_client = yp_env_configurable.yp_instance.create_client(config={"user": user_id})
            try:
                return user_client.get_object("user", user_id, selectors=["/meta/id"])
            except Exception as err:
                return err

        # Check unlimited user
        yp_client.create_object("user", attributes={
            "meta": {"id": user_id},
            "spec": {"request_weight_rate_limit": 0, "request_queue_size_limit": 0}})

        assert [0, 0] == yp_client.get_object(
            "user",
            user_id,
            selectors=['/spec/request_weight_rate_limit', '/spec/request_queue_size_limit'])

        yp_env_configurable.sync_access_control()

        def check_no_limit():
            call_count = 10
            calls = []
            with futures.ThreadPoolExecutor(call_count) as executor:
                for i in range(call_count):
                    calls.append(executor.submit(sample_batch_call))

            for call_result in futures.as_completed(calls, timeout=10):
                assert call_result.result() == sample_batch_call_response

        check_no_limit()

        def check_acl():
            # Test ACL
            acl_user_client = yp_env_configurable.yp_instance.create_client(config={"user": user_id})
            with pytest.raises(yp_common.YtResponseError):
                acl_user_client.update_object(
                    "user",
                    user_id,
                    set_updates=[{"path": "/spec/request_weight_rate_limit", "value": 100}])
        check_acl()

        def check_validation():
            # Test invalid values configuration
            with pytest.raises(yp_common.YtResponseError):
                yp_client.update_object(
                    "user",
                    user_id,
                    set_updates=[{"path": "/spec/request_weight_rate_limit", "value": -100}])

            with pytest.raises(yp_common.YtResponseError):
                yp_client.update_object(
                    "user",
                    user_id,
                    set_updates=[{"path": "/spec/request_queue_size_limit", "value": -100}])

        check_validation()

        def check_request_queue_size_limit():
            # throttle user and check request_queue_size_limit
            yp_client.update_object(
                "user",
                user_id,
                set_updates=[
                    {"path": "/spec/request_weight_rate_limit", "value": 1},
                    {"path": "/spec/request_queue_size_limit", "value": 1},
                ])

            yp_env_configurable.sync_access_control()

            call_count = 10
            calls = []
            with futures.ThreadPoolExecutor(call_count) as executor:
                for i in range(call_count):
                    calls.append(executor.submit(sample_call))

            any_failed = False
            for call_result in futures.as_completed(calls, timeout=2):
                value = call_result.result()
                if isinstance(value, list):
                    assert value == [user_id]
                elif isinstance(value, yp_common.YpRequestThrottledError):
                    any_failed = True
                else:
                    raise RuntimeError("Unexpected result: {}".format(value))

            assert any_failed
        check_request_queue_size_limit()

        def check_request_weight_rate_limit():
            # throttle user and check request_weight_rate_limit
            yp_client.update_object(
                "user",
                user_id,
                set_updates=[
                    {"path": "/spec/request_weight_rate_limit", "value": 1},
                    {"path": "/spec/request_queue_size_limit", "value": 0},
                ])

            yp_env_configurable.sync_access_control()
            call_count = 10
            calls = []
            wall_clock_start_ts = time.time()
            with futures.ThreadPoolExecutor(call_count) as executor:
                for i in range(call_count):
                    calls.append(executor.submit(sample_call))

            for call_result in futures.as_completed(calls, timeout=20):
                value = call_result.result()
                assert value == [user_id]
            wall_clock_end_ts = time.time()
            assert wall_clock_end_ts - wall_clock_start_ts > 8

        check_request_weight_rate_limit()

        # Test user recreation
        def check_recreation():
            yp_client.remove_object("user", user_id)
            yp_env_configurable.sync_access_control()

            with pytest.raises(yp_common.YpAuthenticationError):
                user_client = yp_env_configurable.yp_instance.create_client(config={"user": user_id})
                user_client.get_object("user", "root", selectors=["/meta/id"])

            yp_client.create_object("user", attributes={
                "meta": {"id": user_id},
                "spec": {"request_weight_rate_limit": 100, "request_queue_size_limit": 100}})

            yp_env_configurable.sync_access_control()
            assert sample_batch_call() == sample_batch_call_response

        check_recreation()
