from yt_odin_checks.lib.yp_helpers import YpGarbageCollectedClient

import yp.tests.helpers.conftest  # noqa

from yp.common import YpNoSuchObjectError

from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks

import pytest

import time


class TestYPGarbageCollector:
    # Overrides big (comparing to the check timeout) default timeout.
    YP_CLIENT_CONFIG = dict(request_timeout=10 * 1000)

    def test(self, yp_env):
        yp_client = yp_env.yp_client

        # Garbage collecting client expects this user to be present.
        yp_client.create_object("user", attributes=dict(meta=dict(id="robot-yt-odin")))

        yt_cluster_url = yp_env.yt_client.config["proxy"]["url"]

        yp_gc_client = YpGarbageCollectedClient("test", yp_client)
        pod_set_id = yp_gc_client.create_object("pod_set")
        pod_id = yp_gc_client.create_object(
            "pod",
            attributes=dict(meta=dict(creation_time=0, pod_set_id=pod_set_id)),
        )
        second_pod_set_id = yp_gc_client.create_object("pod_set", ttl=10)

        # Wait for second pod set to expire.
        time.sleep(10)

        check_options = dict(
            default_ttl=3600,
            yp_grpc_address=yp_env.yp_instance.yp_client_grpc_address,
            yp_http_address=yp_env.yp_instance.yp_http_address,
            yp_config=dict(enable_ssl=False),
        )
        checks_path = make_check_dir("yp_garbage_collector", check_options)
        storage = configure_and_run_checks(yt_cluster_url, checks_path)
        assert abs(storage.get_service_states("yp_garbage_collector")[0] - FULLY_AVAILABLE_STATE) <= 0.001

        with pytest.raises(YpNoSuchObjectError):
            yp_gc_client.get_object("pod", pod_id, ["/meta/id"])

        yp_gc_client.get_object("pod_set", pod_set_id, ["/meta/id"])

        with pytest.raises(YpNoSuchObjectError):
            yp_gc_client.get_object("pod_set", second_pod_set_id, ["/meta/id"])
