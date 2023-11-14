from yp.tests.helpers.conftest import create_nodes

from yp.admin.db_operations import set_account_infinite_resource_limits

from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import (make_check_dir, configure_and_run_checks, wait)


class TestYPSchedulePod:
    # Overrides big (comparing to the check timeout) default timeout.
    YP_CLIENT_CONFIG = dict(request_timeout=10 * 1000)

    def test(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("account", attributes=dict(meta=dict(id="odin")))
        set_account_infinite_resource_limits(yp_client, "odin")

        # Garbage collecting client expects this user to be present.
        yp_client.create_object("user", attributes=dict(meta=dict(id="robot-yt-odin")))

        create_nodes(yp_client, node_count=10)

        def is_segment_empty(segment):
            return yp_client.get_object(
                "node_segment",
                segment,
                selectors=["/status/total_resources/cpu/capacity"],
            )[0] == 0

        # Check filters out empty node segments.
        wait(lambda: not is_segment_empty("default"))

        yt_cluster_url = yp_env.yt_client.config["proxy"]["url"]
        check_options = dict(
            yp_grpc_address=yp_env.yp_instance.yp_client_grpc_address,
            yp_http_address=yp_env.yp_instance.yp_http_address,
            yp_config=dict(enable_ssl=False),
        )
        checks_path = make_check_dir("yp_schedule_pod", check_options)
        storage = configure_and_run_checks(yt_cluster_url, checks_path)

        assert abs(storage.get_service_states("yp_schedule_pod")[0] - FULLY_AVAILABLE_STATE) <= 0.001
