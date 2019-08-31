from yp.common import GrpcError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestClientConfig(object):
    def test_grpc_channel_options(self, yp_env):
        def get_options(*args, **kwargs):
            client = yp_env.yp_instance.create_client(*args, **kwargs)
            return dict(client._transport_layer._create_channel_options())

        options = get_options(
            config=dict(
                request_timeout=1000,
                grpc_channel_options=dict(
                    keepalive_time_ms=10,
                    keepalive_timeout_ms=1,
                ),
            ),
        )
        assert options == {"grpc.keepalive_time_ms": 10, "grpc.keepalive_timeout_ms": 1}

        options = get_options(config=dict(request_timeout=999))
        assert options == {"grpc.keepalive_time_ms": 499, "grpc.keepalive_timeout_ms": 999}

        options = get_options(
            config=dict(
                request_timeout=1000,
                grpc_channel_options=dict(
                    keepalive_time_ms=15,
                ),
            ),
        )
        assert options == {"grpc.keepalive_time_ms": 15, "grpc.keepalive_timeout_ms": 1000}

        # Make sure Grpc options are actually working.
        client = yp_env.yp_instance.create_client(
            config=dict(
                request_timeout=1000,
                grpc_channel_options=dict(
                    max_receive_message_length=1,
                ),
            ),
        )
        with pytest.raises(GrpcError):
            client.generate_timestamp()

        # Make sure YP client is working with no options specified.
        client = yp_env.yp_instance.create_client()
        client.generate_timestamp()
