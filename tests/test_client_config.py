from yp.common import GrpcError

from yt.packages.six import PY3

import pytest


@pytest.mark.usefixtures("yp_env")
class TestClientConfig(object):
    def test_grpc_channel_options(self, yp_env):
        def get_options(*args, **kwargs):
            client = yp_env.yp_instance.create_client(*args, **kwargs)
            return dict(client._transport_layer._create_channel_options())

        def validate_keepalive_values(options, keepalive_time, keepalive_timeout):
            assert keepalive_time == options["grpc.keepalive_time_ms"]
            assert keepalive_timeout == options["grpc.keepalive_timeout_ms"]

        options = get_options(
            config=dict(
                request_timeout=1000,
                grpc_channel_options=dict(
                    keepalive_time_ms=10,
                    keepalive_timeout_ms=1,
                ),
            ),
        )
        validate_keepalive_values(options, 10, 1)

        options = get_options(config=dict(request_timeout=999))
        validate_keepalive_values(options, 499, 999)

        options = get_options(
            config=dict(
                request_timeout=1000,
                grpc_channel_options=dict(
                    keepalive_time_ms=15,
                ),
            ),
        )
        validate_keepalive_values(options, 15, 1000)

        def validate_receive_message_length_constraint(receive_message_length):
            client = yp_env.yp_instance.create_client(
                config=dict(
                    request_timeout=1000,
                    grpc_channel_options=dict(
                        max_receive_message_length=receive_message_length,
                    ),
                ),
            )
            with pytest.raises(GrpcError):
                client.generate_timestamp()

        # Make sure Grpc options are actually working.
        validate_receive_message_length_constraint(1)

        # Check long to int implicit conversion.
        if not PY3:
            validate_receive_message_length_constraint(long(1))

        # Make sure YP client is working with no options specified.
        client = yp_env.yp_instance.create_client()
        client.generate_timestamp()
