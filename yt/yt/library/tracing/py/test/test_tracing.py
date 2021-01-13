import yt_tracing


def test_tracing():
    yt_tracing.initialize_tracer({
        "service_name": "python",
        "flush_period": 100,
        "collector_channel_config": {"address": "localhost:12345"},
        "enable_pid_tag": True,
    })
