PY3_PROGRAM(queue_agent_alerts)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/queue_agent_helpers
    yt/python/yt/wrapper
)

PY_SRCS(
    __main__.py
)

END()
