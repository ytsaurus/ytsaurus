PY3_PROGRAM(queue_agent_controller_liveness)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/queue_agent_helpers
    yt/python/yt/wrapper

    contrib/python/dacite
    contrib/python/pytz
)

PY_SRCS(
    __main__.py
)

END()
