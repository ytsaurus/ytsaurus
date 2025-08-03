PY3_PROGRAM(queue_agent_controller_liveness)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/python/yt/wrapper

    contrib/python/pytz
)

PY_SRCS(
    __main__.py
)

END()
