PY3_PROGRAM(scheduler_alerts_nodes_with_insufficient_resource_limits)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/scheduler_alerts
)

PY_SRCS(
    __main__.py
)

END()
