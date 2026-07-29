PY3_PROGRAM(gpu_sre_alerts)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/scheduler_alerts
)

PY_SRCS(
    __main__.py
)

END()
