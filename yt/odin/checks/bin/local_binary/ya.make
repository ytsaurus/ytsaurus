PY3_PROGRAM(local_binary)

PEERDIR(
    yt/python/yt/wrapper
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/quorum_health
)

PY_SRCS(
    __main__.py
)

END()
