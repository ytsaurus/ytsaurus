PY3_PROGRAM(quorum_health)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/quorum_health
    yt/python/contrib/python-requests
    yt/python/yt/wrapper
)

PY_SRCS(
    __main__.py
)

END()
