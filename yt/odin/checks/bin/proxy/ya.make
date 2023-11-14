PY3_PROGRAM(proxy)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/python/contrib/python-requests
    yt/python/yt/wrapper
)

PY_SRCS(
    __main__.py
)

END()
