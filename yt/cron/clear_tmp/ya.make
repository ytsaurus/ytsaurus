PY3_PROGRAM()

PEERDIR(
    yt/cron/library
    yt/python/client
)

PY_SRCS(
    __main__.py
)

END()

RECURSE_FOR_TESTS(tests)
