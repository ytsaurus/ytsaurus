PY3_PROGRAM()

PEERDIR(
    yt/python/client
)

PY_SRCS(
    __main__.py
)

END()

RECURSE_FOR_TESTS(tests)
