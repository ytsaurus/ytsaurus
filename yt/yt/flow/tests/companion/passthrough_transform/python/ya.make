PY3_PROGRAM(passthrough_transform)

NO_CHECK_IMPORTS()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()

RECURSE_FOR_TESTS(
    test
)
