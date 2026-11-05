PY3_PROGRAM(http_client_py_companion)

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
