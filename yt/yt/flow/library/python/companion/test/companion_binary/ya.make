PY3_PROGRAM(companion_test_binary)

NO_CHECK_IMPORTS()

PY_SRCS(
    __init__.py
    __main__.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()
