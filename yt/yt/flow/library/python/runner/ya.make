PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/python/yt/wrapper
)

END()

RECURSE_FOR_TESTS(
    tests
)
