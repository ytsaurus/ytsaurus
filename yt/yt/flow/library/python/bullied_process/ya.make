PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    library/python/testing/yatest_common
)

END()

RECURSE_FOR_TESTS(
    test
)
