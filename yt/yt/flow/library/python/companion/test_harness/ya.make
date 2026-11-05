PY3_LIBRARY()

PY_SRCS(
    __init__.py
    harness.py
    response.py
    schema.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
    yt/python/yt/yson
)

END()

RECURSE_FOR_TESTS(
    test
)
