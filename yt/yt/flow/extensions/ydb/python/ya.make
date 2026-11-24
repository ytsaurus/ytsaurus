PY3_LIBRARY()

PY_SRCS(
    __init__.py
    codecs.py
    read.py
)

PEERDIR(
    contrib/python/ydb
    contrib/python/zstandard
)

END()

RECURSE_FOR_TESTS(
    tests
)
