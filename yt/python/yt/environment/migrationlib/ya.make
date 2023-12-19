PY23_LIBRARY()

PEERDIR(
    yt/python/yt
    yt/python/yt/yson
    yt/python/yt/wrapper
)

PY_SRCS(
    NAMESPACE yt.environment.migrationlib
    __init__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
