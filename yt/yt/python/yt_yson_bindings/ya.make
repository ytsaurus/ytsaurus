PY23_LIBRARY()

PEERDIR(yt/yt/python/yson)

PY_SRCS(
    NAMESPACE yt_yson_bindings

    __init__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
