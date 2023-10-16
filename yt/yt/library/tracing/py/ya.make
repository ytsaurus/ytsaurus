PY23_LIBRARY()

SRCS(
    init.cpp
)

PY_SRCS(
    TOP_LEVEL
    yt_tracing.pyx
)

PEERDIR(
    yt/yt/core
    yt/yt/library/tracing/jaeger
    yt/python/yt/yson
    yt/yt/python/yt_driver_rpc_bindings
)

END()

RECURSE_FOR_TESTS(test)
