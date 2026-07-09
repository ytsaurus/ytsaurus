PY3_LIBRARY()

PY_SRCS(
    __init__.py
    _api.py
    _proto_compat.py
    wire_protocol.py
    row.py
    computation.py
    context.py
    state.py
    job.py
    stream.py
    proto_mapper.py
    service.py
    server.py
    sizing.py
    worker.py
    parent.py
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/proto
    yt/yt/flow/library/cpp/common/proto
    yt/yt/flow/library/python/runner
    yt/yt_proto/yt/core
    yt/python/yt/yson
    yt/python/yt/wrapper
    contrib/python/grpcio
)

END()

RECURSE(
    test_harness
)

RECURSE_FOR_TESTS(
    test
)
