PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    conftest.py
    test_wire_protocol.py
    test_computation.py
    test_context.py
    test_state.py
    test_request_processor.py
    test_server.py
    test_api.py
    test_sizing.py
    test_worker.py
    test_parent.py
    test_lazy_load.py
    test_parallelism.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/library/python/companion/test/proto
    yt/yt/flow/library/cpp/companion/proto
    yt/yt/flow/library/cpp/common/proto
    yt/yt_proto/yt/core
    contrib/python/grpcio
    library/python/port_manager
)

SIZE(MEDIUM)

END()

RECURSE(
    companion_binary
)
