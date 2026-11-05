PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    test_schema.py
    test_harness.py
    test_proto_external_state.py
    test_response.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/library/python/companion/test/proto
    yt/python/yt/yson
)

SIZE(SMALL)

END()
