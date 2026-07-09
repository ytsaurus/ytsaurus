PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    test_schema.py
    test_harness.py
    test_response.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/python/yt/yson
)

SIZE(SMALL)

END()
