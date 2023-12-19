PY3TEST()

SIZE(MEDIUM)
    
PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/testlib
    yt/python/yt/yson
    
    contrib/python/flaky
)

TEST_SRCS(
    test_schema.py
)

END()
