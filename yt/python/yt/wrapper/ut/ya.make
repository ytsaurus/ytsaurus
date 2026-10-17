PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SIZE(SMALL)

PEERDIR(
    yt/python/yt/wrapper
)

TEST_SRCS(
    test_ping_transaction.py
)

END()
