PY23_TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SIZE(SMALL)

REQUIREMENTS(
    cpu:1
)

PEERDIR(
    yt/python/yt/yson
    yt/python/yt/ypath
    yt/python/yt/wrapper
    yt/python/yt/testlib
)

TEST_SRCS(
    test_fork.py
)

END()
