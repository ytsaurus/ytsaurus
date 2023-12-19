PY3TEST()

DATA(sbr://1323178467)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/tests/library
    contrib/python/pytest-benchmark
)

TEST_SRCS(
    test_yson_performance.py
)

REQUIREMENTS(
    ram:32
    # cpu:2
)

END()
