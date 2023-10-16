PY23_TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SIZE(MEDIUM)

# Required for test_thread_pool.py
FORK_TEST_FILES()

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/testlib
    yt/python/yt/yson

    contrib/python/flaky
)

TEST_SRCS(
    test_common.py
    test_formats.py
    test_schema.py
    test_thread_pool.py
)

END()

RECURSE(py3_only)
