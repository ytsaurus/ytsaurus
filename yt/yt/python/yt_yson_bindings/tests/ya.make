PY3TEST()

TEST_SRCS(
    signal_handlers.py
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt/testlib
    yt/yt/python/yt_yson_bindings
)

DEPENDS(
    yt/yt/python/yt_yson_bindings/tests/test_program
)

END()

RECURSE(
    test_program
)
