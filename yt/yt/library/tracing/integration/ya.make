PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/restrict_licenses_tests.inc)

PEERDIR(
    contrib/python/requests
)

TEST_SRCS(
    test_jaeger.py
)

DEPENDS(
    yt/yt/library/tracing/example
    yt/jaeger/bundle
)

END()
