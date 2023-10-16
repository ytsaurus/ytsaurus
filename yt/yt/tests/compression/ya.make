PY3TEST()

DATA(sbr://2042900053)

SIZE(MEDIUM)

DEPENDS(yt/yt/tools/run_codec)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(yt/python/yt/environment/arcadia_interop)

TEST_SRCS(
    test_compression.py
)

END()
