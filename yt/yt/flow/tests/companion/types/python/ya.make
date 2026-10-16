PY3TEST()

TEST_SRCS(
    test_types.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/tests/companion/types/common
)

DEPENDS(
    ${MODDIR}/pipeline
    yt/yt/flow/bin/flow_server
    yt/python/yt/wrapper/bin/yt_make
)

DATA(arcadia/${MODDIR}/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
