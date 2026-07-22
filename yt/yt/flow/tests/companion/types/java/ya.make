PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

TEST_SRCS(
    test_types.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/tests/companion/types/common
)

DEPENDS(
    ${MODDIR}/companion
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/companion/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    companion
)
