PY3TEST()

SET(YT_CLUSTER_NAMES primary,remote_0)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

TEST_SRCS(
    test_wait_click_join.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/..
    ${MODDIR}/../tools/yt_sync
)

DATA(arcadia/${MODDIR}/../pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
