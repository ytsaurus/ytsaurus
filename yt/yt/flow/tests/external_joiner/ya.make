PY3TEST()

TEST_SRCS(
    test.py
    yt_sync.py
)

SET(YT_CLUSTER_NAMES primary)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(arcadia/${MODDIR}/pipeline/pipeline_auto.yson)
DATA(arcadia/${MODDIR}/pipeline/pipeline_manual.yson)

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
