PY3TEST()

TEST_SRCS(
    test_distributed_throttler.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(arcadia/${MODDIR}/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
    ram_disk:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
