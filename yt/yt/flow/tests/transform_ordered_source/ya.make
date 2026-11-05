PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_pipeline.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
    yt/yt/flow/tests/transform_ordered_source/pipeline/proto
)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(
    arcadia/${MODDIR}/pipeline/distribute_pipeline.yson
    arcadia/${MODDIR}/pipeline/pipeline.yson
    arcadia/${MODDIR}/pipeline/proto_pipeline.yson
    arcadia/${MODDIR}/pipeline/proto_state_pipeline.yson
    arcadia/${MODDIR}/pipeline/state_pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
    ram_disk:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
