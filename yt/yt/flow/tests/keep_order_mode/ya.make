PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_pipeline.py
    yt_sync.py
)

PEERDIR(
    library/python/codecs
    yt/yt/flow/library/python/queue
)

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
