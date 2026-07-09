PY3TEST()

TEST_SRCS(
    test_pipeline.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

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
