PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_static_table_join.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/..
)

DATA(arcadia/${MODDIR}/../pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
