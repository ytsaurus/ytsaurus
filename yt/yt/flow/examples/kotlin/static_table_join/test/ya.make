PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_kotlin_static_table_join.py
    yt_sync.py
)

DEPENDS(
    ${MODDIR}/../static_table_join
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/../static_table_join/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
