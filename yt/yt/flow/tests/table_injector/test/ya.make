PY3TEST()

TEST_SRCS(
    test_table_injector.py
    test_buffer_playground.py
    yt_sync.py
)

SET(YT_CLUSTER_NAMES primary,remote_0)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

DEPENDS(
    ${MODDIR}/..
)

DATA(
    arcadia/${MODDIR}/../node_config.yson
    arcadia/${MODDIR}/../pipeline.yson
    arcadia/${MODDIR}/../pipeline_two_injectors.yson
    arcadia/${MODDIR}/../pipeline_unequal_load.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

SIZE(MEDIUM)

END()
