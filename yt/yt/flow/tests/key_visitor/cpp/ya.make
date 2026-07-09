PY3TEST()

TEST_SRCS(
    test_key_visitor.py
    test_swift_key_visitor.py
    yt_sync.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

DEPENDS(
    ${MODDIR}/pipeline
    ${MODDIR}/pipeline_external
    ${MODDIR}/pipeline_keyvisitor_only
    ${MODDIR}/pipeline_swift
    yt/python/yt/wrapper/bin/yt_make
)

DATA(
    arcadia/${MODDIR}/pipeline/pipeline.yson
    arcadia/${MODDIR}/pipeline_external/pipeline.yson
    arcadia/${MODDIR}/pipeline_keyvisitor_only/pipeline.yson
    arcadia/${MODDIR}/pipeline_swift/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    lib
    pipeline
    pipeline_external
    pipeline_keyvisitor_only
    pipeline_swift
)

RECURSE_FOR_TESTS(
    unittest
)
