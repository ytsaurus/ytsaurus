PY3TEST()

TEST_SRCS(
    test_repartition.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/tests/key_visitor/cpp/common
)

DEPENDS(
    yt/yt/flow/tests/key_visitor/cpp/pipeline
    yt/python/yt/wrapper/bin/yt_make
)

DATA(
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
