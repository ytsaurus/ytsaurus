PY3TEST()

TEST_SRCS(
    test_completion.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/tests/key_visitor/cpp/common
)

DEPENDS(
    yt/yt/flow/tests/key_visitor/cpp/pipeline_keyvisitor_only
    yt/yt/flow/tests/key_visitor/cpp/pipeline_visitor_loop
    yt/python/yt/wrapper/bin/yt_make
)

DATA(
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline_keyvisitor_only/pipeline.yson
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline_visitor_loop/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

FORK_SUBTESTS()
SPLIT_FACTOR(2)

SIZE(MEDIUM)

END()
