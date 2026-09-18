PY3TEST()

TEST_SRCS(
    test_sweep.py
    test_swift_sweep.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/tests/key_visitor/cpp/common
)

DEPENDS(
    yt/yt/flow/tests/key_visitor/cpp/pipeline
    yt/yt/flow/tests/key_visitor/cpp/pipeline_external
    yt/yt/flow/tests/key_visitor/cpp/pipeline_swift
    yt/python/yt/wrapper/bin/yt_make
)

DATA(
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline/pipeline.yson
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline_external/pipeline.yson
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline_external/pipeline_manual.yson
    arcadia/yt/yt/flow/tests/key_visitor/cpp/pipeline_swift/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

# Four ~30s sweeps do not fit one 600s MEDIUM chunk under sanitizers.
FORK_SUBTESTS()
SPLIT_FACTOR(4)

SIZE(MEDIUM)

END()
