PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_queue.py
    yt_sync.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    yt/yt/flow/library/cpp/connectors/queue/tests/pipeline
)

DATA(arcadia/yt/yt/flow/library/cpp/connectors/queue/tests/pipeline/pipeline_swift.yson)
DATA(arcadia/yt/yt/flow/library/cpp/connectors/queue/tests/pipeline/pipeline_transform.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
