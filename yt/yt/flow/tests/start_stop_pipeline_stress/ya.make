PY3TEST()

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/computation_cycles_and_buffers/lib/ya.make.inc)

PEERDIR(
    library/python/sanitizers
    yt/yt/flow/library/python/queue
)

DEPENDS(
)

DATA(arcadia/yt/yt/flow/tests/start_stop_pipeline_stress/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
