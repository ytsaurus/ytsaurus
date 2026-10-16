INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/tests/computation_cycles_and_buffers/lib
)

DEPENDS(
    yt/yt/flow/tests/computation_cycles_and_buffers/pipeline
)

DATA(arcadia/yt/yt/flow/tests/computation_cycles_and_buffers/pipeline/pipeline.yson)
