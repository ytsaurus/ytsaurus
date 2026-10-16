PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_reanimate.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    yt/yt/flow/tests/reanimate_vanilla/java/pipeline
    yt/yt/flow/bin/flow_server
    yt/yt/flow/tools/reanimate_vanilla_operation
)

DATA(arcadia/yt/yt/flow/tests/reanimate_vanilla/java/pipeline/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

# The sanitized binaries are too slow for this heavyweight integration scenario; the regular
# builds keep the coverage.
IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
