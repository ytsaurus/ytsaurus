PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_secret_env.py
    yt_sync.py
)

DEPENDS(
    yt/yt/flow/tests/secret_env/pipeline
)

DATA(arcadia/yt/yt/flow/tests/secret_env/pipeline/pipeline.yson)

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
