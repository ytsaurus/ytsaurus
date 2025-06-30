PY3TEST()

TEST_SRCS(test.py)

PEERDIR(
    yt/python/client
    yt/python/yt/wrapper/testlib
)

SET(YT_WITH_QUERY_TRACKER yes)
SET(YT_CONFIG_PATCH {components=[{name=query-tracker};{name=yql-agent;}]})

INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

DEPENDS(yt/yt/packages/tests_package)

SIZE(MEDIUM)

END()
