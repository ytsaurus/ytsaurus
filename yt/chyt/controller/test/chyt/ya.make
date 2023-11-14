GO_TEST()

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

DEPENDS(
    yt/chyt/server/bin
    yt/chyt/trampoline
)

SRCS(helpers.go)

GO_TEST_SRCS(chyt_test.go)

END()
