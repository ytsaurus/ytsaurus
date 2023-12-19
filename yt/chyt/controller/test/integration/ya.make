GO_TEST()

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

GO_TEST_SRCS(
    api_test.go
    monitoring_test.go
    strawberry_test.go
)

END()
