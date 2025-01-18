GO_TEST()

TAG(ya:huge_logs)

SIZE(MEDIUM)

IF (OPENSOURCE AND AUTOCHECK)
    SKIP_TEST(Tests use docker)
ENDIF()

ENV(YT_STUFF_MAX_START_RETRIES=10)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_multicells.inc)
ENDIF()

GO_TEST_SRCS(
    cross_cell_commands_test.go
)

IF (OPENSOURCE)
    GO_TEST_SRCS(
        main_test.go
    )
ENDIF()

END()
